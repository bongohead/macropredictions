"""
Script for scraping Conference Board economic forecasts.
"""

#%% ------- Load libs
import os
import pandas as pd
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo   
import json 

import asyncio
import base64
from playwright.async_api import async_playwright
from IPython.display import HTML, display

from py_helpers.env import load_env
from py_helpers.pg import get_postgres_query, write_postgres_df, execute_postgres_query
from py_helpers.image_cleaners import resize_image
from py_helpers.llm import get_llm_responses_openai
from py_helpers.misc import anti_join
from py_helpers.exceptions import SkipRun
from py_helpers.async_helpers import async_run

load_env()
validation_log = {}

#%% ------- Set constants
LLM_ATTEMPTS = 2

#%% ------- Get last stored vdate
last_stored_vdate = str(get_postgres_query(
    """
    SELECT
        DISTINCT(vdate) AS vdate 
    FROM forecast_values_v2
    WHERE forecast = 'cb'
    ORDER BY 1 DESC
    LIMIT 1
    """
)['vdate'][0])
print(f'Last stored vdate: {last_stored_vdate}') 
validation_log['last_stored_vdate'] = last_stored_vdate

#%% ------- Check if new vdate available
async def get_publication_date() -> str:
    """
    Get last publication date
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless = True)
        page = await browser.new_page()
        
        # Go to the page and wait for network quiescence.
        await page.goto(
            'https://www.conference-board.org/research/us-forecast',
            wait_until = 'networkidle'
        )
        
        # Wait until the date element is in the DOM & visible.
        await page.wait_for_selector('.newPDate', state = 'visible', timeout = 5_000)        
        raw_date = await page.locator('.newPDate').inner_text() # Extract text
        
        # Convert to ISO 8601 (YYYY-MM-DD).
        iso_date = datetime.strptime(raw_date.strip(), '%B %d, %Y').date().isoformat()

        await browser.close()
        return iso_date
    
available_vdate = async_run(get_publication_date())
print(f'Currently available vdate: {available_vdate}')
validation_log['available_vdate'] = available_vdate

#%% ------- Compare dates
if not available_vdate or available_vdate <= last_stored_vdate:
    validation_log['exit_message'] = 'Exiting early, no new data'
    raise SkipRun()

#%% ------- Get screenshot of forecast table
async def screenshot_to_b64_async(url, width = 800, height = 600):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport = {'width': width, 'height': height})
        await page.goto(url)
        await page.wait_for_load_state('networkidle')
        await page.evaluate('window.scrollTo(0, document.body.scrollHeight)') # trigger lazy iframes – scroll them into view
        await page.wait_for_timeout(1_000) # Extra ms delay
        await asyncio.gather(*[f.wait_for_load_state('load') for f in page.frames if f != page.main_frame]) # Wait for every embed iframe to load
        full_h = await page.evaluate('document.documentElement.scrollHeight') # Get full height + reset viewport
        await page.set_viewport_size({'width': width, 'height': full_h})
        png_bytes = await page.screenshot(type = 'png', full_page = True)

    png_b64 = base64.b64encode(png_bytes).decode()
    webp_b64 = resize_image(png_b64, output_format = 'webp')['image']
    display(HTML(f'<img src="data:image/webp;base64,{webp_b64}" width="500"/>'))
    return webp_b64

img_b64 = async_run(screenshot_to_b64_async('https://datawrapper.dwcdn.net/KboSH/'))

#%% ------- Prepare input prompt
system_prompt =\
"""
You will receive a screenshot containing a quarterly economic forecast table. Extract data from this image and return them as a JSON object.

# Objective
1. *Identify only the rows corresponding to the following variables* (inexact string matches are permissable, but they must represent the exact same underlying economic variable):
  - Real GDP (NOT YoY) -> `gdp`
  - Real Consumer Spending -> `pce`
  - Residential Investment -> `pdir`
  - Nonresidential Investment -> `pdin`
  - Total Government Spending -> `govt`
  - Exports -> `ex`
  - Imports -> `im`
  - Unemployment Rate -> `unemp`
  - Labor Force Participation Rate -> `lfpr`
  - Core PCEPI -> `pcepi`
  - Fed Funds Rate (%) -> `ffr`
2. *Transform dates* into YYYY[Q]QQ format, e.g. `2026Q4`.
3. *Extract values* from those dates: strip commas, asterisks, and percent signs; keep numeric precision as shown.

# Response format
Return them in a JSON object in the following format:
{"extractions": [
  {"variable": "ffr", "forecasts": [{"date": "2025Q1", "value": 2.55}, ...]
  ...
]}

# Guidelines:
- Be extremely careful to extract the EXACT numbers from the image. Double check EVERY EXTRACTION carefully.
- Omit any quarter whose cell is blank or an otherwise invalid numeric intentity.
- The valid variable names should only be from the list of variables I provided previously.
- Only return quarterly-frequency forecasts; ignore year-level forecasts, forecasts, and any information outside the table.
""".strip()
base_prompt = [{'role': 'system', 'content': system_prompt}]
full_prompt = base_prompt + [{'role': 'user', 'content': [
    {
        'type': 'image_url',
        'image_url': {
            'url': 'data:image/webp;base64,' + resize_image(
                img_b64,
                width = 1000,
                quality = None,
                output_format = 'webp'
            )['image']
        }
    }
]}]

all_prompts = [full_prompt] * LLM_ATTEMPTS

#%% ------- Send LLM requests
def parse_response(llm_response, attempt_index):
    try:
        usage = llm_response.get('usage', {})
        parsed_extractions = json.loads(llm_response['choices'][0]['message']['content'])['extractions']
        extractions_df = pd.concat([
            pd.DataFrame(v['forecasts'])\
                .assign(
                    varname = v['variable'],
                    attempt_index = attempt_index
                )
            for v in parsed_extractions
        ])
        return {
            'llm_response': llm_response,
            'input_tokens': usage['prompt_tokens'],
            'output_tokens': usage['completion_tokens'],
            'extractions': extractions_df,
            'llm_response_err': None
        }
    except Exception as e:
        return {
            'llm_response': llm_response,
            'llm_response_err': str(e)
        }

raw_llm_responses = async_run(get_llm_responses_openai(
    all_prompts,
    params = {
        'model': 'o4-mini',
        'response_format': {'type': 'json_object'},
        'reasoning_effort': 'high'
    },
    batch_size = 3,
    max_retries = 2,
    api_key = os.getenv('OPENAI_API_KEY')
))

parsed_results = [
    parse_response(llm_response, i)
    for i, llm_response in enumerate(raw_llm_responses)
]

results_df = pd.concat([x['extractions'] for x in parsed_results])

for x in parsed_results:
    if x['llm_response_err'] is not None:
        print(x)
        raise Exception('LLM response error')

#%% ------- Checks
# 1. Check first that for each LLM attempt, each (date, varname) is returned only a single time (no duplicates)
date_varname_attempt_counts =\
    results_df\
    .groupby(['date', 'varname', 'attempt_index'], as_index = False)\
    .agg(n_values = ('value', 'count'))\
    .pipe(lambda df: df[df['n_values'] > 1])

if len(date_varname_attempt_counts) > 0:
    print(date_varname_attempt_counts)
    raise Exception('Multiple vals for single (date, varname, attempt_index)')

# 2a. Aggregate to date, varname level
date_varname_counts =\
    results_df\
    .groupby(['date', 'varname'], as_index = False)\
    .agg(
        n_values = ('value', 'count'),
        n_unique_values = ('value', 'nunique')
    )

# 2b. Check that for each (date, varname), all LLM responses returned the SAME value
nonunique_date_varnames = date_varname_counts.pipe(lambda df: df[df['n_unique_values'] > 1])

if len(nonunique_date_varnames) > 0:
    print(nonunique_date_varnames)
    raise Exception('Different values for single (date, varname)')

# 2c. Check that for each (date, varname), there are at least LLM_ATTEMPT values
missing_date_varnames = date_varname_counts.pipe(lambda df: df[df['n_values'] != LLM_ATTEMPTS])

if len(missing_date_varnames) > 0:
    print(missing_date_varnames)
    raise Exception('Exiting early, some LLM attempts missing (date, varname)')

# 3. Check each varname has the SAME amount of dates
varname_n_dates =\
    date_varname_counts\
    .groupby('varname', as_index = False)\
    .agg(all_values = ('date', 'nunique'))

if len(set(varname_n_dates['all_values'].tolist())) > 1:
    print(varname_n_dates)
    raise Exception('Some varnames have more values than others')

# 4. Check that the list of extracted varnames is equal to the last of desired varnames
response_varnames = varname_n_dates['varname'].tolist()
desired_varnames = ['gdp', 'pce', 'pdir', 'pdin', 'govt', 'ex', 'im', 'unemp', 'lfpr', 'pcepi', 'ffr']
if set(response_varnames) - set(desired_varnames):
    print('Extra extractions:', sorted(set(response_varnames) - set(desired_varnames)))
if set(desired_varnames) - set(response_varnames):
    print('Missing extractions:', sorted(set(desired_varnames) - set(response_varnames)))

#%% ------- Cost
INPUT_COST_PER_1M = 1.1
OUTPUT_COST_PER_1M = 4.4

total_cost = np.round(np.sum([
    INPUT_COST_PER_1M/1_000_000 * x['input_tokens'] + OUTPUT_COST_PER_1M/1_000_000 * x['output_tokens']
    for x in parsed_results
]), 2)

print(f'Total cost: ${total_cost:.2f}')
validation_log['total_cost'] = total_cost

#%% ------ Prepare final output

# Also filter out forecasts are 6 montsh before vdate
cb_forecasts =\
    pd.concat([x['extractions'] for x in parsed_results])\
    .groupby(['date', 'varname'], as_index = False)\
    .agg(value = ('value', 'mean'))\
    .assign(
        forecast = 'cb',
        form = 'd1',
        freq = 'q',
        varname = lambda df: df['varname'],
        vdate = available_vdate,
        date = lambda df: pd.PeriodIndex(df['date'], freq = 'Q').start_time,
        value = lambda df: df['value']
    )\
    .pipe(lambda df: df[
        df['date'] > pd.Timestamp(last_stored_vdate) - pd.DateOffset(months = 6)
    ])\
    .assign(
        date = lambda df: df['date'].dt.strftime('%Y-%m-%d')
    )\
    [['forecast', 'form', 'freq', 'varname', 'vdate', 'date', 'value']]

#%% ------- Write to SQL
if len(cb_forecasts) > 0:
    today_string = datetime.now(ZoneInfo('US/Eastern')).strftime('%Y-%m-%d')

    df_to_write =\
        cb_forecasts\
        .assign(mdate = today_string)\
        [['forecast', 'vdate', 'freq', 'form', 'varname', 'date', 'value', 'mdate']]
	
    existing_forecast_combinations = get_postgres_query(
        'SELECT forecast, varname, vdate FROM forecast_values_v2 GROUP BY 1, 2, 3'
    )

    store_df = anti_join(df_to_write, existing_forecast_combinations, on = ['forecast', 'varname', 'vdate'])

    rows = write_postgres_df(
        store_df, 
        'forecast_values_v2', 
        """
        ON CONFLICT (mdate, forecast, vdate, form, freq, varname, date) 
        DO UPDATE SET value=EXCLUDED.value
        """,
        split_size = 1_000
    )
    execute_postgres_query('REFRESH MATERIALIZED VIEW CONCURRENTLY forecast_values_v2_all')
    execute_postgres_query('REFRESH MATERIALIZED VIEW CONCURRENTLY forecast_values_v2_latest')

    validation_log['rows_added'] = rows

else:
    validation_log['rows_added'] = 0