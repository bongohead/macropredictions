"""
Script for scraping Wells Fargo economic forecasts and reading them via OCR model.
"""

#%% ------- Load libs
from pathlib import Path
import re
import pandas as pd
import numpy as np
from tqdm import tqdm
import datetime
import tempfile
import pytz

from cairosvg import svg2png
import easyocr
from PIL import Image

import requests
from bs4 import BeautifulSoup

from py_helpers.db import load_env, get_postgres_query, write_postgres_df, execute_postgres_query
from py_helpers.ocr.easyocr import easyocr_to_df, draw_easyocr_output, get_bb_darkness

DIR = str(Path(__file__).parent.absolute())
reader = easyocr.Reader(['en'], gpu = False)
load_env()

validation_log = {}
data_dump = {}

#%% ------- Download any new SVGs that don't already exist in a folder
def download_raw_svgs():
    
    # Get list of pages
    def extract_vdate(str: str) -> str:
        match = re.search(r'\d{2}/\d{2}/\d{4}', str.strip())
        if match:
            date_str = match.group()
            dt = datetime.datetime.strptime(date_str, '%m/%d/%Y')
            formatted_date = dt.strftime('%Y-%m-%d')
            return formatted_date
        else:
            raise Exception ('No date found!')

    def get_pages() -> list[dict]:
        resp = requests.get('https://www.wellsfargo.com/cib/insights/economics/us-outlook/')
        soup = BeautifulSoup(resp.text, 'html.parser')
        li_els = soup.find('div', {'id': 'contentBody'}).find_all('li')
        return [
            {'vdate': extract_vdate(li.getText()), 'href': li.find('a').get('href')}
            for li in li_els
        ]
    
    svg_files = [f.stem for f in Path(DIR + '/wfc-dump/svgs').glob('*.svg')]
    pages_with_link = [p for p in get_pages() if p['vdate'] not in svg_files]

    # Get link
    def get_svg_link(url: str) -> str:
        resp = requests.get(url)
        soup = BeautifulSoup(resp.text, 'html.parser')
        h3_el = soup.find('h3', string = 'U.S. Forecast Table')
        src = h3_el.find_next('table', class_ = 'bm-img-table').find('img').get('bm-img-svg')
        abs_src = 'https://wellsfargo.bluematrix.com/images/' + src.split('image:')[1]
        return abs_src
    
    pages_with_img_link = [{**p, 'img_href': get_svg_link(p['href'])} for p in pages_with_link]

    # Download
    downloaded_svgs = []
    for p in pages_with_img_link:
        img_resp = requests.get(p['img_href'])
        path = DIR + '/wfc-dump/svgs/' + p['vdate'] + '.svg'
        with open(path, 'wb') as f:
            f.write(img_resp.content)
            downloaded_svgs.append(path)

    return downloaded_svgs

new_svgs = download_raw_svgs()
print(f'***Downloaded {len(new_svgs)} SVGs: {", ".join([Path(s).stem for s in new_svgs])}')


#%% ------- Extract and convert SVGs to PNGs that don't already exist in PNG dump
def convert_svgs_to_pngs():

    scale = 5 

    exists = [f.stem for f in Path(DIR + '/wfc-dump/pngs').glob('*.png')]
    files_to_convert = sorted([f.stem for f in Path(DIR + '/wfc-dump/svgs').glob('*.svg') if f.stem not in exists])

    # Convert & grayscale
    converted_pngs = []
    for file_to_convert in files_to_convert:
        temp_path = tempfile.NamedTemporaryFile(suffix = '.png')
        png_path = DIR + '/wfc-dump/pngs/' + file_to_convert + '.png'
        try: 
            # Create initial PNG
            with open(DIR + '/wfc-dump/svgs/' + file_to_convert + '.svg', 'r') as f:
                svg_data = f.read()
            svg2png(svg_data, write_to = temp_path, scale = scale)
            init_png = Image.open(temp_path)
            # Clean
            img_np = np.array(init_png.convert('L'))
            Image.fromarray(img_np).save(png_path)
            converted_pngs.append(png_path)
        finally:
            temp_path.close()

    return converted_pngs

new_pngs = convert_svgs_to_pngs()
print(f'***Converted {len(new_pngs)} PNGs: {", ".join([Path(s).stem for s in new_pngs])}')

#%% ------- Get list of vdates to run OCR on (any vdates with PNGs but has at least one missing variable in SQL)
# Desired variables
varnames_map = pd.DataFrame.from_records(
    data = [
        ('real gross domestic product', 'gdp'),
        ('personal consumption', 'pce'),
        ('core consumer price index', 'cpi'),
        ('pce deflator', 'pcepi'),
        ('unemployment rate', 'unemp'),
        ('housing starts', 'houst'),
        ('federal funds target rate', 'ffr'),
        ('secured overnight financing rate', 'sofr'),
        # ('prime rate', 'primerate'),
        ('conventional mortgage rate', 'mort30y'),
        ('3 month bill', 't03m'),
        ('6 month bill', 't06m'),
        ('1 year bill', 't01y'),
        ('2 year note', 't02y'),
        ('5 year note', 't05y'),
        ('10 year note', 't10y'),
        ('30 year bond', 't30y')
    ],
    columns = ['rn', 'varname']
)

desired_vdates = [f.stem for f in Path(DIR + '/wfc-dump/pngs').glob('*.png')]
desired_data = varnames_map[['varname']].merge(pd.DataFrame({'vdate': desired_vdates}), how = 'cross')

# Existing data
existing_data = get_postgres_query(
    f"""
    SELECT CAST(vdate AS VARCHAR) AS vdate, varname
    FROM forecast_values_v2
    WHERE 
        forecast = 'wfc' 
        AND varname IN ({','.join(["'" + x + "'" for x in varnames_map['varname'].tolist()])})
    GROUP BY 1, 2
    """
)

# Anti join desired data to existing data - returns vdates with ANY missing varname
parse_vdates =\
    desired_data\
    .merge(existing_data, how = 'outer', indicator = True, on = ['vdate', 'varname'])\
    .pipe(lambda df: df[df._merge == 'left_only'].drop('_merge', axis = 1))\
    .sort_values(by = 'vdate')\
    ['vdate']\
    .drop_duplicates()\
    .tolist()[0:3]

print(f"***Get data for: {', '.join(parse_vdates)}")

#%% ------- Iterate and convert PNGs to dataframes via OCR
print(f'***Starting OCR')
final_forecasts = []
for i, vdate in tqdm(enumerate(parse_vdates)): 

    print(f'*****OCR for: {vdate}')

    path = f'{DIR}/wfc-dump/pngs/{vdate}.png'
    img = np.array(Image.open(path))

    ### First pass - strip outside borders ###### 

    ocr_df = easyocr_to_df(reader.readtext(
        image = img, min_size = 50, mag_ratio = 4, # Only get big texts on this pass
        slope_ths = .25, contrast_ths = .01, adjust_contrast = .75,
        add_margin = .25,
        ycenter_ths = 0.75, # .25 maximum shift in y direction
        width_ths = 1.75, # maxmimum horizontal distance to merge boxes; 0.5 default,
        height_ths = 1.0 # maximum difference in y height to shift boxes
    ))

    # draw_easyocr_output(img, ocr_df, f'{DIR}/wfc-dump/parse/{vdate}-00-prestrip.png')

    # Get the upper and right bounding box; second instance of word "actual" 
    if len(ocr_df[ocr_df['text'] == 'Actual']) != 2: 
        raise Exception('Unable to find upper & right bound - count of "actual" not 2')

    actual_box =\
        ocr_df[ocr_df['text'] == 'Actual']\
        .pipe(lambda df: df[df['tl_x'] == df['tl_x'].max()])\
        .to_dict('records')[0]
    
    # Get the bottom bounding box; first instance of the word "bond"
    if len(ocr_df[ocr_df['text'].str.contains('Bond')]) != 1: 
        raise Exception('Unable to find bottom bound - count of "bond" not 1')

    bond_box =\
        ocr_df\
        .pipe(lambda df: df[df['text'].str.contains('Bond')])\
        .to_dict('records')[0]
    
    # Removal region
    img2 = img.copy()[(actual_box['bl_y'] - 15):(bond_box['bl_y'] + 15), 5:(actual_box['bl_x'] - 30)]

    Image.fromarray(img2).save(f'{DIR}/wfc-dump/parse/{vdate}-01-stripped.png')
    
    #### Second pass - get rownames and colnames ####    
    # Wide boxes to capture 
    ocr_df2 = easyocr_to_df(reader.readtext(
        image = img2, min_size = 20, mag_ratio = 4,
        slope_ths = .25, contrast_ths = .01, adjust_contrast = .75,
        add_margin = .3, ycenter_ths = 0.75, width_ths = 2.00, height_ths = 1.00
    ))

    draw_easyocr_output(img2, ocr_df2, f'{DIR}/wfc-dump/parse/{vdate}-02-rownames-colnames.png')
    
    # Detect year and quarter rows - must be in top 200 px at top
    years = ocr_df2[
        (ocr_df2['cy'] <= 200) & 
        (ocr_df2['text'].str.match(r'^(' + '|'.join([str(y) for y in range(2020, 2099)]) + ')$'))
    ]
    quarters = ocr_df2[
        (ocr_df2['cy'] <= 200) & 
        (ocr_df2['text'].str.match(r'^([1-4][0OQ])$'))
    ]

    if (np.abs(years['c_y'].max() - years['c_y'].min()) >= 10):
        print(years)
        raise Exception('Y-axis discrepancy of years too large')
    
    if (np.abs(quarters['c_y'].max() - quarters['c_y'].min()) >= 10):
        print(quarters)
        raise Exception('Y-axis discrepancy of quarters too large')

    # Get closest years to each row
    colnames = \
        quarters\
        .merge(years[['text', 'c_x']].rename(columns = {'c_x': 'year_x', 'text': 'year'}), how = 'cross')\
        .assign(diff = lambda df: np.abs(df['c_x'] - df['year_x']))\
        .pipe(lambda df: df[df.groupby('ix', group_keys=False)['diff'].transform('min') == df['diff']])\
        .assign(text = lambda df: df['year'] + 'Q' + df['text'].apply(lambda x: x[0]))\
        [['c_x', 'c_y', 'tl_x', 'tl_y', 'bl_y', 'text']]
    
    if not all(re.compile(r'\d{4}Q\d').fullmatch(s) for s in colnames['text'].tolist()):
        print(colnames)
        raise Exception('Column names are messed up!')

    def clean_varnames(text):
        text = text.lower() # Lowercase
        text = re.sub(r'\(.*?\)', '', text) # Remove everything in parentheses inclusive
        text = re.sub(r'\W+', ' ', text) # Remove all non-alphanumeric
        text = text.strip() # Remove leading and trailing spaces
        text = re.sub(r'\s+', ' ', text) # Remove multiple spaces
        return text

    # Group together close rows
    rownames =\
        ocr_df2[ocr_df2['tr_x'] < (colnames['tl_x'].min() - 10)]\
        .sort_values(by = ['c_y', 'c_x'])\
        .assign(
            chg = lambda df: df['c_y'] - df['c_y'].shift(1), 
            row_ix = lambda df: df['chg'].gt(10).astype(int).cumsum()
            )\
        .sort_values(by = ['row_ix', 'c_x'])\
        .groupby('row_ix')\
        .agg({'text': ' '.join, 'c_x': 'mean', 'c_y': 'mean', 'tr_x': 'max'})\
        .assign(text = lambda df: df['text'].apply(clean_varnames))\
        .reset_index()
    
    missing_varnames = [v for v in varnames_map['rn'].tolist() if v not in rownames['text'].tolist()]
    if len(missing_varnames) > 0:
        print('Missing varnames: ', ','.join(missing_varnames))

    # Now iterate through each column and parse
    # Split columns by taking the between-column mean, except for first and last column
    colnames_with_edges = colnames.assign(
        xstart = lambda df: (.5 * (df['c_x'] + df['c_x'].shift(1))).fillna(rownames['tr_x'].max()).astype(int),
        xend = lambda df: (.5 * (df['c_x'] + df['c_x'].shift(-1))).fillna(img2.shape[1]).astype(int),
        ystart = colnames['bl_y'].max()
        )
    
    parsed_dfs = []
    for i, col in enumerate(colnames_with_edges.to_dict('records')):
        
        img_tmp = img2[col['ystart']:, col['xstart']:col['xend']]
        ocr_tmp = easyocr_to_df(reader.readtext(
            image = img_tmp, contrast_ths = .01, adjust_contrast = .75, min_size = 30, mag_ratio = 4,
            slope_ths = .25, add_margin = .45, ycenter_ths = 0.75, width_ths = 1.50, height_ths = 2.0,
            allowlist = '0123456789.-'
        ))
        # draw_easyocr_output(img_tmp, ocr_tmp, f'{DIR}/wfc-dump/parse/{vdate}-04-col-{str(i).zfill(2)}.png')

        # Get only forecast (shaded) regions); shift coords back to proper position
        forecast_region =\
            ocr_tmp\
            .assign(
                date = col['text'],
                darkness = lambda df: df.apply(lambda x: get_bb_darkness(
                    img_tmp,
                    x['tl_x'], x['tl_y'], x['tr_x'], x['tr_y'], x['br_x'], x['br_y'], x['bl_x'], x['bl_y']
                ), axis=1),
                is_forecast = lambda df: np.where(df['darkness'].lt(240), 1, 0)
            )\
            .pipe(lambda df: df[df['is_forecast'] == 1])\
            .assign(
                c_x = lambda df: df['c_x'] + col['xstart'],
                tl_x = lambda df: df['tl_x'] + col['xstart'], tr_x = lambda df: df['tr_x'] + col['xstart'],
                br_x = lambda df: df['br_x'] + col['xstart'], bl_x = lambda df: df['bl_x'] + col['xstart'],
                c_y = lambda df: df['c_y'] + col['ystart'],
                tl_y = lambda df: df['tl_y'] + col['ystart'], tr_y = lambda df: df['tr_y'] + col['ystart'],
                br_y = lambda df: df['br_y'] + col['ystart'], bl_y = lambda df: df['bl_y'] + col['ystart'],
            )\
            [[
                'ix', 'c_y', 'c_x', 'tl_y', 'tl_x', 'tr_y', 'tr_x', 'br_y', 'br_x', 'bl_y', 'bl_x',
                'date', 'text', 'prob', 'darkness'
            ]]
                
        if len(forecast_region) == 0:
            continue

        # Get closest row to each value; map to original dimensions
        forecast_mapped =\
            forecast_region\
            .merge(rownames[['text', 'c_y']].rename(columns = {'c_y': 'rn_y', 'text': 'rn'}), how = 'cross')\
            .assign(y_diff = lambda df: np.abs(df['c_y'] - df['rn_y']))\
            .pipe(lambda df: df[df.groupby('ix', group_keys=False)['y_diff'].transform('min') == df['y_diff']])\
            .assign(value = lambda df: pd.to_numeric(df['text'], errors = 'coerce'))\
            .merge(varnames_map, on = ['rn'], how = 'inner')\
            .sort_values(by = ['varname'])

        if forecast_mapped['y_diff'].max() >= 10: 
            print(forecast_mapped[forecast_mapped['y_diff'] >= 10])
            print('There exist values with an y distance too far from any varname y!')

        parsed_dfs.append(forecast_mapped)
    
    parsed_df = pd.concat(parsed_dfs).assign(vdate = vdate)
    draw_easyocr_output(img2, parsed_df.assign(text = parsed_df['value']), f'{DIR}/wfc-dump/parse/{vdate}-03-final.png')

    final_forecasts.append(parsed_df)

if len(parse_vdates) > 0:
    final_forecast = pd.concat(final_forecasts)
else:
    final_forecast = pd.DataFrame()

#%% ------- Validate
if len(parse_vdates) > 0:

    forecast_counts = [
        x.groupby('varname').agg(count = ('ix', 'size')).reset_index().assign(vdate = vdate) 
        for vdate, x in final_forecast.groupby('vdate')
        ]

    for vdate_count in forecast_counts:
        satisfied = sum([varname in vdate_count['varname'].tolist() for varname in varnames_map['varname'].tolist()])
        not_satisfied = sum([varname not in vdate_count['varname'].tolist() for varname in varnames_map['varname'].tolist()])

        print(f"{vdate_count['vdate'][0]}: satisfied {satisfied} | not satisfied {not_satisfied}")

#%% ------- Write to SQL
if len(parse_vdates) > 0:

    utc_now = datetime.datetime.now(pytz.utc)
    est = pytz.timezone('US/Eastern')
    est_now = utc_now.astimezone(est)
    est_date = est_now.strftime('%Y-%m-%d')

    df_to_write =\
        final_forecast\
        .assign(
            forecast = 'wfc',
            freq = 'q',
            form = 'd1', 
            date = lambda df: pd.PeriodIndex(df['date'], freq='Q').to_timestamp().strftime('%Y-%m-%d'),
            mdate = est_date
        )[['forecast', 'vdate', 'freq', 'form', 'varname', 'date', 'value', 'mdate']]
    
    rows = write_postgres_df(
        df_to_write, 
        'forecast_values_v2', 
        """
        ON CONFLICT (mdate, forecast, vdate, form, freq, varname, date) 
        DO UPDATE SET
            value=EXCLUDED.value
        """,
        split_size = 200
    )
    execute_postgres_query('REFRESH MATERIALIZED VIEW CONCURRENTLY forecast_values_v2_all')
    execute_postgres_query('REFRESH MATERIALIZED VIEW CONCURRENTLY forecast_values_v2_latest')

    validation_log['rows_added'] = rows

else:
    validation_log['rows_added'] = 0