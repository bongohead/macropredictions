"""
Script for loading scoring models, pulling input data, running inference, and writing the results to SQL

Constants:
    @model_path: The save directory containing models, 
     where each model is a torchscript file under /{model_path}/{train_state}.pt.
    @model_defs: A list of tuples, where each tuple is a 4-tuple containing: the
     model_type, label_key, model_version, and train_state.
"""

#%% ------- Import libs
import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader
from transformers import AlbertTokenizer

import pandas as pd
from tqdm import tqdm
import os

from py_helpers.textmodels.loaders import TextDataset
from py_helpers.db import get_postgres_query, write_postgres_df, load_env

load_env()
device = torch.device('cpu')
validation_log = {}

#%% ------- Set constants
model_save_dir = os.path.join(os.getenv('MP_DIR'), 'modules', 'text_indices', 'saved_models')
model_defs = [
    ('financial_health', 'financial_sentiment', 'financial_health_v1-financial_sentiment-20231112', 'epoch_002_step_00000'),
    ('financial_health', 'employment_status', 'financial_health_v1-employment_status-20231112', 'epoch_002_step_00000')
]

#%% ------- List of unique models. Both each label_key and each model_version must be unique
model_list = [
    {
        'model_type': x[0], 'label_key': x[1],'model_version': x[2], 'train_state': x[3], 
        'model': torch.jit.load(f'{model_save_dir}/{x[2]}/{x[3]}.pt', map_location = device)
    }
    for x in model_defs
]

#%% ------- Test inference on models
tokenizer = AlbertTokenizer.from_pretrained('albert-base-v2')

ds = TextDataset(tokenizer, [
    'The job market sucks! Unemployed and broke.', 
    'Just got a big pay raise! How do start saving for retirement?',
    'I like my job, but I am underpaid. What is the best way to negotiate for a higher salary?', 
    '$1 million in credit card debt due to vaseline addiction. Should I declare bankruptcy?',
    'I just inherited $100k from my great-great grandmother! What to do with the money?',
    'Recommendations for pet insurance? Dog smells.',
    'Laid off at work, what do?'
])
dl = DataLoader(ds, batch_size = 1, shuffle = False)

for model_obj in model_list:
    print(model_obj['label_key'])
    for b in dl:
        with torch.no_grad():
            logits = model_obj['model'](b['input_ids'].to(device), b['attention_mask'].to(device))['logits'].cpu()
            probs = F.softmax(logits, dim = 1)
            result = torch.argmax(probs, dim = 1).numpy()[0]
            print(probs.numpy().flatten()[result], result)


#%% ------- Get list of datasets to be scored under each model
required_datasets = [
    f"SELECT '{m['model_type']}' AS model_type, '{m['label_key']}' AS label_key, '{m['model_version']}' AS model_version" 
    for m in model_list
]

raw_data = get_postgres_query(
    """
    /* Pulls the first instance of every post (by post id), even if scraped multiple times.
        * Then, cross joins on all desired combinations of model_type x model_versions x label_keys given above.
        * Finally, removes any posts where the existing model_type/label_key/model_version combinations already exist.
        * 
        * Notes on text_scraper_reddit_classifier_scores:
        *  - train_state is merely a descriptive column, and has no uniqueness properties.
        *  - The unique index is model_type x model_version x post_id x label_key
        */
    WITH desired_combinations_0 AS (
        SELECT
            scrape_id, post_id,
            CONCAT(TRIM(title), '\n', REGEXP_REPLACE(TRIM(selftext), '[\t\n\r]', ' ', 'g')) AS input_text,
            ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(trim(selftext), E'\\W+'), 1) * 1.33 AS n_tokens,
            ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY created_dttm) AS rn
        FROM text_scraper_reddit_scrapes 
        WHERE
            selftext IS NOT NULL
            AND source_board IN ('jobs', 'careerguidance', 'personalfinance')
            AND (
                (DATE(created_dttm) BETWEEN '2018-01-01' AND '2022-12-31' AND scrape_method = 'pushshift_backfill')
                OR (DATE(created_dttm) BETWEEN '2023-01-01' AND '2023-08-20' AND scrape_method = 'pullpush_backfill')
                OR DATE(created_dttm) > '2023-08-20'
            )
    ), desired_combinations AS (
        SELECT scrape_id, post_id, input_text, n_tokens
        FROM desired_combinations_0
        WHERE rn = 1 AND n_tokens >= 5 AND n_tokens <= 1024
    )
    SELECT a.*, b.*
    FROM desired_combinations a
    CROSS JOIN ({q}) b
    -- Anti join on any model type/version/post combinations that already exist
    LEFT JOIN text_indices_reddit_classifier_scores c
        ON b.model_type = c.model_type AND b.model_version = c.model_version AND b.model_type = c.model_type AND a.post_id = c.post_id
    WHERE c.score_id IS NULL
    LIMIT 10000
    """.format(q = '\nUNION ALL '.join(required_datasets))
)

datasets = [
    raw_data\
        .pipe(lambda df: df[(df['model_type'] == m['model_type']) & (df['label_key'] == m['label_key']) & (df['model_version'] == m['model_version'])])\
        .reset_index(drop = True)
    for m in model_list
]


#%% ------- Run inference
@torch.no_grad()
def run_inference(ts_model, input_texts: list, device, batch_size = 16):
    """
    Run model inference
    
    Params:
        @model_obj: The torchscript model object
        @input_texts: A list with input texts to run
    """
    ts_model.eval()
    ds = TextDataset(tokenizer, input_texts)
    dl = DataLoader(ds, batch_size = batch_size, shuffle = False)

    values_list = []
    probs_list = []
    
    for _, b in tqdm(enumerate(dl), total = len(dl)):
        logits = ts_model(b['input_ids'].to(device), b['attention_mask'].to(device))['logits']
        probs = F.softmax(logits, dim = 1)
        argmax = torch.argmax(probs, dim = 1)
        argmax_probs = probs.gather(1, index = argmax.view(-1, 1)).squeeze()
        
        values_list.extend(argmax.numpy().tolist())
        probs_list.extend(argmax_probs.numpy().tolist())
        
    return (values_list, probs_list)

results_list = []
for model_obj, dataset in zip(model_list, datasets):
    values_list, probs_list = run_inference(model_obj['model'], dataset['input_text'].tolist(), device)
    model_df = pd.DataFrame({
        'model_type': model_obj['model_type'],
        'model_version': model_obj['model_version'],
        'train_state': model_obj['train_state'],
        'scrape_id': dataset['scrape_id'].tolist(),
        'post_id': dataset['post_id'].tolist(),
        'label_key': model_obj['label_key'],
        'label_value': values_list,
        'label_prob': probs_list
    })
    results_list.append(model_df)

output_df = pd.concat(results_list)

#%% ------- Write to SQL
rows = write_postgres_df(
    output_df, 
    'text_indices_reddit_classifier_scores', 
    """
    ON CONFLICT (model_type, model_version, post_id, label_key) 
    DO UPDATE SET
        train_state=EXCLUDED.train_state,
        scrape_id=EXCLUDED.scrape_id,
        label_value=EXCLUDED.label_value,
        label_prob=EXCLUDED.label_prob
    """,
    split_size = 200
)

validation_log['rows_added'] = rows
