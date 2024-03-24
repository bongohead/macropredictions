from dotenv import load_dotenv
import pandas as pd
import os
import sys

def chunk_list(input_list, chunk_size):
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

def flatten_list(xss):
    return [x for xs in xss for x in xs]

def anti_join(x: pd.DataFrame, y: pd.DataFrame, on = None):
    """
    Perform an anti-join between two dfs.

    Params:
        @x: The left dataframe
        @y: The right dataframe
        @on: A string or list of strings providing columns to join on
    """
    # Perform an outer join with indicator
    merged = x.merge(y, how = 'outer', indicator = True, on = on)

    # Select rows where the merge indicator is 'left_only'
    anti_join_result = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)

    return anti_join_result
