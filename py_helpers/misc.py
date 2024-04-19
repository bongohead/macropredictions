import pandas as pd

def chunk_list(input_list, chunk_size: int):
    """
    Split a list (or a dataframe) into chunks of max size
    
    Params
        @input_list: A list or a dataframe
        @chunk_size: The maximum size of the chunk
    """
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

def flatten_list(xss):
    return [x for xs in xss for x in xs]
    

def anti_join(x: pd.DataFrame, y: pd.DataFrame, on = None):
    """
    Performs an anti-join between two dataframes, returning all rows in x that aren't in y

    Params:
        @x: The left dataframe
        @y: The rtight dataframe
        @on: A string or list of strings providing columns to join on
    """
    # Performs an outer join with indicator
    merged = x.merge(y, how = 'outer', indicator = True, on = on)

    # Select rows where the merge indicator is 'left_only'
    anti_join_result = merged[merged['_merge'] == 'left_only'].drop('_merge', axis = 1)

    return anti_join_result

