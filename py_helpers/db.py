import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
from tqdm import tqdm
import sys 

def load_env():
    """
    Loads dotenv file by searching through your Python search paths for an .env file.

    Returns
        True if loaded successfully.
    """
    paths = list(set(
        [os.path.abspath(os.path.join(x, '.env')) for x in sys.path if os.path.isfile(os.path.join(x, '.env'))]
        ))
    
    if (len(paths) == 0):
        raise Exception('No .env file found! Make sure an .env file lives in a directory stored in the Python search path (check sys.path).')
    else:
        for path in paths:
            load_dotenv(path, override = True)
        return True

def check_env_variables(variables: list[str]):
    """
    Validates that variables are defined in the system env.
    
    Params
        @variables: A list of variables to verify exist in the system env.

    Returns
        True if all variables do exist.
    """
    if any(os.environ.get(x) is None for x in variables):
        raise Exception('Missing variables in system env!')
    return True


def get_postgres_query(query: str) -> pd.DataFrame: 
    """
    Get query result from Postgres

    Params
        @query The query to send to the Postgres database.
    
    Returns
        A pandas dataframe
    """
    check_env_variables(['DB_DATABASE', 'DB_USERNAME', 'DB_PASSWORD', 'DB_SERVER'])

    engine = create_engine(
        "postgresql+psycopg2://{user}:{password}@{host}/{dbname}".format(
           dbname = os.getenv('DB_DATABASE'),
           user = os.getenv('DB_USERNAME'),
           password = os.getenv('DB_PASSWORD'),
           host = os.getenv('DB_SERVER')
        )
    )
    
    pg = engine.connect()
    res = pd.read_sql(query, con = pg)
    pg.close()
    
    return res

def write_postgres_df(df, tablename, append = '', split_size = 1000):
    """
    Write a pandas dataframe to Postgres via an INSERT query.

    Params
        @df A pandas dataframe.
        @tablename The name of the Postgres table.
        @append Additional text to append to the end of the INSERT string.

    """
    check_env_variables(['DB_DATABASE', 'DB_USERNAME', 'DB_PASSWORD', 'DB_SERVER'])

    conn = psycopg2.connect(
        database = os.getenv('DB_DATABASE'),
        user = os.getenv('DB_USERNAME'),
        password = os.getenv('DB_PASSWORD'),
        host = os.getenv('DB_SERVER')
    )
    cursor = conn.cursor()

    dfs = split_df(df, chunk_size = split_size)
    row_added = 0

    for d in dfs:
        data = [tuple(x) for x in d.to_numpy()]
        columns = ','.join(d.columns.to_list())
        values_placeholder = ','.join(['%s' for s in range(len(d.columns.to_list()))])
        query =\
            f"""
            INSERT INTO {tablename} ({columns}) VALUES ({values_placeholder}) {append};
            """ 
        cursor.executemany(query, data)
        row_added += cursor.rowcount
        conn.commit()

    cursor.close()
    conn.close()
    
    return row_added

def execute_postgres_query(query:str) -> bool:
    """
    Execute a single Postgres query.

    Params
        @tablename The name of the Postgres materialized view.
    """
    check_env_variables(['DB_DATABASE', 'DB_USERNAME', 'DB_PASSWORD', 'DB_SERVER'])

    conn = psycopg2.connect(
        database = os.getenv('DB_DATABASE'),
        user = os.getenv('DB_USERNAME'),
        password = os.getenv('DB_PASSWORD'),
        host = os.getenv('DB_SERVER')
    )
    cursor = conn.cursor()    
    cursor.execute(query)
    cursor.close()
    conn.close()

    return True


def split_df(df, chunk_size = 200):
   """
   Split a dataframe into chunks
   """
   chunks = []
   num_chunks = len(df) // chunk_size + 1
   for i in range(num_chunks):
       chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
   return chunks

