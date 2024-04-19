from dotenv import load_dotenv 
import os 
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
    Validates that variables are defined in the system env
    
    Params
        @variables: A list of variables to verify exist in the system env.

    Returns
        True if all variables do exist.
    """
    if any(os.environ.get(x) is None for x in variables):
        raise Exception('Missing variables in system env!')
    return True