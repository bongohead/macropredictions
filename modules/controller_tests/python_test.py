#%% 
import os
from py_helpers.env import load_env
validation_log = {}
load_env()

#%%
print(os.getenv('DB_SERVER'))
validation_log['test'] = 'OK'
