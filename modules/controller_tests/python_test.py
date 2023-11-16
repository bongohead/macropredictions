#%% 
import os
from py_helpers.db import load_env
 
load_env()

#%%
print(os.getenv('DB_SERVER'))
