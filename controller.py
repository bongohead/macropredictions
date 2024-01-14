"""
Calling this controller from the command line allows you to run Python scripts (located in modules/),
but includes additional logging to a database and dynamic passing of command line arguments.

The command line args include:
- -d: The directory to the project folder
- -j: The name of the job to use for logging.
- -f: The filename to call to run the module.

For example, in the command line, call `.venv/bin/python3 controller.py -f modules/test/test-py.py -j test_py -d /project/directory/path`.
"""

import argparse
import os
import json
import psycopg2
import sys
from datetime import datetime
from dotenv import load_dotenv
from contextlib import redirect_stdout

# Load Args ---------------------------------------------------------------
parser = argparse.ArgumentParser(description='Python controller.')
parser.add_argument('-d', '--mpdir', action = 'store', default = None, type = str, help = 'The directory to the project folder', required = True)
parser.add_argument('-f', '--filename', action = 'store', default = None, type = str, help = 'The filename to call to run the module.', required = True)
parser.add_argument('-j', '--jobname', action = 'store', default = None, type = str, help = 'The name of the job to use for logging.', required = True)

args = parser.parse_args()

# Check if all arguments are provided
if not all([args.filename, args.jobname, args.mpdir]):
   print('Missing variables')
   sys.exit(1)

# Check if file and directory exist
if not os.path.isfile(args.filename) or not os.path.isdir(args.mpdir):
   print('Script file or ef directory does not exist')
   sys.exit(1)

# Load Constants ---------------------------------------------------------------
load_dotenv(os.path.join(args.mpdir, '.env'))
jobname = args.jobname
filename = args.filename
log_path = os.path.join(args.mpdir, 'logs', f'{args.jobname}.log')

print(f'jobname={jobname}\nfilename={filename}\nlog_path={log_path}')

# Run Script ---------------------------------------------------------------
start_time = datetime.now()

try:
    with open(log_path, 'w+') as f:
        with redirect_stdout(f):
            print(f'----------- START {datetime.now().strftime("%m/%d/%Y %I:%M %p")} ----------\n')
            exec(open(filename).read())
            print(f'\n----------- FINISHED {datetime.now().strftime("%m/%d/%Y %I:%M %p")} ----------')

    script_output = {
       'success': True,
       'validation_log': validation_log if 'validation_log' in globals() else None,
       'err_message': None
    }
except Exception as e:
    script_output = {
       'success': False,
       'validation_log': validation_log if 'validation_log' in globals() else None,
       'err_message': str(e)
    }

end_time = datetime.now()

# Read Log Output ---------------------------------------------------------------
with open(log_path, 'r') as file:
    stdout = file.read()

# Collect All Output ---------------------------------------------------------------
log_results = {
	'jobname': jobname,
	'is_interactive': hasattr(sys, 'ps1'),
	'is_success': script_output['success'],
	'start_dttm': start_time,
	'end_dttm': end_time,
	'run_secs': round((end_time - start_time).total_seconds()),
	'validation_log': json.dumps(script_output['validation_log'])
        if isinstance(script_output['validation_log'], list) or isinstance(script_output['validation_log'], dict)
        else script_output['validation_log'],
	'err_message': script_output['err_message'],
	'stdout': stdout
}

# Push into DB ---------------------------------------------------------------
conn = psycopg2.connect(
    database = os.getenv('DB_DATABASE'),
    user = os.getenv('DB_USERNAME'),
    password = os.getenv('DB_PASSWORD'),
    host = os.getenv('DB_SERVER')
)
cursor = conn.cursor()

log_results_clean = {}
for key, value in log_results.items():
   if value is None or value == []: log_results_clean[key] = None
   elif isinstance(value, datetime): log_results_clean[key] = value.strftime('%Y-%m-%d %H:%M:%S %Z')
   else: log_results_clean[key] = value

colnames_str = ','.join(log_results_clean.keys())
bind_str = ','.join(['%s' for _ in range(len(log_results_clean.keys()))])
query = f"INSERT INTO jobscript_runs ({colnames_str}) VALUES ({bind_str})"
print(log_results_clean)

cursor.execute(query, tuple([v for _, v in log_results_clean.items()]))
conn.commit()

cursor.close()
conn.close()
