# About

# Setup
**Initial steps:**
1. Clone the repo
2. Install R (4.3.0+) and Python (3.10+) on your system if not already.
3. Setup and launch a Postgres database. Setup a user with full access to the database. 
4. Create an `.env` file in the project directory named .env. This file should look like the below. 
```yaml
MP_DIR=/path/to/this/project/dir
DB_SERVER=your.db.ip
DB_PORT=your_db_port
DB_USERNAME=your_db_user
DB_PASSWORD=your_db_password
DB_DATABASE=your_db_name
```

**Python setup**
1. Create a Python venv in this project directory: `python3 -m venv .venv`. Use Python 3.10 or later.
2. In the venv's site-packages folder (`$PROJ_DIR/.venv/lib/pythonX.Y/site-packages`), create an `add_path.pth` file where the only content is `/path/to/this/project/dir`. This will add the the project directory to the PYTHONPATH whenever the venv is active, so that Python modules can always be loaded using a path relative to this directory, and that the env file can be read relative to this directory. You can validate this by running `import sys; print(sys.path)` when running Python3 in the activated venv.
3. Install Python libraries `pip3 install -r requirements.txt`. If this fails, try installing all necessary libraries from scratch: `pip3 install jupyter lab python-dotenv numpy pandas sqlalchemy psycopg2 torch torchaudio torchvision tqdm camelot-py opencv-python requests matplotlib`.

**R setup**
1. Create an `.Renviron` file containing the location of your project directory. You can use the bash script below.
```bash
PROJ_DIR="/path/to/this/dir"
ENV_PATH=$(Rscript -e "cat(file.path(getwd(), '.Renviron'))")
rm -rf ${ENV_PATH}
cat <<EOF >${ENV_PATH}
MP_DIR='${PROJ_DIR}'
EOF
echo "----- Complete -----"
```
You can test this by launching R and verifying that `Sys.getenv('MP_DIR')` returns the expected result.
2. Launch R and install the functions in `r_helpers` as a package. 
```R
install.packages(file.path(Sys.getenv('MP_DIR'), 'r_helpers'), repos = NULL, type = 'source')
```
This package will be available through `library(macropredictions)`.


# Usage
**Running module scripts**
1. TBD

(For internal users) You can still call modules directly for testing. However, for productionalizing, cronjobs should instead use controller.py (for Python modules) or controller.r for R modules.


# Changelog
* v3.0.0 - 2023.11+
- Migrated from previous `econforecasting-r` git repo