import sys
import subprocess
import os
import logging
from datetime import date, time
current_date = date.today().strftime('%Y-%m-%d')

logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run_script(script_path, script_arg=None) -> None:
    command = [sys.executable, script_path]
    if script_arg is not None:
        command.append(script_arg)
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing {script_path}: {e}")
        print(f"Error executing {script_path}: {e}")
        sys.exit(1)


def main():
    if os.path.isfile(f"./src/retrieve_data.py") and os.path.isfile(f"./src/load_data_to_db.py"):
        #retrieve data
        logging.info("Starting ETL Process...")
        run_script('./src/retrieve_data.py')
        logging.info("Data retrieval completed.")

        #load data to internal db
        logging.info("Loading data into database...")
        run_script('./src/load_data_to_db.py', f"./data/weather_data_{current_date}.csv")
        logging.info("Data loading completed.")

        print("ETL Process Finished.")
        logging.info("ETL Process Finished.")
        sys.exit(0)
    else:
        logging.error("ERROR: ETL files not found. Exiting...")
        print("ERROR: ETL files not found. Exiting...")
        sys.exit(1)


if __name__ == "__main__":
    main()
