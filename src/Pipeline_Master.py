import sys
import subprocess
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import date, datetime, timedelta
import boto3

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

def get_weather_data():
    logging.info("Starting ETL Process...")
    run_script('./src/retrieve_data.py')
    logging.info("Data retrieval completed.")

def store_weather_data_internal() -> None:
    logging.info("Loading data into database...")
    if os.path.isfile(f"./data/weather_data_{current_date}.csv"):
        run_script('./src/load_data_to_db.py', f"./data/weather_data_{current_date}.csv")
    else:
        print(f"ERROR: No data found. Exiting...")
        exit()
    logging.info("Internal db loading completed.")

def store_weather_data_s3(json_data) -> None:
    logging.info("Loading data into s3...")
    if os.path.isfile(f"./data/weather_data_{current_date}.json"):
        run_script('./src/load_data_to_s3.py', f"./data/weather_data_{current_date}.json")
    else:
        run_script('./src/load_data_to_s3.py', json_data)
    logging.info("S3 loading completed.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,6,17),
    'retries': 1,
    #'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    schedule='*/10 * * * *', #every 10 minutes
)

get_weather_data_task = PythonOperator(
    task_id='get_weather_data',
    python_callable=get_weather_data,
    dag=dag
)

load_data_to_s3_task = PythonOperator(
    task_id='store_weather_data_s3',
    python_callable=store_weather_data_s3,
    dag=dag
)

load_data_internal_task = PythonOperator(
    task_id='store_weather_data_internal',
    python_callable=store_weather_data_internal,
    dag=dag
)

if os.path.isfile(f"./src/retrieve_data.py") and os.path.isfile(f"./src/load_data_to_db.py") and os.path.isfile(
        f"./src/load_data_to_s3.py"):
    # Run pipeline master task
    get_weather_data_task >> load_data_to_s3_task >> load_data_internal_task

    print("ETL Process Finished.")
    logging.info("ETL Process Finished.")
    sys.exit(0)
else:
    logging.error("ERROR: ETL files not found. Exiting...")
    print("ERROR: ETL files not found. Exiting...")
    sys.exit(1)
