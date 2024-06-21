import sys
import subprocess
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import date, datetime

#set date and time variables for files
current_date = date.today().strftime('%Y-%m-%d')
current_time = datetime.now().strftime("%H")
json_file = f"./data/weather_data_{current_date}_{current_time}.json"
csv_file = f"./data/weather_data_{current_date}_{current_time}.csv"

logging.basicConfig(filename='etl.log', level=logging.INFO)

# for executing scripts
def run_script(script_path, script_arg=None) -> None:
    command = [sys.executable, script_path]
    if script_arg is not None:
        command.append(script_arg)
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing {script_path}: {e}")
        print(f"Error executing {script_path}: {e}")

def get_weather_data() -> None:
    logging.info("Starting ETL Process...")
    run_script('./src/retrieve_data.py')
    logging.info("Data retrieval completed.")

def load_weather_data_internal() -> None:
    logging.info("Loading data into database...")
    if os.path.isfile(csv_file):
        run_script('./src/load_data_to_db.py', csv_file)
    else:
        logging.error(f"ERROR: No data found. Exiting...")
        print(f"ERROR: No data found. Exiting...")
    logging.info("Internal db loading completed.")

def load_weather_data_s3() -> None:
    logging.info("Loading data into s3...")
    if os.path.isfile(json_file):
        run_script('./src/load_data_to_s3.py', json_file)
    else:
        logging.error(f"ERROR: No data found. Exiting...")
        print(f"ERROR: No data found. Exiting...")
    logging.info("S3 loading completed.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 17),
    'retries': 1,
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    schedule='0 * * * *',  # top of every hour
)

get_weather_data_task = PythonOperator(
    task_id='get_weather_data',
    python_callable=get_weather_data,
    dag=dag
)

load_data_to_s3_task = PythonOperator(
    task_id='load_weather_data_s3',
    python_callable=load_weather_data_s3,
    dag=dag
)

load_data_internal_task = PythonOperator(
    task_id='load_weather_data_internal',
    python_callable=load_weather_data_internal,
    dag=dag
)

get_weather_data_task >> load_data_to_s3_task >> load_data_internal_task
