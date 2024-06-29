import subprocess
import sys
import warnings
import os
import logging
from datetime import date, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
warnings.filterwarnings('ignore')

#specify project root and files to execute
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
pull_restaurant_data_path = os.path.join(PROJECT_ROOT, 'src', 'yelp_pull_restaurant_data.py')
load_restaurant_data_path = os.path.join(PROJECT_ROOT, 'src', 'load_restaurant_data.py')
data_analysis_path = os.path.join(PROJECT_ROOT, 'src', 'Restaurant_Data_Analysis.py')

logging.basicConfig(filename='etl.log', level=logging.INFO)

def run_script(script_path, city, state) -> None:
    command = [sys.executable, script_path, "--city", city, "--state", state]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_path}: {e}")

def get_restaurant_data() -> None:
    logging.info("Starting ETL Process...")
    run_script(pull_restaurant_data_path)
    logging.info("Data retrieval completed.")

def load_restaurant_data() -> None:
    logging.info("Starting ETL Process...")
    run_script(load_restaurant_data_path)
    logging.info("Data retrieval completed.")

def run_data_analysis() -> None:
    logging.info("Starting ETL Process...")
    run_script(data_analysis_path)
    logging.info("Data retrieval completed.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
}

dag = DAG(
    'restaurant_data_pipeline',
    default_args=default_args,
    schedule='0 12 * * *',  # every day at noon
)

get_restaurant_data_task = PythonOperator(
    task_id='get_restaurant_data',
    python_callable=get_restaurant_data,
    dag=dag
)

load_restaurant_data_task = PythonOperator(
    task_id='load_restaurant_data',
    python_callable=load_restaurant_data,
    dag=dag
)

run_data_analysis_task = PythonOperator(
    task_id='run_data_analysis',
    python_callable=run_data_analysis,
    dag=dag
)

get_restaurant_data_task >> load_restaurant_data_task >> run_data_analysis_task

def main():
    if len(sys.argv) != 3:
        print("Usage: python main_script.py <city> <state>")
        sys.exit(1)

    city = sys.argv[1]
    state = sys.argv[2]

    print(f"Pulling data for {city}, {state}...")
    run_script(pull_restaurant_data_path, city, state)
    
    print(f"Loading data into database...")
    run_script(load_restaurant_data_path, city, state)

    print(f"Running data analysis...")
    run_script(data_analysis_path, city, state)

    print("ETL Process Finished.")
    sys.exit(1)
    
if __name__ == "__main__":
    main()
