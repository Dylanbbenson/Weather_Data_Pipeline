from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(repo_path)

# import functions
from src.retrieve_data import retrieve_and_upload_weather_data
from src.forecast_weather import (
    retrieve_all_data,
    retrieve_todays_data,
    save_full_historical_to_csv,
    append_today_to_csv,
    load_cached_csv,
    forecast_data,
    upload_forecast_to_s3,
)

#define dag
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='daily_weather_forecast_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Fetch daily weather data and run a 7-day temperature forecast',
)

fetch_and_upload_weather = PythonOperator(
    task_id='fetch_and_upload_weather',
    python_callable=retrieve_and_upload_weather_data,
    dag=dag,
)

def update_local_weather_csv():
    from pathlib import Path
    base_path = Path(__file__).resolve().parent.parent
    local_csv = base_path / 'data' / 'historical_weather.csv'

    if not local_csv.exists():
        full_df = retrieve_all_data()
        save_full_historical_to_csv(full_df)
    else:
        today_df = retrieve_todays_data()
        append_today_to_csv(today_df)

update_weather_cache = PythonOperator(
    task_id='update_weather_cache',
    python_callable=update_local_weather_csv,
    dag=dag,
)

def run_forecast():
    all_data = load_cached_csv()
    forecast_df = forecast_data(all_data)
    upload_forecast_to_s3(forecast_df)

run_forecast_task = PythonOperator(
    task_id='run_forecast',
    python_callable=run_forecast,
    dag=dag,
)

fetch_and_upload_weather >> update_weather_cache >> run_forecast_task
