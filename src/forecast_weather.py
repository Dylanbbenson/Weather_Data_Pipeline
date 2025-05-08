import pandas as pd
from datetime import datetime, timedelta
from prophet import Prophet
import boto3
from pathlib import Path
import json
import io
from sqlalchemy import create_engine

# config
today = datetime.today().strftime('%Y-%m-%d')
one_year_ago = (datetime.today() - timedelta(days=365)).strftime('%Y%m%d')
base_path = Path(__file__).resolve().parent.parent
local_csv = base_path / 'data' / 'historical_weather.csv'

#credentials
with open(base_path / 'config' / 'credentials.json') as f:
    credentials = json.load(f)

S3_BUCKET = credentials['bucket']
S3_ACCESS_KEY = credentials['athena_access_key']
S3_SECRET_KEY = credentials['athena_secret_key']
ATHENA_DB = credentials['database']
S3_QUERY_RESULTS = f"s3://{S3_BUCKET}/athena-results/"
REGION = credentials['region']

s3 = boto3.client("s3", region_name=REGION)

def get_athena_engine():
    return create_engine(
        f'awsathena+rest://@athena.{REGION}.amazonaws.com:443/{ATHENA_DB}?s3_staging_dir={S3_QUERY_RESULTS}',
        connect_args={
            "aws_access_key_id": S3_ACCESS_KEY,
            "aws_secret_access_key": S3_SECRET_KEY
        }
    )

def retrieve_past_year_data():
    engine = get_athena_engine()
    query = f"""
        SELECT
          date(COALESCE(try(date_parse(date, '%%Y-%%m-%%d')), try(date_parse(date, '%%Y%%m%%d'))) ) AS date,
          temperature
        FROM weather.processed
        WHERE COALESCE(try(date_parse(date, '%%Y-%%m-%%d')), try(date_parse(date, '%%Y%%m%%d'))) >=
              COALESCE(try(date_parse('{one_year_ago}', '%%Y-%%m-%%d')), try(date_parse('{one_year_ago}', '%%Y%%m%%d')))
          AND temperature IS NOT NULL
        ORDER BY 1
    """
    df = pd.read_sql(query, engine).dropna()
    return df

def retrieve_all_data():
    engine = get_athena_engine()
    query = """
        SELECT
          date(COALESCE(try(date_parse(date, '%%Y-%%m-%%d')), try(date_parse(date, '%%Y%%m%%d'))) ) AS date,
          temperature
        FROM weather.processed
        WHERE temperature IS NOT NULL
        ORDER BY 1
    """
    df = pd.read_sql(query, engine).dropna()
    return df

def retrieve_todays_data():
    engine = get_athena_engine()
    query = """
        SELECT
          date(COALESCE(try(date_parse(date, '%%Y-%%m-%%d')), try(date_parse(date, '%%Y%%m%%d'))) ) AS date,
          temperature
        FROM weather.processed
        WHERE date = date_format(current_date, '%%Y%%m%%d')
    """
    df = pd.read_sql(query, engine).dropna()
    return df

def save_full_historical_to_csv(df: pd.DataFrame):
    df.to_csv(local_csv, index=False)

def append_today_to_csv(df: pd.DataFrame):
    df.to_csv(local_csv, mode='a', index=False, header=False)

def load_cached_csv() -> pd.DataFrame:
    return pd.read_csv(local_csv)

def forecast_data(df: pd.DataFrame) -> pd.DataFrame:
    df_prophet = df.rename(columns={'date': 'ds', 'temperature': 'y'})
    model = Prophet(daily_seasonality=True)
    model.fit(df_prophet)
    future = model.make_future_dataframe(periods=7)
    forecast = model.predict(future)
    forecast = forecast[forecast['ds'] >= pd.to_datetime(today)]
    result = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
    result['run_date'] = today
    return result

def upload_forecast_to_s3(df: pd.DataFrame):
    file_name = f'forecast/date={today}/forecast.csv'
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    s3.upload_fileobj(csv_buffer, S3_BUCKET, file_name)

if __name__ == '__main__':
    if not local_csv.exists():
        print("CSV not found. Saving full historical data.")
        full_df = retrieve_all_data()
        save_full_historical_to_csv(full_df)
    else:
        print("CSV found. Appending today's data.")
        today_df = retrieve_todays_data()
        append_today_to_csv(today_df)

    all_data = load_cached_csv()
    forecast_df = forecast_data(all_data)
    upload_forecast_to_s3(forecast_df)
    print("Pipeline complete.")