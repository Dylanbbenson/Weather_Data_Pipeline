import boto3
import requests
import concurrent.futures
from datetime import datetime, timedelta
import json

with open('./config/credentials.json') as f:
    credentials = json.load(f)

BASE_URL = "https://api.weather.com/v1/location/KFAR:9:US/observations/historical.json"
API_KEY = credentials['api_key']

#import s3
s3 = boto3.client('s3')
S3_BUCKET_NAME = credentials['bucket']

#upload json to s3
def upload_to_s3(json_data, file_name):
    try:
        s3.put_object(
            Body=json.dumps(json_data),
            Bucket=S3_BUCKET_NAME,
            Key=file_name,
            ContentType='application/json'
        )
        print(f"Uploaded {file_name} to S3")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

#fetch data for single date
def fetch_weather_data(date_str):
    params = {
        'apiKey': API_KEY,
        'units': 'e',
        'startDate': date_str,
        'endDate': date_str
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {date_str}: {e}")
        return None

#generate dates to pull data for
def generate_dates(start_date, end_date):
    delta = timedelta(days=1)
    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime("%Y%m%d")
        current_date += delta

#fetch data in parallel
def fetch_and_upload_data(start_date, end_date, max_workers=10):
    dates = list(generate_dates(start_date, end_date))

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_weather_data, date) for date in dates]
        for future, date_str in zip(concurrent.futures.as_completed(futures), dates):
            result = future.result()
            if result:
                file_name = f"raw/weather_data_{date_str}.json"
                upload_to_s3(result, file_name)

start_date = datetime(2024, 1, 1)
end_date = datetime.now()
fetch_and_upload_data(start_date, end_date, max_workers=10)

print(f"Data retrieval and upload to S3 complete.")