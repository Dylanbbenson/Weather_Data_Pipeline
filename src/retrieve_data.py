import boto3
import requests
import concurrent.futures
from datetime import date
import json
today = date.today().strftime('%Y%m%d')
from pathlib import Path

#load credentials
credentials_path = Path(__file__).resolve().parent.parent / 'config' / 'credentials.json'
with open(credentials_path) as f:
    credentials = json.load(f)

BASE_URL = "https://api.weather.com/v1/location/KFAR:9:US/observations/historical.json"
API_KEY = credentials['api_key']

#initialize S3
s3 = boto3.client('s3')
S3_BUCKET_NAME = credentials['bucket']

#load JSON data to S3
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
        print(f"Error uploading {file_name} to S3: {e}")

#fetch weather data
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
        data = response.json()

        for observation in data.get('observations', []):
            observation['date'] = date_str

        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {date_str}: {e}")
        return None

#run in parallel
def fetch_and_upload_data(today, max_workers=10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future = executor.submit(fetch_weather_data, today)
        result = future.result()
        if result:
            file_name = f"raw/weather_data_{today}.json"
            upload_to_s3(result, file_name)


#fetch data in parallel
fetch_and_upload_data(today, max_workers=10)
print("Data retrieval and upload to S3 complete.")