import boto3
import requests
import concurrent.futures
from datetime import date, datetime
import json
from pathlib import Path

# Load credentials
credentials_path = Path(__file__).resolve().parent.parent / 'config' / 'credentials.json'
with open(credentials_path) as f:
    credentials = json.load(f)

BASE_URL = "https://api.weather.com/v1/location/KFAR:9:US/observations/historical.json"
API_KEY = credentials['api_key']
S3_BUCKET_NAME = credentials['bucket']

s3 = boto3.client('s3')

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

        print(f"Fetched weather data for {date_str}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {date_str}: {e}")
        return None

def upload_weather_json_to_s3(json_data, date_str):
    if not json_data:
        print(f"No data to upload for {date_str}")
        return

    file_name = f"raw/weather_data_{date_str}.json"
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

def retrieve_and_upload_weather_data(date_str=None):
    if not date_str:
        date_str = date.today().strftime('%Y%m%d')
    else:
        date_str = datetime.strptime(date_str, "%Y%m%d").strftime("%Y%m%d")

    weather_data = fetch_weather_data(date_str)
    upload_weather_json_to_s3(weather_data, date_str)