import sys
import os
import json
import boto3
from datetime import datetime, date, time
from decouple import config
from dotenv import load_dotenv

load_dotenv(dotenv_path='./config/config.env')
bucket =config('bucket')
current_date = date.today().strftime('%Y-%m-%d')
current_time = datetime.now().strftime("%H")
json_file = f"./data/weather_data_{current_date}_{current_time}.json"
s3_key = f"current_weather_{current_date}_{current_time}.json"

def load_to_s3(json_data, bucket, key) -> bool:
    try:
        json_bytes = json.dumps(json_data).encode('utf-8')
        s3_client = boto3.client('s3')
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=json_bytes)
        print(f"JSON data pushed to S3.")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1:
        json_data = sys.argv[1]
    elif os.path.isfile(json_file):
        with open(json_file, 'r') as f:
            json_data = json.load(f)
    else:
        print(f"ERROR: No data found. Exiting...")
        exit()

    if load_to_s3(json_data, bucket, s3_key):
        print("Data loaded successfully.")
    else:
        print("Data failed to load.")
