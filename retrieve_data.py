import json
import csv
import os
from datetime import date, time
import requests
import pandas as pd
import sqlalchemy
import streamlit
import matplotlib
current_date = date.today().strftime('%Y-%m-%d')

#get weather api key
with open('./config/credentials.json') as f:
    credentials = json.load(f)
API_KEY = credentials['dyls_weather_key']

def get_weather_data() -> json:
    r = requests.get(f"https://api.openweathermap.org/data/3.0/onecall?lat=33.44&lon=-94.04&appid={API_KEY}")
    if not r.ok:
        raise Exception("Couldn't retrieve API data")
    data = r.json()
    print("Retrieving data...")
    return data

#flatten nested json returned by api
def flatten_json(json_obj, parent_key='', sep='_') -> dict:
    items=[]
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            items.extend(flatten_json(value, new_key, sep=sep).items())
    elif isinstance(json_obj, list):
        for i, value in enumerate(json_obj):
            new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            items.extend(flatten_json(value, new_key, sep=sep).items())
    else:
        items.append((parent_key, json_obj))
    return dict(items)

def dump_data_to_files(json_data) -> None:
    #flatten json data for csv
    flattened_json = flatten_json(json_data)

    #output raw json
    with open(f"./data/weather_data_{current_date}.json", 'w') as json_f:
        json.dump(json_data, json_f)

    df = pd.DataFrame(pd.json_normalize(flattened_json))

    df = df.iloc[:,:21]   #only get necessary columns (first 21)

    if os.path.exists(f"./data/weather_data_{current_date}.csv"):
        df.to_csv(f"./data/weather_data_{current_date}.csv", index=False, mode='a', header=False)
    else:
        df.to_csv(f"./data/weather_data_{current_date}.csv", index=False, mode='w')

    print(f"JSON file saved at /data/weather_data_{current_date}.json")
    print(f"CSV file saved at /data/weather_data_{current_date}.csv")


if __name__ == '__main__':
    data = get_weather_data()
    dump_data_to_files(data)
