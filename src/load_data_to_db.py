import sys
import os
import pandas as pd
import mysql.connector
from datetime import date, time, datetime
from decouple import config
from dotenv import load_dotenv
import subprocess
import logging

load_dotenv(dotenv_path='./config/config.env')
current_date = date.today().strftime('%Y-%m-%d')
current_time = datetime.now().strftime("%H")
json_file = f"./data/weather_data_{current_date}_{current_time}.json"
csv_file = f"./data/weather_data_{current_date}_{current_time}.csv"
logging.basicConfig(filename='etl.log', level=logging.INFO)

def create_mysql_db():
    try:
        subprocess.run(["./src/create_mysql_db.sh"], check=True, text=True, capture_output=True)
    except Exception as err:
        logging.error(f"Error running bash script: {err}")
        raise

# Establish DB connection using config.env variables
def create_mysql_connection() -> mysql.connector:
    try:
        connection = mysql.connector.connect(
            host=config('DB_HOST'),
            user=config('DB_USER'),
            password=config('DB_PASSWORD'),
            database=config('DB_NAME')
        )
        return connection
    except mysql.connector.Error as err:
        print("Error:", err)
        sys.exit(1)


def load_data_to_mysql(data, connection) -> bool:
    try:
        cursor = connection.cursor()
        for index, row in data.iterrows():
            columns = ", ".join(data.columns)
            values = ", ".join(["%s" for _ in range(len(data.columns))])
            insert_query = f"INSERT INTO weather.Weather ({columns}) VALUES ({values})"
            cursor.execute(insert_query, tuple(row))

        connection.commit()
        cursor.close()
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False



if __name__ == "__main__":
    if len(sys.argv) > 1:
        csv = sys.argv[1]
        df = pd.read_csv(csv)
    elif os.path.isfile(csv_file):
        df = pd.read_csv(csv_file)
    else:
        print(f"ERROR: No data found. Exiting...")
        exit()

    df = df.fillna('')
    create_mysql_db()
    mysql_connection = create_mysql_connection()
    if load_data_to_mysql(df, mysql_connection):
        print("Data loaded successfully.")
    else:
        print("Data failed to load.")
    mysql_connection.close()
