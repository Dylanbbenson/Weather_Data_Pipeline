import boto3
import time
import json
from pathlib import Path
import csv
import os
from datetime import datetime
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, CreateMode, TableName, Inserter, HyperException

#load credentials and config
credentials_path = Path(__file__).resolve().parent.parent / 'config' / 'credentials.json'
with open(credentials_path) as f:
    credentials = json.load(f)

client = boto3.client("athena", region_name="us-east-2")
s3_client = boto3.client("s3", region_name="us-east-2")
ATHENA_DATABASE = credentials['database']
aws_bucket = credentials['bucket']
ATHENA_OUTPUT_BUCKET = f"s3://{aws_bucket}/athena-results/"
LOCAL_QUERY_RESULTS_PATH = f"./query_results.csv"

ATHENA_QUERY = """
select 
date,temperature,forecast_desc,dew_point,heat_index,relative_humidity,pressure,visibility,wind_chill,wind_direction,wind_direction_cardinal,gust,wind_speed,total_precipitation,total_snow,uv,uv_index,feels_like
from processed order by date;
"""
HYPER_FILE_PATH = "weather.hyper"
if os.path.exists(HYPER_FILE_PATH):
    os.remove(HYPER_FILE_PATH)

def download_csv_from_s3():
    s3_client.download_file(
        Bucket=ATHENA_OUTPUT_BUCKET,
        Key=f"athena-results/query_results.csv",
        Filename=LOCAL_QUERY_RESULTS_PATH
    )
    print(f"Downloaded CSV from S3 to {LOCAL_QUERY_RESULTS_PATH}")

def preprocess_row(row):
    def safe_float(val):
        try:
            return float(val) if val.strip() else None
        except ValueError:
            return None

    def safe_int(val):
        try:
            return int(val) if val.strip() else None
        except ValueError:
            return None

    def safe_str(val):
        try:
            return str(val) if str.strip() else None
        except ValueError:
            return None

    def format_date(date_str):
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}" if date_str else None

    return [
        format_date(row["date"]),
        safe_int(row["Temperature"]),
        safe_str(row["forecast_desc"]) or "Unknown",
        safe_float(row["dew_point"]),
        safe_float(row["heat_index"]),
        safe_float(row["relative_humidity"]),
        safe_float(row["pressure"]),
        safe_float(row["visibility"]),
        safe_float(row["wind_chill"]),
        safe_int(row["wind_direction"]),
        safe_str(row["wind_direction_cardinal"]) or "Unknown",
        safe_float(row["gust"]),
        safe_float(row["wind_speed"]),
        safe_float(row["total_precipitation"]),
        safe_float(row["total_snow"]),
        safe_str(row["uv"]) or "Unknown",
        safe_float(row["uv_index"]),
        safe_int(row["feels_like"])
    ]

def query_athena(query, database, output_bucket):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_bucket},
    )
    query_execution_id = response["QueryExecutionId"]

    while True:
        status = client.get_query_execution(QueryExecutionId=query_execution_id)["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        print("Waiting for query to complete...")
        time.sleep(1)

    if status != "SUCCEEDED":
        raise Exception(f"Query failed with status: {status}")

    results_location = \
    client.get_query_execution(QueryExecutionId=query_execution_id)["QueryExecution"]["ResultConfiguration"][
        "OutputLocation"]
    return results_location


def download_results(s3_url, local_path="query_results.csv"):
    bucket_name, key = s3_url.replace("s3://", "").split("/", 1)
    s3_client.download_file(bucket_name, key, local_path)
    print(f"Downloaded results to {local_path}")
    return local_path


def create_hyper_file(csv_file_path, hyper_file_path):
    def process_row(row):
        return [
            datetime.strptime(row[0], "%Y%m%d").date() if row[0] else None,  # date
            int(row[1]) if row[1] else None,     #temperature
            row[2],                              #forecast_desc
            float(row[3]) if row[3] else None,   #dew_point
            float(row[4]) if row[4] else None,   #heat_index
            float(row[5]) if row[5] else None,   #relative_humidity
            float(row[6]) if row[6] else None,   #pressure
            float(row[7]) if row[7] else None,   #visibility
            float(row[8]) if row[8] else None,   #wind_chill
            int(row[9]) if row[9] else None,     #wind_direction
            row[10],                             #wind_direction_cardinal
            float(row[11]) if row[11] else None, #gust
            float(row[12]) if row[12] else None, #wind_speed
            float(row[13]) if row[13] else None, #total_precipitation
            float(row[14]) if row[14] else None, #total_snow
            row[15],                             #uv
            float(row[16]) if row[16] else None, #uv_index
            int(row[17]) if row[17] else None,   #feels_like
        ]

    # Start the Hyper process
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper
        with Connection(
            endpoint=hyper.endpoint,
            database=hyper_file_path,
            create_mode=CreateMode.CREATE_AND_REPLACE,
        ) as connection:
            print(f"Successfully created Hyper file: {hyper_file_path}")

            #define table structure
            table_definition = TableDefinition(
                table_name="Extract.WeatherData",
                columns=[
                    TableDefinition.Column("date", SqlType.date()),
                    TableDefinition.Column("temperature", SqlType.double()),
                    TableDefinition.Column("forecast_desc", SqlType.text()),
                    TableDefinition.Column("dew_point", SqlType.double()),
                    TableDefinition.Column("heat_index", SqlType.double()),
                    TableDefinition.Column("relative_humidity", SqlType.double()),
                    TableDefinition.Column("pressure", SqlType.double()),
                    TableDefinition.Column("visibility", SqlType.double()),
                    TableDefinition.Column("wind_chill", SqlType.double()),
                    TableDefinition.Column("wind_direction", SqlType.int()),
                    TableDefinition.Column("wind_direction_cardinal", SqlType.text()),
                    TableDefinition.Column("gust", SqlType.double()),
                    TableDefinition.Column("wind_speed", SqlType.double()),
                    TableDefinition.Column("total_precipitation", SqlType.double()),
                    TableDefinition.Column("total_snow", SqlType.double()),
                    TableDefinition.Column("uv", SqlType.text()),
                    TableDefinition.Column("uv_index", SqlType.double()),
                    TableDefinition.Column("feels_like", SqlType.int())
                ]
            )

            connection.catalog.create_table(table_definition)
            print(f"Table created: {table_definition.table_name}")

            #read and insert to hyperfile
            with Inserter(connection, table_definition) as inserter:
                with open(csv_file_path, "r") as csv_file:
                    csv_reader = csv.reader(csv_file)
                    next(csv_reader) 
                    for row in csv_reader:
                        try:
                            processed_row = process_row(row)
                            inserter.add_row(processed_row)
                        except Exception as e:
                            print(f"Error processing row {row}: {e}")
                            break
                inserter.execute()
            print("Data successfully inserted into Hyper file.")

if __name__ == "__main__":
    try:
        if os.path.exists(LOCAL_QUERY_RESULTS_PATH):
            csv_file = LOCAL_QUERY_RESULTS_PATH
            print("Local csv retrieved.")

        else:
            s3_results = query_athena(ATHENA_QUERY, ATHENA_DATABASE, ATHENA_OUTPUT_BUCKET)
            csv_file = download_results(s3_results, local_path="query_results.csv")

        create_hyper_file(csv_file, HYPER_FILE_PATH)

        print("Process completed successfully!")
    except Exception as e:
        print(f"Error: {e}")
