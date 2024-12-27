import boto3
import time
import json
from pathlib import Path
import csv
import os
from datetime import datetime

#load credentials and configs
credentials_path = Path(__file__).resolve().parent.parent / 'config' / 'credentials.json'
with open(credentials_path) as f:
    credentials = json.load(f)

client = boto3.client("athena", region_name="us-east-2")
s3_client = boto3.client("s3", region_name="us-east-2")
ATHENA_DATABASE = credentials["database"]
aws_bucket = credentials['bucket']
ATHENA_OUTPUT_BUCKET = f"s3://{aws_bucket}/athena-results/"
LOCAL_QUERY_RESULTS_PATH = f"./query_results.csv"
tableau_data_path = f"./tableau_data.csv"
tmp_data_path = f"./today_data.csv"

ATHENA_QUERY_ALL = """
select 
date,temperature,forecast_desc,dew_point,heat_index,relative_humidity,pressure,visibility,wind_chill,wind_direction,wind_direction_cardinal,gust,wind_speed,total_precipitation,total_snow,uv,uv_index,feels_like
from processed 
order by date;
"""

ATHENA_QUERY_TODAY = """
select 
date,temperature,forecast_desc,dew_point,heat_index,relative_humidity,pressure,visibility,wind_chill,wind_direction,wind_direction_cardinal,gust,wind_speed,total_precipitation,total_snow,uv,uv_index,feels_like
from processed 
WHERE DATE_PARSE(date, '%Y%m%d') = current_date;
"""

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
        time.sleep(2)

    if status != "SUCCEEDED":
        raise Exception(f"Query failed with status: {status}")

    results_location = \
    client.get_query_execution(QueryExecutionId=query_execution_id)["QueryExecution"]["ResultConfiguration"][
        "OutputLocation"]
    return results_location


def fix_data_types(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
        header = next(reader)
        writer.writerow(header)

        for row in reader:
            fixed_row = []
            for value in row:
                if value.isdigit():
                    fixed_row.append(int(value)) 
                else:
                    try:
                        fixed_row.append(float(value)) 
                    except ValueError:
                        fixed_row.append(value.strip())
            writer.writerow(fixed_row)


def append_today_to_csv(query_result_csv, output_csv):
    with open(output_csv, "a", newline="") as output_file, open(query_result_csv, "r") as result_file:
        output_writer = csv.writer(output_file)
        result_reader = csv.reader(result_file)

        next(result_reader)
        for row in result_reader:
            output_writer.writerow(row)
    print(f"Appended today's data to {output_csv}")


def download_results(s3_url, local_path="query_results.csv"):
    bucket_name, key = s3_url.replace("s3://", "").split("/", 1)
    s3_client.download_file(bucket_name, key, local_path)
    print(f"Downloaded results to {local_path}")
    return local_path


if __name__ == "__main__":
    try:
        if os.path.exists(tableau_data_path):
            s3_results = query_athena(ATHENA_QUERY_TODAY, ATHENA_DATABASE, ATHENA_OUTPUT_BUCKET)
            csv_file = download_results(s3_results, local_path="query_results.csv")
            fix_data_types(csv_file, tmp_data_path)
            append_today_to_csv(tmp_data_path, tableau_data_path)
            print("Today's date has been appended to existing csv.")

        else:
            s3_results = query_athena(ATHENA_QUERY_ALL, ATHENA_DATABASE, ATHENA_OUTPUT_BUCKET)
            csv_file = download_results(s3_results, local_path="query_results.csv")
            fix_data_types(csv_file, tableau_data_path)
            print("New csv file pulled.")

        print("Process completed successfully.")
    except Exception as e:
        print(f"Error: {e}")
