import boto3
import time
import json
from pathlib import Path

#load credentials and configs
credentials_path = Path(__file__).resolve().parent.parent / 'config' / 'credentials.json'
with open(credentials_path) as f:
    credentials = json.load(f)

athena = boto3.client("athena", region_name="us-east-2")
s3_client = boto3.client("s3", region_name="us-east-2")
athena_db = credentials["database"]
aws_bucket = credentials['bucket']
s3_output = f"s3://{aws_bucket}/athena-results/"


def get_missing_dates(): #get missing dates from start of year
    query = """
        WITH date_range AS (
            SELECT replace(cast(date_add('day', x, DATE '2025-01-01') as varchar), '-', '') as date
            FROM UNNEST(sequence(0, date_diff('day', DATE '2025-01-01', current_date))) as t(x)
        ),
        existing_dates AS (
                SELECT DISTINCT cast(date as varchar) as date
                FROM weather.processed
        )
        SELECT d.date
        FROM date_range d
        LEFT JOIN existing_dates e ON d.date = e.date
        WHERE e.date IS NULL
        ORDER BY d.date;
    ZZ"""

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': athena_db},
        ResultConfiguration={'OutputLocation': s3_output}
    )

    query_execution_id = response['QueryExecutionId']

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if status != 'SUCCEEDED':
        raise Exception("Athena query failed")

    # Now get the results
    result = athena.get_query_results(QueryExecutionId=query_execution_id)
    missing_dates = [row['Data'][0]['VarCharValue'] for row in result['ResultSet']['Rows'][1:]]
    return missing_dates
