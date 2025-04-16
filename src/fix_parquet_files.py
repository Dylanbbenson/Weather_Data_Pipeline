import boto3
import re
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import s3fs
import io
import tempfile
import json
import os
from pathlib import Path
import logging

#load credentials and config
credentials_path = Path(__file__).resolve().parent.parent / 'config' / 'credentials.json'
with open(credentials_path) as f:
    credentials = json.load(f)

s3 = boto3.client("s3", region_name="us-east-2")
bucket_name = credentials['bucket']
input_prefix = 'weather_data_partitioned/year=2025/'
output_prefix = 'weather_data_partitioned/year=2025-fixed/'
date_regex = re.compile(r'(\d{4})-(\d{2})-(\d{2})')  # Match YYYY-MM-DD
date_column = 'date'
s3_fs = s3fs.S3FileSystem()

# List all files in the prefix
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=input_prefix)

for page in pages:
    for obj in page.get('Contents', []):
        key = obj['Key']
        if not key.endswith('.parquet'):
            continue  # Skip non-Parquet files

        print(f"Processing file: {key}")

        # Download the Parquet file from S3
        obj_body = s3.get_object(Bucket=bucket_name, Key=key)['Body'].read()
        buf = io.BytesIO(obj_body)

        # Read Parquet file into a PyArrow table
        table = pq.read_table(buf)

        # Convert table to pandas DataFrame
        df = table.to_pandas()

        # Fix the 'date' column to ensure it is a string type
        if 'date' in df.columns:
            df['date'] = df['date'].astype(str)

        # Convert the DataFrame back to a PyArrow Table
        table_fixed = pa.Table.from_pandas(df)

        # Write the fixed table back to a new buffer in Parquet format with SNAPPY compression
        out_buf = io.BytesIO()
        pq.write_table(table_fixed, out_buf, compression='snappy')

        # Construct the output key (new file path in the "2025-fixed" folder)
        output_key = key.replace(input_prefix, output_prefix, 1)

        # Upload the fixed Parquet file to the "2025-fixed" directory in S3
        s3.put_object(Bucket=bucket_name, Key=output_key, Body=out_buf.getvalue())
        print(f"✓ Fixed and uploaded: {output_key}")

print("Processing complete!")