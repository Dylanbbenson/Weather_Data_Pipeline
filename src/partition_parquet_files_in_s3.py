import boto3
import re
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import s3fs
import tempfile
import json
import os
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)

#load credentials and config
credentials_path = Path(__file__).resolve().parent.parent / 'config' / 'credentials.json'
with open(credentials_path) as f:
    credentials = json.load(f)

s3 = boto3.client("s3")
bucket_name = credentials['bucket']
source_prefix = 'processed/'
target_prefix = 'weather_data_partitioned/'
s3_fs = s3fs.S3FileSystem()

#list all parquet files
result = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)
parquet_files = [obj for obj in result.get('Contents', []) if obj['Key'].endswith('.parquet')]

for obj in parquet_files:
    key = obj['Key']
    file_size = obj['Size'] / 1024
    logging.info(f"Processing: {key} ({file_size:.1f} KB)")

    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        s3.download_file(bucket_name, key, tmp_file.name)
        local_file_path = tmp_file.name

    try:
        table = pq.read_table(local_file_path)
        df = table.to_pandas()

        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        unique_dates = df['date'].dropna().dt.date.unique()

        if len(unique_dates) == 1:   #put file in own partitioned directory
            date = unique_dates[0]
            year, month, day = date.year, f"{date.month:02d}", f"{date.day:02d}"
            dest_key = f"{target_prefix}year={year}/month={month}/day={day}/{os.path.basename(key)}"
            s3.upload_file(local_file_path, bucket_name, dest_key)
            logging.info(f"Moved to partition: {dest_key}")

        elif file_size < 10:
            logging.info(f"Small multi-row file found. Splitting up by date...")

            for date_val, group_df in df.groupby(df['date'].dt.date):
                year, month, day = date_val.year, f"{date_val.month:02d}", f"{date_val.day:02d}"
                partition_key = (
                    f"{target_prefix}year={year}/month={month}/day={day}/"
                    f"{os.path.basename(key).replace('.parquet', f'_{date_val}.parquet')}"
                )

                with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as part_file:
                    table_part = pa.Table.from_pandas(group_df)
                    pq.write_table(table_part, part_file.name)
                    s3.upload_file(part_file.name, bucket_name, partition_key)
                    os.remove(part_file.name)
                    logging.info(f"Wrote partition: {partition_key}")

        else: #put file in historical directory
            dest_key = f"{target_prefix}historical/{os.path.basename(key)}"
            s3.upload_file(local_file_path, bucket_name, dest_key)
            logging.info(f"Larger multi-date file. Moved to: {dest_key}")

    except Exception as e:
        logging.error(f"Error processing {key}: {e}")

    finally:
        os.remove(local_file_path)

logging.info("\nFinished.")