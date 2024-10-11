import json
import boto3
import csv
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        #get bucket name and object key
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        #read json and flatten observations array
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
        observations = json_data.get("observations", [])
        if not observations or not isinstance(observations, list) or not all(isinstance(i, dict) for i in observations):
            raise ValueError("Invalid JSON data structure: Expected a list of dictionaries under 'observations'.")

        #convert to csv
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerow(observations[0].keys())

        for item in observations:
            csv_writer.writerow(item.values())

        #output to processed directory and save to s3
        output_key = object_key.replace('raw/', 'processed/').replace('.json', '.csv')

        s3.put_object(Bucket=bucket_name, Key=output_key, Body=csv_buffer.getvalue())
        print(f"Successfully processed and saved to {output_key}")

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        raise e
