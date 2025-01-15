# Weather_Data_Pipeline

Weather data project demonstrating data engineering concepts such as ETL, storage, logging, task scheduling, orchestration, and visualization. This project is split into three parts:
- ingestion and transformation of historical weather data to create a complete timeline of metrics in my area
- pipeline to pull daily weather data from the OpenWeather API, perform transformations, and add to the existing dataset in S3
- visualization of the data using Metabase

I have this project running on an AWS ec2 instance and pulls data from the OpenWeather api for the Chicago area, performs some data transformation, and loads it to both a local MySQL db and an S3 bucket with the rest of the historical data. This project will also create a MySQL db if not created already, and save 2 backup files in the form of json and csv to a local directory. A visual graph of this process is located below.

This data can be viewed as a sharable metabase dashboard at this link (you will need to login to Metabase to use it): https://dashboard.dylsdomain.com

The historical dataset was accumulated by scraping data from Wunderground.com using Python, storing in s3 as json objects, aggregating and cleaning them into parquet files in Glue, then loading and querying in Athena.

**Project Structure**

/dags:
- Pipeline_Master.py: python script that specifies an Airflow DAG and workflow to run the python scripts retrieve_data, load_data_to_s3, and load_data_to_db in order. This script is Cron programmed to run once at the top of every hour.

/src:
- retrieve_data.py: python script that pulls today's weather data from wunderground api.
- load_data_to_s3.py: python script that loads json data to an AWS s3 bucket.
- load_data_to_db.py: python script that loads data to an internal MySQL database located on the ec2 instance.
- create_mysql_db.sh: shell script to create mysql database, create user with privileges, and run create_weather_table.sql script.
- create_weather_table.sql: sql script to create MySQL table for weather data.
  json_to_parquet.py - PySpark script to aggregate and transform historical json data into parquet files in S3
- json_to_parquet.json - The actual Glue job that executes the above python script as a workflow
- json_to_parquet_daily.py: Pyspark script to aggregate and transform today's weather data into parquet in s3
- json_to_parquet_daily.json: The actual Glue job that executes the above python script as a workflow
- lambda_function.py - (optional) lambda function I wrote to convert the raw json to a csv format instead. I wrote this before deciding to convert to parquet instead, but still kept it
- create_athena_table.sql - sql to create a table for the data in Athena using the Apache Iceberg table format
- Athena_to_csv.py - converts Athena data to a local csv file
- Athena_to_hyper.py - (optional) converts Athena data to a hyper file that can be used with Tableau. I developed this when I planned on using Tableau for visualization, but decided on another dashboard program instead

/config:
- config.env: template config file for specifying environment variables for this project.

/data:
- example_data.snappy.parquet: example parquet file created from the Glue job converting json to parquet

/metabase:
- questions-38-56.json: metabase questions exported as json files that can be imported to a metabase dashboard

/:
- requirements.txt: all of the pip requirements for this project
- dockerfile: docker file to create the docker image of this project
- docker-compose.yml: for setting up Airflow service

![Weather Data Pipeline](https://github.com/Dylanbbenson/Weather_Data_Pipeline/assets/70871558/8000fafe-895c-4910-98eb-811692b8cc9d)

**Airflow DAG chart:**

![image](https://github.com/Dylanbbenson/Weather_Data_Pipeline/assets/70871558/104081af-ae79-436d-b191-69b3ec62a8a6)

**Historical Dataset Aggregation Pipeline**

![image](https://github.com/user-attachments/assets/19c0e92a-dd88-4a0f-b5bd-efe151391cd1)
