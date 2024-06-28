# Weather_Data_Pipeline

Weather data project demonstrating data engineering concepts such as ETL, storage, logging, task scheduling, and orchestration. I have this project running on an AWS ec2 instance and pulls data from the OpenWeather api for the Chicago area, performs some data transformation, and loads it to both a local MySQL db and a S3 bucket. This project will also create a MySQL db if not created already, and save 2 backup files in the form of json and csv to a local directory. A visual graph of this process is located below.

**Project Structure**
/dags:
- Pipeline_Master.py: python script that specifies an Airflow DAG and workflow for the three python scripts to run in order. This script is Cron programmed to run once at the top of every hour.

/src:
- retrieve_data.py: python script that pulls data from the OpenWeather api using an api key located in an config.env file (not provided in this project, you'll need to get your own). Saves data to local directory in the format of "weather_data_{current_date}_{current_hour}" as both json and csv. 
- load_data_to_s3.py: python script that loads json data to an AWS s3 bucket.
- load_data_to_db.py: python script that loads data to an internal MySQL database located on the ec2 instance.
- create_mysql_db.sh: shell script to create mysql database, create user with privileges, and run create_weather_table.sql script.
- create_weather_table.sql: sql script to create MySQL table for weather data.

/config:
- requirements.txt: all of the pip requirements for this project
- dockerfile: docker file to create the docker image of this project
- config.env: example config file for specifying environment variables for this project.

![Weather Data Pipeline](https://github.com/Dylanbbenson/Weather_Data_Pipeline/assets/70871558/8000fafe-895c-4910-98eb-811692b8cc9d)

**Airflow DAG chart:**

![image](https://github.com/Dylanbbenson/Weather_Data_Pipeline/assets/70871558/104081af-ae79-436d-b191-69b3ec62a8a6)
