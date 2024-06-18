# Weather_Data_Pipeline

Weather data project demonstrating data engineering concepts such as ETL, storage, logging, task scheduling, and orchestration.

/src:
- retrieve_data.py: python script that pulls data from the OpenWeather api using an api key located in an config.env file (not provided in this project, you'll need to get your own)
- load_data_to_s3.py: python script that loads json data to an AWS s3 bucket.
- load_data_to_db.py: python script that loads data to an internal MySQL database.
- Pipeline_Master.py: python script that uses Airflow to run the previous three scripts as a DAG.
- create_weather_table.sql: sql script to create MySQL table for weather data.
- create_mysql_db.sh: shell script to create mysql database and run create_weather_table.sql script.

/config:
- requirements.txt: all of the pip requirements for this project
- dockerfile: docker file to create the docker image of this project
