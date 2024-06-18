#!/bin/bash

# Load environment variables from config.env
if [ -f ./config/config.env ]; then
    source ./config/config.env
else
    echo "config file not found!"
    exit 1
fi

# SQL script to create the table
CREATE_DB_SCRIPT="
CREATE DATABASE IF NOT EXISTS ${DB_NAME};
"

# Create database
mysql -h ${DB_HOST} -u ${DB_USER} -p${DB_PASSWORD} -e "${CREATE_DB_SCRIPT}"

# Create table
mysql -h ${DB_HOST} -u ${DB_USER} -p${DB_PASSWORD} ${DB_NAME} < ./src/create_weather_table.sql

echo "Database and table created successfully."
