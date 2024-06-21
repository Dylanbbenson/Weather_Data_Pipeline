#!/bin/bash

# Check if log directory exists, if not, create it
if [ ! -d "./log" ]; then
    mkdir -p "log"
fi

LOG="./log/create_mysql_db.log"

# Load environment variables from config.env
if [ -f ./config/config.env ]; then
    source ./config/config.env
else
    echo "[$(date)] Config file not found!" | tee -a ${LOG}
    exit 1
fi

# Ensure all required environment variables are set
if [ -z "${DB_HOST}" ] || [ -z "${DB_USER}" ] || [ -z "${DB_PASSWORD}" ] || [ -z "${DB_NAME}" ]; then
    echo "[$(date)] Missing required environment variables!" | tee -a ${LOG}
    exit 1
fi

# SQL script to create the db
CREATE_DB_SCRIPT="
CREATE DATABASE IF NOT EXISTS ${DB_NAME};
"

GRANT_PRIVILEGES_SCRIPT="
GRANT ALL PRIVILEGES ON *.* TO ${DB_USER}@${DB_HOST} WITH GRANT OPTION;
FLUSH PRIVILEGES;
"

{
  # Test database connection
  mysql -h ${DB_HOST} -u ${DB_USER} -p${DB_PASSWORD} -e "SELECT 1;" 2>&1
  if [ $? -ne 0 ]; then
      echo "[$(date)] Failed to connect to MySQL with provided credentials." | tee -a ${LOG}
      exit 1
  fi

  # Grant Privileges
  sudo mysql -u root -e "${GRANT_PRIVILEGES_SCRIPT}" 2>&1
  if [ $? -ne 0 ]; then
      echo "[$(date)] Failed to grant privileges to user ${DB_USER}." | tee -a ${LOG}
      exit 1
  fi


  # Create database
  mysql -h ${DB_HOST} -u ${DB_USER} -p${DB_PASSWORD} -e "${CREATE_DB_SCRIPT}" 2>&1
  if [ $? -ne 0 ]; then
      echo "[$(date)] Failed to create database ${DB_NAME}." | tee -a ${LOG}
      exit 1
  fi

  # Create table
  mysql -h ${DB_HOST} -u ${DB_USER} -p${DB_PASSWORD} ${DB_NAME} < ./src/create_weather_table.sql 2>&1
  if [ $? -ne 0 ]; then
      echo "[$(date)] Failed to create table in database ${DB_NAME}." | tee -a ${LOG}
      exit 1
  fi

  echo "[$(date)] Database and table created successfully." | tee -a ${LOG}

} >> ${LOG} 2>&1
