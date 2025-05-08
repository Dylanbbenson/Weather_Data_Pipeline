CREATE EXTERNAL TABLE IF NOT EXISTS weather.forecast (
    `ds` date,
    `yhat` float,
    `yhat_lower` float,
    `yhat_upper` float,
    `run_date` date
)
row format delimited
fields terminated by ','
stored as textfile
LOCATION 's3://dyls-weather-data/forecast/'
TBLPROPERTIES (
  'skip.header.line.count'='1'
);
