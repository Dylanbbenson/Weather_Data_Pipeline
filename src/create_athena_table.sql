CREATE TABLE IF NOT EXISTS `weather`.`processed` (
  `key` string,
  `date` string,
  `obs_name` string,
  `temperature` int,
  `forecast_desc` string,
  `dew_point` float,
  `heat_index` float,
  `relative_humidity` float,
  `pressure` float,
  `visibility` float,
  `wind_chill` float,
  `wind_direction` float,
  `wind_direction_cardinal` string,
  `gust` float,
  `wind_speed` float,
  `total_precipitation` float,
  `total_snow` float,
  `uv` string,
  `uv_index` float,
  `feels_like` float
)
LOCATION '{s3_bucket}/processed/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'write_compression' = 'SNAPPY'
);