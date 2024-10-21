import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.sql import functions as F
from pyspark.sql.functions import coalesce, col, lit, explode, expr, size, round
from pyspark.sql.types import StringType, FloatType, IntegerType

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Reading from the source table
datasource0 = glueContext.create_dynamic_frame.from_catalog(database="weather", table_name="raw")

# Convert to DataFrame for processing
df = datasource0.toDF()

# Explode the observations array
flat_df = df.withColumn("observation", F.explode(F.col("observations")))

# Select relevant fields and rename for consistency
flat_df = flat_df.select(
    F.col("observation.date").alias("date"),
    F.col("observation.temp").alias("temperature"),
    F.col("observation.wx_phrase").alias("forecast_desc"),
    F.col("observation.dewPt").cast("float").alias("dew_point"),
    F.col("observation.heat_index").cast("float").alias("heat_index"),
    F.col("observation.rh").cast("float").alias("relative_humidity"),
    F.col("observation.pressure").cast("float").alias("pressure"),
    F.col("observation.vis").cast("float").alias("visibility"),
    F.col("observation.wc").cast("float").alias("wind_chill"),
    F.col("observation.wdir").alias("wind_direction"),
    F.col("observation.wdir_cardinal").alias("wind_direction_cardinal"),
    F.col("observation.gust").cast("float").alias("gust"),
    F.col("observation.wspd").cast("float").alias("wind_speed"),
    F.col("observation.precip_total").cast("float").alias("total_precipitation"),
    F.col("observation.snow_hrly").cast("float").alias("total_snow"),
    F.col("observation.uv_desc").alias("UV"),
    F.col("observation.uv_index").cast("float").alias("UV_index"),
    F.col("observation.feels_like").cast("float").alias("feels_like")
)

# Fill missing values with defaults
flat_df = flat_df.fillna({
    "gust": 0.0,
    "wind_speed": 0.0,
    "total_precipitation": 0.0,
    "total_snow": 0.0,
    "dew_point": 0.0,
    "heat_index": 0.0,
    "relative_humidity": 0.0,
    "pressure": 0.0,
    "visibility": 0.0,
    "wind_chill": 0.0,
    "UV_index": 0.0,
    "feels_like": 0.0
})

flat_df = flat_df.filter(F.col("date").isNotNull())

# Group by 'date' and aggregate all fields to create a single row per day
aggregated_df = flat_df.groupBy("date").agg(
    F.round(F.avg("temperature"), 0).cast("integer").alias("temperature"),
    F.round(F.avg("dew_point"), 2).alias("dew_point"),
    F.round(F.avg("heat_index"), 2).alias("heat_index"),
    F.round(F.avg("relative_humidity"), 2).alias("relative_humidity"),
    F.round(F.avg("pressure"), 2).alias("pressure"),
    F.round(F.avg("visibility"), 2).alias("visibility"),
    F.round(F.avg("wind_chill"), 2).alias("wind_chill"),
    F.round(F.avg("wind_speed"), 2).alias("wind_speed"),
    F.round(F.sum("total_precipitation"), 2).alias("total_precipitation"),
    F.round(F.sum("total_snow"), 2).alias("total_snow"),
    F.max("UV").alias("UV"),
    F.round(F.max("UV_index"), 2).alias("UV_index"),
    F.round(F.avg("feels_like"), 0).cast("integer").alias("feels_like"),
    F.first("wind_direction_cardinal").alias("wind_direction_cardinal"),
    F.first("forecast_desc").alias("forecast_desc"),
    F.max("gust").alias("gust"),
    F.max("wind_direction").alias("wind_direction")
)

final_df = aggregated_df.select(
    col("date").cast(StringType()),
    col("temperature").cast(IntegerType()),
    col("forecast_desc").cast(StringType()),
    col("dew_point").cast(FloatType()),
    col("heat_index").cast(FloatType()),
    col("relative_humidity").cast(FloatType()),
    col("pressure").cast(FloatType()),
    col("visibility").cast(FloatType()),
    col("wind_chill").cast(FloatType()),
    col("wind_direction").cast(IntegerType()),
    col("wind_direction_cardinal").cast(StringType()),
    col("gust").cast(FloatType()),
    col("wind_speed").cast(FloatType()),
    col("total_precipitation").cast(FloatType()),
    col("total_snow").cast(FloatType()),
    col("UV").cast(StringType()),
    col("UV_index").cast(FloatType()),
    col("feels_like").cast(IntegerType())
)

# Write the aggregated data to Parquet in overwrite mode
output_path = "s3://dyls-weather-data/processed/"
final_df.write.mode("append").parquet(output_path)

print("Job completed successfully!")
