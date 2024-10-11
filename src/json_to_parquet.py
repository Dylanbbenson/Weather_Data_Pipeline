import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import coalesce, col, lit, explode, expr, size, round
from pyspark.sql.types import StringType, FloatType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# initialize Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_bucket = "s3://bucket-name"  # specify s3 bucket here

# read from source
datasource0 = glueContext.create_dynamic_frame.from_catalog(database="weather", table_name="raw")

# optional code for testing with single json file
"""
single_file_path = f"{s3_bucket}/raw/weather_data_202301.json"

# Reading from the single JSON file using from_options
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [single_file_path]},
    format="json"
)
"""

df = datasource0.toDF()

# flatten observations array
flat_df = df.withColumn("observation", explode(col("observations.observations")))
flat_df = flat_df.withColumn("date", col("observation.date"))

# aggregate averages
fields_to_aggregate_1 = [
    "temp"
    , "rh"
    , "feels_like"
    , "wc"
    , "heat_index"
    , "wdir"
    , "dewPt"
    , "wspd"
    , "uv_index"
]

for field in fields_to_aggregate_1:
    flat_df = flat_df.withColumn(
        field,
        expr(f"""
            IF(size(observation.{field}) > 0, 
            aggregate(observation.{field}, 0, (acc, x) -> acc + x) / size(observation.{field}), 
            NULL)
        """)
    )

flat_df = flat_df.withColumn(
    "gust",
    F.expr("""
        CASE
            WHEN size(filter(observation.gust, x -> x IS NOT NULL)) > 0 
            THEN aggregate(
                filter(observation.gust, x -> x IS NOT NULL), 
                0D, 
                (acc, x) -> acc + x
            ) / size(filter(observation.gust, x -> x IS NOT NULL))
            ELSE NULL
        END
    """)
)

flat_df = flat_df.withColumn(
    "pressure",
    expr("""
        IF(size(observation.pressure) > 0,
        aggregate(
            transform(observation.pressure, x -> CAST(x AS DOUBLE)), 
            CAST(0.0 AS DOUBLE), 
            (acc, x) -> acc + x
        ) / size(observation.pressure),
        NULL)
    """)
)

flat_df = flat_df.withColumn(
    "precip_hrly",
    F.expr("aggregate(transform(observation.precip_hrly, x -> coalesce(x.int, 0)), 0, (acc, x) -> acc + x)")
)

flat_df = flat_df.withColumn(
    "snow_hrly",
    F.expr("aggregate(observation.snow_hrly, 0D, (acc, x) -> acc + x)")
)

flat_df = flat_df.withColumn(
    "vis",
    F.expr("aggregate(transform(observation.vis, x -> coalesce(x.int, 0)), 0, (acc, x) -> acc + x)")
)

fields_to_mode = [
    "wx_phrase"
    , "uv_desc"
    , "wdir_cardinal"
]

for field in fields_to_mode:
    exploded_df = flat_df.select("observation.date",
                                 F.explode(F.col(f"observation.{field}")).alias(f"{field}_exploded"))
    counted_df = exploded_df.groupBy("date", f"{field}_exploded").count()
    window = Window.partitionBy("date").orderBy(F.desc("count"))
    most_common_value_df = counted_df.withColumn("rank", F.row_number().over(window)) \
        .filter(F.col("rank") == 1) \
        .select("date", F.col(f"{field}_exploded").alias(f"{field}_mode"))
    flat_df = flat_df.drop(f"{field}").join(most_common_value_df, "date")

# select necessary fields
flat_df = flat_df.select(
    coalesce(col("observation.key").cast(StringType()), lit(None)).alias("key"),
    col("observation.date").alias("date"),
    # coalesce(col("observation.class").cast(StringType()), lit(None)).alias("class"),
    # coalesce(col("observation.expire_time_gmt").cast(StringType()), lit(None)).alias("expire_time_gmt"),
    # coalesce(col("observation.obs_id"), lit(None)).alias("obs_id"),
    coalesce(col("observation.obs_name").cast(StringType()), lit(None)).alias("obs_name"),
    # coalesce(col("observation.valid_time_gmt").cast(StringType()), lit(None)).alias("valid_time_gmt"),
    # coalesce(col("observation.day_ind").cast(StringType()), lit(None)).alias("day_ind"),
    round(coalesce(col("temp"), lit(None)), 0).cast(IntegerType()).alias("temperature"),
    # coalesce(col("observation.wx_icon").cast(FloatType()), lit(None)).alias("wx_icon"),
    # coalesce(col("observation.icon_extd").cast(StringType()), lit(None)).alias("icon_extd"),
    coalesce(col("wx_phrase_mode").cast(StringType()), lit(None)).alias("forecast_desc"),
    # coalesce(col("observation.pressure_tend").cast(StringType()), lit(None)).alias("pressure_tend"),
    round(coalesce(col("dewPt").cast(FloatType()), lit(0)), 2).alias("dew_point"),
    round(coalesce(col("heat_index").cast(FloatType()), lit(0)), 2).alias("heat_index"),
    round(coalesce(col("rh").cast(FloatType()), lit(0)), 2).alias("relative_humidity"),
    round(coalesce(col("pressure").cast(FloatType()), lit(0)), 2).alias("pressure"),
    # coalesce(col("pressure_desc_mode").cast(StringType()), lit(None)).alias("pressure_desc"),
    coalesce(col("vis").cast(FloatType()), lit(0)).alias("visibility"),
    round(coalesce(col("wc").cast(FloatType()), lit(0)), 2).alias("wind_chill"),
    round(coalesce(col("wdir"), lit(None)), 0).cast(IntegerType()).alias("wind_direction"),
    coalesce(col("wdir_cardinal_mode").cast(StringType()), lit(None)).alias("wind_direction_cardinal"),
    round(coalesce(col("gust").cast(FloatType()), lit(0)), 2).alias("gust"),
    round(coalesce(col("wspd").cast(FloatType()), lit(0)), 2).alias("wind_speed"),
    # coalesce(col("max_temp").cast(FloatType()), lit(None)).alias("max_temp"),
    # coalesce(col("min_temp").cast(FloatType()), lit(None)).alias("min_temp"),
    # coalesce(col("precip_total").cast(FloatType()), lit(None)).alias("precipitation"),
    round(coalesce(col("precip_hrly").cast(FloatType()), lit(0)), 2).alias("total_precipitation"),
    round(coalesce(col("snow_hrly").cast(FloatType()), lit(0)), 2).alias("total_snow"),
    coalesce(col("uv_desc_mode").cast(StringType()), lit(None)).alias("UV"),
    round(coalesce(col("uv_index").cast(FloatType()), lit(0)), 2).alias("UV_index"),
    round(coalesce(col("feels_like"), lit(None)), 0).cast(IntegerType()).alias("feels_like"),
    # coalesce(col("observation.qualifier").cast(StringType()), lit(None)).alias("qualifier"),
    # coalesce(col("observation.qualifier_svrty").cast(StringType()), lit(None)).alias("qualifier_svrty"),
    # coalesce(col("observation.blunt_phrase").cast(StringType()), lit(None)).alias("blunt_phrase"),
    # coalesce(col("observation.terse_phrase").cast(StringType()), lit(None)).alias("terse_phrase"),
    # coalesce(col("observation.clds").cast(FloatType()), lit(None)).alias("clds")
)

# simplify/drop redundant fields
flat_df = flat_df.withColumn("date", F.expr("date[0]"))
flat_df = flat_df.withColumn("obs_name", F.expr("substring(obs_name, 1, 1)"))
flat_df = flat_df.drop("key", "obs_name")

# write to s3
flat_df.write.mode("overwrite").parquet(f"{s3_bucket}/processed/")
job.commit()
