import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, regexp_replace, to_date, when
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_SOURCE_CALENDAR', 'S3_TARGET_CALENDAR'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- 1. EXTRACT ---
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args['S3_SOURCE_CALENDAR']]},
    format="csv",
    format_options={"withHeader": True}
).toDF()

# --- 2. TRANSFORM ---
df_cleaned = df.select(
    col("listing_id").cast("long"),
    to_date(col("date"), "yyyy-MM-dd").alias("calendar_date"), 
    (when(col("available") == 't', True).otherwise(False)).alias("is_available"),
    (regexp_replace(col("price"), "[$,]", "")).cast("decimal(10, 2)").alias("price"),
    (regexp_replace(col("adjusted_price"), "[$,]", "")).cast("decimal(10, 2)").alias("adjusted_price"),
    col("minimum_nights").cast("int").alias("minimum_nights_on_date"),
    col("maximum_nights").cast("int").alias("maximum_nights_on_date")
).na.drop(subset=["listing_id", "calendar_date"])

# --- 3. LOAD ---
glueContext.write_dynamic_frame.from_options(
    frame = DynamicFrame.fromDF(df_cleaned, glueContext, "df_cleaned"),
    connection_type = "s3",
    connection_options = {"path": args['S3_TARGET_CALENDAR']},
    format = "parquet",
    format_options={"compression": "snappy"}
)

job.commit()