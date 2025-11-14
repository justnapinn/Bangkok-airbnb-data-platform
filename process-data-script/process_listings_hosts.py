import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, regexp_replace, to_date, when
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_SOURCE_LISTINGS', 'S3_TARGET_HOSTS', 'S3_TARGET_LISTINGS'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# EXTRACT (อ่าน listings.csv.gz) 
input_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args['S3_SOURCE_LISTINGS']]},
    format="csv",
    format_options={"withHeader": True, "inferSchema": False} 
).toDF()

print("--- START DEBUGGING ---")
total_rows = input_df.count()
print(f"Total rows read from CSV: {total_rows}")

null_host_ids = input_df.filter(col("host_id").isNull()).count()
print(f"Rows with NULL host_id: {null_host_ids}")

non_null_host_ids = input_df.filter(col("host_id").isNotNull()).count()
print(f"Rows with NON-NULL host_id: {non_null_host_ids}")
print("--- END DEBUGGING ---")

# TRANSFORM 

# Transform dim_hosts table 
df_hosts = input_df.select(
    col("host_id").cast("long"),
    col("host_name"),
    to_date(col("host_since"), "yyyy-MM-dd").alias("host_since_date"),
    col("host_location"),
    col("host_response_time"),
    ( (regexp_replace(col("host_response_rate"), "%", "").cast("float") / 100) ).cast("decimal(5,2)").alias("host_response_rate"),
    ( (regexp_replace(col("host_acceptance_rate"), "%", "").cast("float") / 100) ).cast("decimal(5,2)").alias("host_acceptance_rate"),
    (when(col("host_is_superhost") == 't', True).otherwise(False)).alias("is_superhost"),
    col("host_listings_count").cast("int"),
    (when(col("host_identity_verified") == 't', True).otherwise(False)).alias("is_identity_verified")
).dropDuplicates(['host_id']).na.drop(subset=["host_id"]) # เอา Host ที่ซ้ำ/ว่าง ออก

# Transform dim_listings table 
df_listings = input_df.select(
    col("id").cast("long").alias("listing_id"),
    col("name").alias("listing_name"),
    col("host_id").cast("long"),
    to_date(col("last_scraped"), "yyyy-MM-dd").alias("last_scraped_date"),
    col("neighbourhood_cleansed").alias("neighbourhood"),
    col("latitude").cast("decimal(10,7)"),
    col("longitude").cast("decimal(10,7)"),
    col("property_type"),
    col("room_type"),
    col("accommodates").cast("int"),
    col("bathrooms_text"),
    col("bedrooms").cast("int"),
    col("beds").cast("int"),
    (regexp_replace(col("price"), "[$,]", "")).cast("decimal(10, 2)").alias("default_price"),
    col("minimum_nights").cast("int").alias("default_min_nights"),
    col("maximum_nights").cast("int").alias("default_max_nights"),
    (when(col("has_availability") == 't', True).otherwise(False)).alias("has_availability"),
    (when(col("instant_bookable") == 't', True).otherwise(False)).alias("instant_bookable"),
    col("number_of_reviews").cast("int"),
    col("number_of_reviews_ltm").cast("int"),
    to_date(col("first_review"), "yyyy-MM-dd").alias("first_review_date"),
    to_date(col("last_review"), "yyyy-MM-dd").alias("last_review_date"),
    col("review_scores_rating").cast("decimal(3,2)"),
    col("review_scores_cleanliness").cast("decimal(3,2)"),
    col("review_scores_location").cast("decimal(3,2)"),
    col("reviews_per_month").cast("decimal(5,2)")
).na.drop(subset=["listing_id"]) # delete row null ID 

# 3. LOAD (save as Parquet) 

# save dim_hosts 
glueContext.write_dynamic_frame.from_options(
    frame = DynamicFrame.fromDF(df_hosts, glueContext, "df_hosts"),
    connection_type = "s3",
    connection_options = {"path": args['S3_TARGET_HOSTS']},
    format = "parquet",
    format_options={"compression": "snappy"}
)

# save dim_listings 
glueContext.write_dynamic_frame.from_options(
    frame = DynamicFrame.fromDF(df_listings, glueContext, "df_listings"),
    connection_type = "s3",
    connection_options = {"path": args['S3_TARGET_LISTINGS']},
    format = "parquet",
    format_options={"compression": "snappy"}
)

job.commit()