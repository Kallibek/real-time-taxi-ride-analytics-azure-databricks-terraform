# Databricks notebook source
# Create text widgets with default values
dbutils.widgets.text("bronze_fact_taxi_rides_hist_path", "/mnt/bronze/fact_taxi_rides_hist", "Bronze Path")
dbutils.widgets.text("silver_fact_taxi_rides_path", "/mnt/silver/fact_taxi_rides", "Silver Path")

# Retrieve widget values into variables
bronze_fact_taxi_rides_hist_path = dbutils.widgets.get("bronze_fact_taxi_rides_hist_path")
silver_fact_taxi_rides_path = dbutils.widgets.get("silver_fact_taxi_rides_path")

# Display the parameter values
print("Bronze Path:", bronze_fact_taxi_rides_hist_path)
print("Silver Path:", silver_fact_taxi_rides_path)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, hour, to_timestamp, lit
from pyspark.sql.types import TimestampType, DateType, IntegerType

# Read the batch (historical) data from the Bronze Delta table.
# All fields are originally read as strings.
bronze_df = spark.read.format("parquet").load(bronze_fact_taxi_rides_hist_path)

# display(bronze_df.limit(20))
print(bronze_df.printSchema())

# COMMAND ----------

# Optionally, preview the data
# display(bronze_df.limit(50))
# Define the required columns for cleaning.
# (Columns that exist in bronze_df and are needed for transformation)
required_columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "store_and_fwd_flag",
    "Airport_fee\r",  # Removed trailing carriage return character
    "DOLocationID",
    "PULocationID",
    "RatecodeID"
]

# Drop rows that have null/NaN in any of the required columns
cleanedDF = bronze_df.dropna(subset=required_columns)

# Convert data types and rename columns to match the expected silver schema.
# Here we cast each column from its string representation to the correct type.
transformedDF = (
    cleanedDF
    .select(
        col("VendorID").cast("integer").alias("VendorID"),
        to_timestamp("tpep_pickup_datetime").alias("pickup_datetime"),
        to_timestamp("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("passenger_count").cast("integer").alias("passenger_count"),
        col("trip_distance").cast("double").alias("trip_distance"),
        col("RatecodeID").cast("integer").alias("rateCodeID"),
        col("store_and_fwd_flag"),
        col("PULocationID").cast("integer").alias("PULocationID"),
        col("DOLocationID").cast("integer").alias("DOLocationID"),
        col("payment_type").cast("integer").alias("payment_type"),
        col("fare_amount").cast("double").alias("fare_amount"),
        col("extra").cast("double").alias("extra"),
        col("mta_tax").cast("double").alias("mta_tax"),
        col("tip_amount").cast("double").alias("tip_amount"),
        col("tolls_amount").cast("double").alias("tolls_amount"),
        col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        col("total_amount").cast("double").alias("total_amount"),
        col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        col("Airport_fee\r").cast("double").alias("Airport_fee"),
    )
    .withColumn("ingestion_timestamp", lit(None).cast(TimestampType()))
    .withColumn("ingestion_date", lit(None).cast(DateType()))
    .withColumn("ingestion_hour", lit(None).cast(IntegerType()))
    .withColumn("dropoff_date", to_date(col("dropoff_datetime")))
    .withColumn("dropoff_hour", hour(col("dropoff_datetime")))
)

# Remove rows where store_and_fwd_flag is "NaN" and rateCode gt 6. The max rate code is 6
# TO-DO: refer to silver dim_rate_codes table once silver table is ready
filteredDF = transformedDF.filter((col("store_and_fwd_flag") != "NaN") & (col("rateCodeID") <= 6))



# Repartition the DataFrame by dropoff_date and dropoff_hour for optimal file organization
partitionedDF = filteredDF.repartition(1, "dropoff_date", "dropoff_hour")
partitionedDF.printSchema()
display(partitionedDF.limit(10))



# COMMAND ----------

# Write the transformed data to the Silver Delta table in batch mode.
# We use "append" mode so that this historical load adds to any existing data.
partitionedDF.write.format("delta") \
    .mode("append") \
    .partitionBy("dropoff_date", "dropoff_hour") \
    .save(silver_fact_taxi_rides_path)

print("Historical batch data load complete.")