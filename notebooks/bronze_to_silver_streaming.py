# Databricks notebook source
dbutils.widgets.text("bronze_fact_taxi_rides_nrt_path", "/mnt/bronze/fact_taxi_rides_nrt", "Bronze Table Path")
dbutils.widgets.text("silver_fact_taxi_rides_path", "/mnt/silver/fact_taxi_rides", "Silver Table Path")

bronze_fact_taxi_rides_nrt_path = dbutils.widgets.get("bronze_fact_taxi_rides_nrt_path")
silver_fact_taxi_rides_path = dbutils.widgets.get("silver_fact_taxi_rides_path")


# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Bronze table path
bronze_fact_taxi_rides_nrt_path = "/mnt/bronze/fact_taxi_rides_nrt"
# Silver table path
silver_fact_taxi_rides_path = "/mnt/silver/fact_taxi_rides"

# Read the streaming data from the Bronze Delta table
bronze_streaming_df = spark.readStream.format("delta").load(bronze_fact_taxi_rides_nrt_path)
# Optionally dsiplay the records
# display(bronze_streaming_df.limit(50))


# COMMAND ----------

from pyspark.sql.functions import col, to_date, hour

# Drop rows where any of the required columns contain NaN or Null values
cleanedDF = bronze_streaming_df.dropna(
    subset=[
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee"
    ]
)

# Convert data types
transformedDF = (
    cleanedDF
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    .withColumn("dropoff_date", to_date(col("dropoff_datetime")))
    .withColumn("dropoff_hour", hour(col("pickup_datetime")))
)

# Remove rows where store_and_fwd_flag is "NaN" and rateCode gt 6. The max rate code is 6
# TO-DO: refer to silver dim_rate_codes table once silver table is ready
filteredDF = transformedDF.filter(
    (col("store_and_fwd_flag") != "NaN") & (col("rateCodeID") <= 6)
)

# display(filteredDF.limit(50))


# COMMAND ----------

partitionedDF = filteredDF.repartition(1, "dropoff_date", "dropoff_hour")
print(partitionedDF.printSchema())

# Write to Silver Delta table with checkpointing
query = partitionedDF.writeStream \
    .format("delta") \
    .option("checkpointLocation", f"{silver_fact_taxi_rides_path}/checkpoint") \
    .option("failOnDataLoss", "false") \
    .partitionBy("dropoff_date", "dropoff_hour") \
    .option("failOnDataLoss", "false") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start(silver_fact_taxi_rides_path)

query.awaitTermination()