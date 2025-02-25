# Databricks notebook source
# Create text widgets with default values
dbutils.widgets.text("silver_fact_taxi_rides_path", "/mnt/silver/fact_taxi_rides/")
dbutils.widgets.text("silver_dim_payment_type_path", "/mnt/silver/dim_payment_type/")
dbutils.widgets.text("silver_dim_rate_codes_path", "/mnt/silver/dim_rate_codes/")
dbutils.widgets.text("silver_dim_taxi_zones_path", "/mnt/silver/dim_zone/")
dbutils.widgets.text("gold_ride_summary_path", "/mnt/gold/ride_summary")
dbutils.widgets.text("gold_taxi_payment_summary_path", "/mnt/gold/taxi_payment_summary")

# Retrieve the widget values when needed in your notebook
silver_fact_taxi_rides_path = dbutils.widgets.get("silver_fact_taxi_rides_path")
silver_dim_payment_type_path = dbutils.widgets.get("silver_dim_payment_type_path")
silver_dim_rate_codes_path = dbutils.widgets.get("silver_dim_rate_codes_path")
silver_dim_taxi_zones_path = dbutils.widgets.get("silver_dim_taxi_zones_path")
gold_ride_summary_path = dbutils.widgets.get("gold_ride_summary_path")
gold_taxi_payment_summary_path = dbutils.widgets.get("gold_taxi_payment_summary_path")

# Now you can use these variables in your code
print("Silver Fact Taxi Rides Path:", silver_fact_taxi_rides_path)
print("Silver Dim Payment Type Path:", silver_dim_payment_type_path)
print("Silver Dim Rate Codes Path:", silver_dim_rate_codes_path)
print("Silver Dim Taxi Zones Path:", silver_dim_taxi_zones_path)
print("Gold Ride Summary Path:", gold_ride_summary_path)
print("Gold Taxi Payment Summary Path:", gold_taxi_payment_summary_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 1: Overall Taxi Ride Summary

# COMMAND ----------

from pyspark.sql.functions import window, count, sum, avg, col, date_format, broadcast, round

# Read the streaming fact table (Delta)
fact_df = spark.readStream.format("delta")\
    .option("startingVersion", "0") \
    .load(silver_fact_taxi_rides_path)
    

# Read the dimension tables (static)
dim_payment = spark.read.format("parquet")\
    .load(silver_dim_payment_type_path)
dim_rate = spark.read.format("parquet")\
    .load(silver_dim_rate_codes_path)
dim_zone = spark.read.format("parquet")\
    .load(silver_dim_taxi_zones_path)

# Enrich the fact table by joining with dimension tables
rides_enriched = fact_df \
    .join(broadcast(dim_payment), fact_df.payment_type == dim_payment.payment_type, "left") \
    .join(broadcast(dim_rate), fact_df.rateCodeID == dim_rate.rate_code, "left") \
    .join(broadcast(dim_zone), fact_df.PULocationID == dim_zone.LocationID, "left")

# Aggregate overall ride metrics per 1-hour window based on dropoff_datetime
ride_summary = rides_enriched \
    .withWatermark("dropoff_datetime", "1 minutes") \
    .groupBy(window(col("dropoff_datetime"), "1 hour")) \
    .agg(
        count("*").alias("ride_count"),
        round(sum("fare_amount"), 2).alias("total_fare"),
        round(avg("trip_distance"), 2).alias("avg_trip_distance")
    ) \
    .select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("hour"),
        "ride_count",
        "total_fare",
        "avg_trip_distance"
    )
# display(ride_summary)
# Write the aggregation out as a Delta table in append mode
ride_summary.writeStream \
    .format("delta") \
    .option("checkpointLocation", f"{gold_ride_summary_path}/checkpoint") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start(gold_ride_summary_path)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2: Taxi Payment Type Summary

# COMMAND ----------

from pyspark.sql.functions import date_format

# Aggregate ride metrics per 1-hour window and by payment type description
payment_summary = rides_enriched \
    .withWatermark("dropoff_datetime", "1 minutes") \
    .groupBy(
        window(col("dropoff_datetime"), "1 hour"),
        dim_payment.description.alias("payment_description")
    ) \
    .agg(
        count("*").alias("ride_count"),
        round(sum("fare_amount"), 2).alias("total_fare")
    ) \
    .select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("hour"),
        "payment_description",
        "ride_count",
        "total_fare"
    )

# Write the payment summary aggregation to a Delta table
payment_summary.writeStream \
    .format("delta") \
    .option("checkpointLocation", f"{gold_taxi_payment_summary_path}/checkpoint") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start(gold_taxi_payment_summary_path)
