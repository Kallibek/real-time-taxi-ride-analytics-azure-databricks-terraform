# Databricks notebook source
# Create notebook widgets with default values
dbutils.widgets.text("bronze_fact_taxi_rides_path", "mnt/bronze/fact_taxi_rides_nrt", "Base path for bronze Delta table")
dbutils.widgets.text("EH_NAMESPACE", "taxi-data-analytics-ehns", "Event Hubs namespace")
dbutils.widgets.text("EH_NAME", "taxi-data-events", "Event Hubs name")
dbutils.widgets.text("EH_CONN_SHARED_ACCESS_KEY_NAME", "send-policy", "Event Hubs shared access key name")

# Retrieve the widget values into variables
bronze_fact_taxi_rides_path = dbutils.widgets.get("bronze_fact_taxi_rides_path")
EH_NAMESPACE = dbutils.widgets.get("EH_NAMESPACE")
EH_NAME = dbutils.widgets.get("EH_NAME")
EH_CONN_SHARED_ACCESS_KEY_NAME = dbutils.widgets.get("EH_CONN_SHARED_ACCESS_KEY_NAME")

EH_CONN_STR = dbutils.secrets.get(scope="application", key="eventhub_conn_str")


# COMMAND ----------


# Kafka Consumer configuration for reading from Event Hubs via Kafka protocol
KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
         f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
         f"username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";"
    ),
    # Use startingOffsets to control where the stream starts (e.g., "earliest")
    "startingOffsets": "earliest" # other option: "latest"
}

# Create a streaming DataFrame from Kafka (which connects to Event Hubs)
streamingDF = (
    spark.readStream
         .format("kafka")
         .options(**KAFKA_OPTIONS)
         .load()
)


# COMMAND ----------

from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# Define the hardcoded schema
hardcoded_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),  # You can also use TimestampType if you prefer
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])

def parse_taxi(df):
    return (
        df.selectExpr("CAST(value AS STRING) as json_str")
          .withColumn("parsed", from_json(col("json_str"), hardcoded_schema))
          .select("parsed.*")
          .withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("passenger_count", col("passenger_count").cast("integer"))
          .withColumn("rateCodeID", col("RatecodeID").cast("integer"))
    )

# Parse the streaming data
parsedDF = parse_taxi(streamingDF)

# Print the schema to verify
# parsedDF.printSchema()

# Optionally display the parsed records
# display(parsedDF.limit(50))

# COMMAND ----------

from pyspark.sql.functions import to_date, hour, col

# Create additional columns from ingestion time for partitioning.
# Here we extract the date and hour
transformedDF = parsedDF \
    .withColumn("ingestion_date", to_date(col("ingestion_timestamp"))) \
    .withColumn("ingestion_hour", hour(col("ingestion_timestamp")))

# Repartition so that for each unique (ingestion_date, ingestion_hour) we have one partition.
# This helps ensure that one file is written per minute in each micro-batch.
# Note: repartitioning can affect performance if data volumes are high.
partitionedDF = transformedDF.repartition(1, "ingestion_date", "ingestion_hour")

partitionedDF.printSchema()

# Write the streaming data to Delta partitioned by dropoff date, hour, and minute.
query = partitionedDF.writeStream \
    .format("delta") \
    .option("checkpointLocation", f"{bronze_fact_taxi_rides_path}/checkpoint") \
    .option("failOnDataLoss", "false") \
    .partitionBy("ingestion_date", "ingestion_hour") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start(bronze_fact_taxi_rides_path)

query.awaitTermination()