# Databricks notebook source
# Create text widgets with default values
dbutils.widgets.text("bronze_dim_payment_type_path", "/mnt/bronze/dim_payment_type/")
dbutils.widgets.text("bronze_dim_rate_codes_path", "/mnt/bronze/dim_rate_codes/")
dbutils.widgets.text("bronze_dim_taxi_zones_path", "/mnt/bronze/dim_zone/")

dbutils.widgets.text("silver_dim_payment_type_path", "/mnt/silver/dim_payment_type/")
dbutils.widgets.text("silver_dim_rate_codes_path", "/mnt/silver/dim_rate_codes/")
dbutils.widgets.text("silver_dim_taxi_zones_path", "/mnt/silver/dim_zone/")

# Retrieve the parameter values
bronze_payment_type_path = dbutils.widgets.get("bronze_dim_payment_type_path")
bronze_rate_codes_path = dbutils.widgets.get("bronze_dim_rate_codes_path")
bronze_taxi_zones_path = dbutils.widgets.get("bronze_dim_taxi_zones_path")

silver_payment_type_path = dbutils.widgets.get("silver_dim_payment_type_path")
silver_rate_codes_path = dbutils.widgets.get("silver_dim_rate_codes_path")
silver_taxi_zones_path = dbutils.widgets.get("silver_dim_taxi_zones_path")


# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_payment_type

# COMMAND ----------

from pyspark.sql.functions import col, trim, regexp_replace

bronze_dim_payment_type_df = spark.read.parquet(bronze_payment_type_path)
bronze_dim_payment_type_df.printSchema()
bronze_dim_payment_type_df = (bronze_dim_payment_type_df
                                .withColumnRenamed("description\r", "description")
                                .withColumn("description", regexp_replace(col("description"), "\r", ""))  # Remove "\r"
                                .withColumn("payment_type", col("payment_type").cast("integer"))
                                .filter(
                                    col("payment_type").isNotNull() &
                                    col("description").isNotNull() &
                                    (trim(col("description")) != "?") &
                                    (trim(col("description")) != "") &
                                    (trim(col("description")) != "\r")
                                ) 
                              )
bronze_dim_payment_type_df.show(10, truncate=False)
bronze_dim_payment_type_df.write.mode("overwrite").parquet(silver_payment_type_path)         

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_rate_codes

# COMMAND ----------


bronze_dim_rate_codes_df = spark.read.parquet(bronze_rate_codes_path)
bronze_dim_rate_codes_df = (bronze_dim_rate_codes_df
                                .withColumnRenamed("description\r", "description")
                                .withColumn("description", regexp_replace(col("description"), "\r", ""))  # Remove "\r"
                                .withColumn("rate_code", col("rate_code").cast("integer"))
                                .filter(
                                    col("rate_code").isNotNull() &
                                    col("description").isNotNull() &
                                    (trim(col("description")) != "?") &
                                    (trim(col("description")) != "") &
                                    (trim(col("description")) != "\r")
                                ) 
                              )
bronze_dim_rate_codes_df.printSchema()
bronze_dim_rate_codes_df.show(10, truncate=False)
bronze_dim_rate_codes_df.write.mode("overwrite").parquet(silver_rate_codes_path)    

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_taxi_zones

# COMMAND ----------


from pyspark.sql.functions import col, trim
from functools import reduce

bronze_dim_taxi_zones_df = spark.read.parquet(bronze_taxi_zones_path)

for c in bronze_dim_taxi_zones_df.columns:
    bronze_dim_taxi_zones_df = bronze_dim_taxi_zones_df.withColumn(c, trim(col(c)))

# Define list of invalid string values
invalid_values = ["null", "error", "?", "\r", "N/A"]

# Build a filter condition for all columns:
# Each column must be not null and must not have any of the invalid values.
conditions = [ (col(c).isNotNull()) & (~col(c).isin(invalid_values)) for c in bronze_dim_taxi_zones_df.columns ]
final_condition = reduce(lambda a, b: a & b, conditions)

# Filter out invalid rows
bronze_dim_taxi_zones_df = (bronze_dim_taxi_zones_df
                            .filter(final_condition)
                            .withColumnRenamed("service_zone\r", "service_zone")
                            .withColumn("LocationID", col("LocationID").cast("integer"))
                            .withColumn("service_zone", regexp_replace(col("service_zone"), "\r", ""))  # Remove "\r"
                            )
 
bronze_dim_taxi_zones_df.printSchema()
bronze_dim_taxi_zones_df.show(10, truncate=False)
bronze_dim_taxi_zones_df.write.mode("overwrite").parquet(silver_taxi_zones_path)   