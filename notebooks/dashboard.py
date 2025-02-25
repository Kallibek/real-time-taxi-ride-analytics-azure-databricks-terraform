# Databricks notebook source
dbutils.widgets.text("gold_ride_summary_path", "/mnt/gold/ride_summary")
dbutils.widgets.text("gold_taxi_payment_summary_path", "/mnt/gold/taxi_payment_summary")



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query for overall ride metrics
# MAGIC SELECT
# MAGIC   hour,
# MAGIC   ride_count,
# MAGIC   total_fare,
# MAGIC   avg_trip_distance
# MAGIC FROM delta.`${gold_ride_summary_path}`
# MAGIC ORDER BY hour DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query for payment type metrics
# MAGIC SELECT
# MAGIC   hour,
# MAGIC   payment_description,
# MAGIC   ride_count,
# MAGIC   total_fare
# MAGIC FROM delta.`${gold_taxi_payment_summary_path}`
# MAGIC ORDER BY hour DESC;
# MAGIC