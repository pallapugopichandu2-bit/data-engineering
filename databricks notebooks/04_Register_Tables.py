# Databricks notebook source
# MAGIC %md
# MAGIC ###Configure ADLS

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.azureadls45.dfs.core.windows.net",
    dbutils.secrets.get("azure", "storage-account-key")
)
print("ADLS configured!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create database

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS nyc_trip_analytics")
spark.sql("USE nyc_trip_analytics")
print("Database created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Register all 4 Gold tables

# COMMAND ----------

gold_base = "abfss://gold@azureadls45.dfs.core.windows.net/"

# Register using spark.read + createOrReplaceTempView instead
df_borough = spark.read.format("delta").load(gold_base + "borough_revenue/")
df_borough.createOrReplaceTempView("borough_revenue")
print("borough_revenue registered!")

df_peak = spark.read.format("delta").load(gold_base + "peak_hours/")
df_peak.createOrReplaceTempView("peak_hours")
print("peak_hours registered!")

df_zones_gold = spark.read.format("delta").load(gold_base + "top_zones/")
df_zones_gold.createOrReplaceTempView("top_zones")
print("top_zones registered!")

df_payment = spark.read.format("delta").load(gold_base + "payment_behaviour/")
df_payment.createOrReplaceTempView("payment_behaviour")
print("payment_behaviour registered!")

print("\nAll 4 tables registered as temp views!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Verify tables are queryable via SQl

# COMMAND ----------

print("=== BOROUGH REVENUE ===")
spark.sql("""
    SELECT pickup_borough, taxi_type, total_trips, total_revenue
    FROM borough_revenue
    ORDER BY total_revenue DESC
""").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Query peak hours

# COMMAND ----------

print("=== PEAK HOURS — Top 10 busiest ===")
spark.sql("""
    SELECT pickup_hour, pickup_day_of_week, taxi_type, total_trips
    FROM peak_hours
    ORDER BY total_trips DESC
    LIMIT 10
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###query top zones

# COMMAND ----------

print("=== TOP 10 PICKUP ZONES ===")
spark.sql("""
    SELECT pickup_zone, pickup_borough, total_trips, total_revenue
    FROM top_zones
    ORDER BY total_trips DESC
    LIMIT 10
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###show all registered tables

# COMMAND ----------

print("=== PAYMENT BEHAVIOUR ===")
spark.sql("""
    SELECT payment_type, pickup_borough, taxi_type, total_trips, avg_tip
    FROM payment_behaviour
    ORDER BY total_trips DESC
    LIMIT 10
""").show()

print("\nAll 4 Gold tables successfully queryable via SQL!")