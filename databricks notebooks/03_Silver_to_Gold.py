# Databricks notebook source
# MAGIC %md
# MAGIC ### Configure ADLS

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.azureadls45.dfs.core.windows.net",
    dbutils.secrets.get("azure", "storage-account-key")
)
print("ADLS configured!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read silver tables

# COMMAND ----------

from pyspark.sql.functions import (
    unix_timestamp, round as spark_round,
    hour, dayofweek, month, col
)

# Read Silver
df_trips_raw = spark.read.format("delta").load(
    "abfss://silver@azureadls45.dfs.core.windows.net/trips/"
)
df_zones = spark.read.format("delta").load(
    "abfss://silver@azureadls45.dfs.core.windows.net/zones/"
)

# Add calculated columns on the fly
df_trips = df_trips_raw \
    .withColumn(
        "trip_duration_minutes",
        spark_round(
            (unix_timestamp("tpep_dropoff_datetime") -
             unix_timestamp("tpep_pickup_datetime")) / 60, 2
        )
    ) \
    .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime")) \
    .withColumn("pickup_month", month("tpep_pickup_datetime")) \
    .filter(col("trip_duration_minutes") > 0) \
    .filter(col("trip_duration_minutes") < 300)

print("Trips:", df_trips.count())
print("Zones:", df_zones.count())
print("pickup_hour in columns:", "pickup_hour" in df_trips.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Join trips with zones

# COMMAND ----------

from pyspark.sql.functions import col

# Join trips with pickup zone info
df_joined = df_trips.join(
    df_zones.select(
        col("LocationID").alias("PULocationID"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone"),
        col("service_zone").alias("pickup_service_zone")
    ),
    on="PULocationID",
    how="left"
)

print("Joined rows:", df_joined.count())
print("Done — trips now have borough and zone info!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold table 1: Borough Revenue

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, count, avg, round as spark_round

df_borough_revenue = df_joined \
    .groupBy("pickup_borough", "taxi_type") \
    .agg(
        count("*").alias("total_trips"),
        spark_round(spark_sum("fare_amount"), 2).alias("total_fare"),
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance"),
        spark_round(avg("tip_amount"), 2).alias("avg_tip")
    ) \
    .orderBy("total_revenue", ascending=False)

display(df_borough_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold table 2: Peak Hours

# COMMAND ----------

from pyspark.sql.functions import count, avg
from pyspark.sql.functions import round as spark_round

df_peak_hours = df_trips \
    .groupBy("pickup_hour", "pickup_day_of_week", "taxi_type") \
    .agg(
        count("*").alias("total_trips"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_mins"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance")
    ) \
    .orderBy("total_trips", ascending=False)

display(df_peak_hours)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold table 3: Top Zones

# COMMAND ----------

df_top_zones = df_joined \
    .groupBy("pickup_zone", "pickup_borough", "pickup_service_zone") \
    .agg(
        count("*").alias("total_trips"),
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("tip_amount"), 2).alias("avg_tip")
    ) \
    .orderBy("total_trips", ascending=False) \
    .limit(50)

display(df_top_zones)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold table 4: Payment Behaviour

# COMMAND ----------

df_payment = df_joined \
    .groupBy("payment_type", "pickup_borough", "taxi_type") \
    .agg(
        count("*").alias("total_trips"),
        spark_round(avg("tip_amount"), 2).alias("avg_tip"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("total_amount"), 2).alias("avg_total")
    ) \
    .orderBy("total_trips", ascending=False)

display(df_payment)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write all 4 Gold tables as Delta

# COMMAND ----------

gold_base = "abfss://gold@azureadls45.dfs.core.windows.net/"

# Write borough revenue
df_borough_revenue.write.format("delta").mode("overwrite") \
    .save(gold_base + "borough_revenue/")
print("borough_revenue written!")

# Write peak hours
df_peak_hours.write.format("delta").mode("overwrite") \
    .save(gold_base + "peak_hours/")
print("peak_hours written!")

# Write top zones
df_top_zones.write.format("delta").mode("overwrite") \
    .save(gold_base + "top_zones/")
print("top_zones written!")

# Write payment behaviour
df_payment.write.format("delta").mode("overwrite") \
    .save(gold_base + "payment_behaviour/")
print("payment_behaviour written!")

print("\nAll 4 Gold tables written successfully!")

# COMMAND ----------

print(df_trips.columns)

# COMMAND ----------

gold_base = "abfss://gold@azureadls45.dfs.core.windows.net/"
tables = ["borough_revenue", "peak_hours", "top_zones", "payment_behaviour"]

for table in tables:
    cnt = spark.read.format("delta").load(gold_base + table + "/").count()
    print(f"{table}: {cnt} rows")