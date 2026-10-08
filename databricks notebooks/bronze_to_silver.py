# Databricks notebook source
# MAGIC %md
# MAGIC ###configure adls acess
# MAGIC

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.azureadls45.dfs.core.windows.net",
    dbutils.secrets.get("azure", "storage-account-key")
)
print("ADLS configured!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read Yellow and Green from Bronze
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit, unix_timestamp

# Read yellow
df_yellow = spark.read \
    .option("recursiveFileLookup", "true") \
    .parquet("abfss://bronze@azureadls45.dfs.core.windows.net/yellow/")

# Read green
df_green = spark.read \
    .option("recursiveFileLookup", "true") \
    .parquet("abfss://bronze@azureadls45.dfs.core.windows.net/green/")

print("Yellow rows:", df_yellow.count())
print("Green rows:", df_green.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename Green columns to match Yellow
# MAGIC

# COMMAND ----------

# Rename lpep → tpep so both schemas match
df_green = df_green \
    .withColumnRenamed("lpep_pickup_datetime", "tpep_pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "tpep_dropoff_datetime")

print("Green columns renamed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add missing columns to each dataframe
# MAGIC

# COMMAND ----------

# Add columns that yellow has but green doesn't
df_green = df_green.withColumn("Airport_fee", lit(None).cast("double"))

# Add columns that green has but yellow doesn't
df_yellow = df_yellow \
    .withColumn("trip_type", lit(None).cast("long")) \
    .withColumn("ehail_fee", lit(None).cast("double"))

# Add taxi_type identifier — critical for knowing source after union
df_yellow = df_yellow.withColumn("taxi_type", lit("yellow"))
df_green  = df_green.withColumn("taxi_type", lit("green"))

print("Missing columns added!")
print("Yellow columns now:", len(df_yellow.columns))
print("Green columns now:", len(df_green.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ##selecting only the columns that we want
# MAGIC

# COMMAND ----------

# Define the unified column list — same order for both
unified_cols = [
    "taxi_type",
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
    "Airport_fee",
    "trip_type",
    "ehail_fee"
]

df_yellow_clean = df_yellow.select(unified_cols)
df_green_clean  = df_green.select(unified_cols)

print("Columns aligned!")
print("Yellow:", df_yellow_clean.columns)
print("Green:", df_green_clean.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Union Yellow + Green

# COMMAND ----------

# Stack both dataframes into one unified table
df_combined = df_yellow_clean.union(df_green_clean)
print("Combined rows before cleaning:", df_combined.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Clean bad data
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

df_silver = df_combined \
    .filter(col("tpep_pickup_datetime").isNotNull()) \
    .filter(col("tpep_dropoff_datetime").isNotNull()) \
    .filter(col("fare_amount") > 0) \
    .filter(col("trip_distance") > 0) \
    .filter(col("total_amount") > 0) \
    .filter(col("PULocationID").isNotNull()) \
    .filter(col("DOLocationID").isNotNull())

print("Rows after cleaning:", df_silver.count())
print("Rows removed:", df_combined.count() - df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to Silver as Delta

# COMMAND ----------

silver_path = "abfss://silver@azureadls45.dfs.core.windows.net/trips/"

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

print("Silver Delta table written successfully!")
print("Path:", silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC  ###Read Zones and write to Silver

# COMMAND ----------

# Read zones from bronze
df_zones = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("abfss://bronze@azureadls45.dfs.core.windows.net/zones/")

print("Zones rows:", df_zones.count())
display(df_zones.limit(5))

# Write zones to silver as Delta
zones_path = "abfss://silver@azureadls45.dfs.core.windows.net/zones/"

df_zones.write \
    .format("delta") \
    .mode("overwrite") \
    .save(zones_path)

print("Zones Delta table written to Silver!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Verify Silver

# COMMAND ----------

# Read back and confirm
df_verify = spark.read.format("delta").load(
    "abfss://silver@azureadls45.dfs.core.windows.net/trips/"
)
print("Silver row count:", df_verify.count())
print("Silver columns:", df_verify.columns)
display(df_verify.limit(5))