# Databricks notebook source
# Configure ADLS access
spark.conf.set(
    "fs.azure.account.key.azureadls45.dfs.core.windows.net",
    dbutils.secrets.get("azure", "storage-account-key")
)
print("ADLS access configured!")

# COMMAND ----------

df_yellow = spark.read \
    .option("recursiveFileLookup", "true") \
    .parquet("abfss://bronze@azureadls45.dfs.core.windows.net/yellow/")

print("Row count:", df_yellow.count())
print("Columns:", len(df_yellow.columns))

# COMMAND ----------

df_green=spark.read\
    .option("recursiveFileLookup", "true")\
    .parquet("abfss://bronze@azureadls45.dfs.core.windows.net/green/")

print("Row count:", df_green.count())
print("column count:",len(df_green.columns))

# COMMAND ----------

print("yellow_schema")
df_yellow.printSchema()

# COMMAND ----------

print("green_schema:")
df_green.printSchema()

# COMMAND ----------

# Preview yellow data
display(df_yellow.limit(5))

# COMMAND ----------

#preview green data
display(df_green.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum

# Count nulls in every column of yellow
print("=== YELLOW NULL COUNTS ===")
df_yellow.select([
    spark_sum(col(c).isNull().cast("int")).alias(c) 
    for c in df_yellow.columns
]).show()

# COMMAND ----------

df_yellow.select(
    "fare_amount",
    "tip_amount",
    "total_amount"
).show(10)

# COMMAND ----------

from pyspark.sql.functions import col, sum

null_counts = df_yellow.select([
    sum(col(c).isNull().cast("int")).alias(c) for c in df_yellow.columns
])

null_counts.show(vertical=True)

# COMMAND ----------

from pyspark.sql.functions import col, sum

df_green.select([
    sum(col(c).isNull().cast("int")).alias(c) for c in df_green.columns
]).show(vertical=True)

# COMMAND ----------

df_zones = spark.read \
    .option("header", "true") \
    .option("recursiveFileLookup", "true") \
    .csv("abfss://bronze@azureadls45.dfs.core.windows.net/zones/")
    
print("Zones row count:", df_zones.count())
display(df_zones.limit(5))