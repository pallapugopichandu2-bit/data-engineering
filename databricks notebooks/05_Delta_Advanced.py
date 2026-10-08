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
# MAGIC ###Check Delta table history (audit log)

# COMMAND ----------

from delta.tables import DeltaTable

gold_base = "abfss://gold@azureadls45.dfs.core.windows.net/"

dt = DeltaTable.forPath(spark, gold_base + "borough_revenue/")
dt.history().select("version", "timestamp", "operation", "operationParameters").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Time travel — read previous version

# COMMAND ----------

# Read version 0 (first ever write)
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(gold_base + "borough_revenue/")

print("Version 0 row count:", df_v0.count())

# Read current version
df_current = spark.read.format("delta") \
    .load(gold_base + "borough_revenue/")

print("Current version row count:", df_current.count())
print("Time travel working!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Simulate bad data load + restore

# COMMAND ----------

# Simulate a bad overwrite — write empty data
df_current.limit(0).write.format("delta") \
    .mode("overwrite") \
    .save(gold_base + "borough_revenue/")

print("Bad data written — row count now:", 
    spark.read.format("delta").load(gold_base + "borough_revenue/").count())

# Restore to previous version using time travel
dt = DeltaTable.forPath(spark, gold_base + "borough_revenue/")
dt.restoreToVersion(0)

print("Restored! Row count now:", 
    spark.read.format("delta").load(gold_base + "borough_revenue/").count())
print("Time travel restore successful!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Delta table details

# COMMAND ----------

spark.sql(f"""
    DESCRIBE DETAIL delta.`{gold_base}borough_revenue/`
""").select("format", "numFiles", "sizeInBytes", "location").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###VACUUM — clean old files

# COMMAND ----------

# Set retention to 0 hours for demo (normally 7 days)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

dt = DeltaTable.forPath(spark, gold_base + "borough_revenue/")
dt.vacuum(0)

print("VACUUM complete — old files cleaned!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema evolution demo

# COMMAND ----------

from pyspark.sql.functions import lit

# Add a new column to existing Delta table — schema evolution
df_current = spark.read.format("delta").load(gold_base + "peak_hours/")
df_with_new_col = df_current.withColumn("data_source", lit("nyc_tlc_2024"))

df_with_new_col.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_base + "peak_hours/")

print("Schema evolved — new column added!")
print("New columns:", spark.read.format("delta").load(gold_base + "peak_hours/").columns)