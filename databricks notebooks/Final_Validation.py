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
# MAGIC ###Row count at every layer

# COMMAND ----------

# Bronze
yellow_bronze = spark.read.option("recursiveFileLookup","true") \
    .parquet("abfss://bronze@azureadls45.dfs.core.windows.net/yellow/").count()
green_bronze = spark.read.option("recursiveFileLookup","true") \
    .parquet("abfss://bronze@azureadls45.dfs.core.windows.net/green/").count()
zones_bronze = spark.read.option("header","true") \
    .csv("abfss://bronze@azureadls45.dfs.core.windows.net/zones/").count()

# Silver
silver_trips = spark.read.format("delta") \
    .load("abfss://silver@azureadls45.dfs.core.windows.net/trips/").count()
silver_zones = spark.read.format("delta") \
    .load("abfss://silver@azureadls45.dfs.core.windows.net/zones/").count()

# Gold
gold_base = "abfss://gold@azureadls45.dfs.core.windows.net/"
borough = spark.read.format("delta").load(gold_base + "borough_revenue/").count()
peak = spark.read.format("delta").load(gold_base + "peak_hours/").count()
zones_gold = spark.read.format("delta").load(gold_base + "top_zones/").count()
payment = spark.read.format("delta").load(gold_base + "payment_behaviour/").count()

print("=" * 50)
print("BRONZE LAYER")
print(f"  Yellow trips:     {yellow_bronze:,}")
print(f"  Green trips:      {green_bronze:,}")
print(f"  Zones lookup:     {zones_bronze:,}")
print(f"  Total raw rows:   {yellow_bronze + green_bronze:,}")
print("=" * 50)
print("SILVER LAYER")
print(f"  Unified trips:    {silver_trips:,}")
print(f"  Zones:            {silver_zones:,}")
print("=" * 50)
print("GOLD LAYER")
print(f"  Borough revenue:  {borough:,}")
print(f"  Peak hours:       {peak:,}")
print(f"  Top zones:        {zones_gold:,}")
print(f"  Payment behaviour:{payment:,}")
print("=" * 50)
print("VALIDATION COMPLETE!")