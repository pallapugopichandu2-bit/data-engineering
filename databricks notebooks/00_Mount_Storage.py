# Databricks notebook source
# Configure ADLS access directly — no mounting needed
spark.conf.set(
    "fs.azure.account.key.azureadls45.dfs.core.windows.net",
    dbutils.secrets.get("azure", "storage-account-key")
)
print("ADLS access configured successfully!")

# COMMAND ----------

# Test — list files in bronze container
files = dbutils.fs.ls("abfss://bronze@azureadls45.dfs.core.windows.net/")
for f in files:
    print(f.path)