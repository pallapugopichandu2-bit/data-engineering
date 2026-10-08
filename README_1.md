# NYC Taxi Trip Data Pipeline — Azure End-to-End Data Engineering

![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)
![ADF](https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)

## Overview

An end-to-end production-grade data engineering pipeline on Azure that ingests, transforms and aggregates **38.7 million NYC taxi trip records** using the **Medallion Architecture** (Bronze → Silver → Gold).

The pipeline is fully orchestrated by Azure Data Factory with automated failure alerting via Logic Apps and a weekly schedule trigger.

---

## Architecture

```
NYC TLC Source (public URLs)
        ↓  HTTP Linked Service
        ↓  ADF Copy Activity (parameterised datasets)
        ↓
ADLS Gen2 — Bronze Container
        yellow/year=2024/month=01..03/   38,219,112 rows
        green/year=2024/month=01..03/       502,755 rows
        zones/                                  530 rows
        ↓
Azure Databricks — PySpark Notebooks
        ↓  02_Bronze_to_Silver
        ↓
ADLS Gen2 — Silver Container (Delta Lake)
        trips/    37,320,445 rows (cleaned + unified)
        zones/          530 rows
        ↓  03_Silver_to_Gold
        ↓
ADLS Gen2 — Gold Container (Delta Lake)
        borough_revenue/      15 rows
        peak_hours/          336 rows
        top_zones/            50 rows
        payment_behaviour/    70 rows
        ↓
ADF Master Pipeline — full orchestration
        Schedule trigger: weekly
        Failure alerting: Logic Apps → email
```

---

## Tech Stack

| Service | Purpose |
|---|---|
| Azure Data Factory | Ingestion pipelines + master orchestration |
| Azure Data Lake Storage Gen2 | Storage for all 3 medallion layers |
| Azure Databricks | PySpark transformation notebooks |
| Delta Lake | Reliable, versioned storage for Silver + Gold |
| Azure Logic Apps | Failure alerting via email |
| PySpark | Data cleaning, transformation, aggregation |

---

## Dataset

**Source:** NYC Taxi & Limousine Commission (TLC) public dataset

| File | Type | Size | Rows |
|---|---|---|---|
| yellow_tripdata_2024-01.parquet | Parquet | ~47 MB | ~8.4M |
| yellow_tripdata_2024-02.parquet | Parquet | ~47 MB | ~8.4M |
| yellow_tripdata_2024-03.parquet | Parquet | ~52 MB | ~8.4M |
| green_tripdata_2024-01.parquet | Parquet | ~2 MB | ~167K |
| green_tripdata_2024-02.parquet | Parquet | ~2 MB | ~167K |
| green_tripdata_2024-03.parquet | Parquet | ~2 MB | ~167K |
| taxi_zone_lookup.csv | CSV | <1 MB | 265 |

---

## Pipeline Structure

### ADF Pipelines

```
PL_Master_Ingestion (master orchestration)
├── Execute_yellow    → PL_Ingest_Yellow
│   ├── Copy_Yellow_Jan  (HTTP → Bronze)
│   ├── Copy_Yellow_Feb  (HTTP → Bronze)
│   └── Copy_Yellow_Mar  (HTTP → Bronze)
├── Execute_green     → PL_Ingest_Green
│   ├── Copy_Green_Jan   (HTTP → Bronze)
│   ├── Copy_Green_Feb   (HTTP → Bronze)
│   └── Copy_Green_Mar   (HTTP → Bronze)
├── Execute_zones     → PL_Ingest_Zones
│   └── Copy_Zones       (HTTP → Bronze)
├── ACT_Bronze_to_Silver  (Databricks Notebook)
├── ACT_Silver_to_Gold    (Databricks Notebook)
├── ACT_Register_Tables   (Databricks Notebook)
└── Alert_On_Failure      (Web Activity → Logic Apps)
```

### Databricks Notebooks

| Notebook | Purpose |
|---|---|
| `00_Mount_Storage` | Configure ADLS Gen2 access via abfss:// |
| `01_Explore_Bronze` | EDA — schema discovery, null analysis, row counts |
| `02_Bronze_to_Silver` | Clean, unify Yellow + Green, write Delta |
| `03_Silver_to_Gold` | Aggregate 4 analytics tables, write Delta |
| `04_Register_Tables` | Register Gold tables as SQL views |
| `05_Delta_Advanced` | Time travel, VACUUM, schema evolution |

---

## Key Engineering Decisions

### 1. Parameterised Datasets
Instead of creating 7 separate ADF datasets, I built 2 parameterised datasets using `@dataset().folderPath` and `@dataset().relativePath` dynamic expressions. This reduced 14 datasets to 4 and makes adding new months a zero-pipeline-change operation.

### 2. Hive-style Partitioning in Bronze
```
bronze/yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet
```
Partition pruning allows Spark to skip irrelevant partitions when reading, improving query performance at scale.

### 3. Schema Harmonisation — Yellow vs Green
Yellow taxi uses `tpep_pickup_datetime` / `tpep_dropoff_datetime`.
Green taxi uses `lpep_pickup_datetime` / `lpep_dropoff_datetime`.

Solution: rename Green columns to match Yellow schema before union, add null columns for fields unique to each type (`Airport_fee`, `ehail_fee`, `trip_type`), then union into a single unified Silver table.

### 4. Delta Lake for Silver and Gold
Bronze stays as raw Parquet (immutable landing zone). Silver and Gold use Delta Lake for ACID transactions, time travel, schema enforcement and efficient upserts.

### 5. Calculated Columns Added in Silver
```python
trip_duration_minutes = (dropoff_time - pickup_time) / 60
pickup_hour           = hour(tpep_pickup_datetime)
pickup_day_of_week    = dayofweek(tpep_pickup_datetime)
pickup_month          = month(tpep_pickup_datetime)
```

### 6. Master Orchestration Pipeline
Single ADF pipeline orchestrates the entire flow — ingestion → Bronze → Silver → Gold → registration — with success dependency arrows ensuring each step only starts when the previous succeeds.

---

## Gold Analytics Tables

### borough_revenue
Revenue and trip metrics grouped by borough and taxi type.
```sql
SELECT pickup_borough, taxi_type, total_trips, total_revenue, avg_fare
FROM borough_revenue
ORDER BY total_revenue DESC
```

### peak_hours
Trip volumes by hour of day, day of week and taxi type.
```sql
SELECT pickup_hour, pickup_day_of_week, taxi_type, total_trips
FROM peak_hours
ORDER BY total_trips DESC
```

### top_zones
Top 50 pickup zones by trip volume with revenue metrics.
```sql
SELECT pickup_zone, pickup_borough, total_trips, total_revenue
FROM top_zones
ORDER BY total_trips DESC
```

### payment_behaviour
Payment type analysis by borough and taxi type with tip metrics.
```sql
SELECT payment_type, pickup_borough, taxi_type, total_trips, avg_tip
FROM payment_behaviour
ORDER BY total_trips DESC
```

---

## Delta Lake Advanced Features

### Time Travel
```python
# Read data as of version 0
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("abfss://gold@azureadls45.dfs.core.windows.net/borough_revenue/")
```

### Restore to Previous Version
```python
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, gold_path)
dt.restoreToVersion(0)
```

### Schema Evolution
```python
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(path)
```

---

## Validation Results

| Layer | Table | Row Count |
|---|---|---|
| Bronze | Yellow trips | 38,219,112 |
| Bronze | Green trips | 502,755 |
| Bronze | Zones | 530 |
| Silver | Unified trips | 37,320,445 |
| Silver | Zones | 530 |
| Gold | Borough revenue | 15 |
| Gold | Peak hours | 336 |
| Gold | Top zones | 50 |
| Gold | Payment behaviour | 70 |

**1,401,422 bad rows removed during Silver cleaning** (negative fares, zero distances, null timestamps, trips > 300 minutes).

---

## Project Structure

```
azure-nyc-tlc-data-pipeline/
├── notebooks/
│   ├── 00_Mount_Storage.py
│   ├── 01_Explore_Bronze.py
│   ├── 02_Bronze_to_Silver.py
│   ├── 03_Silver_to_Gold.py
│   ├── 04_Register_Tables.py
│   └── 05_Delta_Advanced.py
├── adf_pipelines/
│   ├── PL_Master_Ingestion.json
│   ├── PL_Ingest_Yellow.json
│   ├── PL_Ingest_Green.json
│   └── PL_Ingest_Zones.json
├── screenshots/
│   ├── architecture_diagram.png
│   ├── master_pipeline_canvas.png
│   ├── bronze_adls_structure.png
│   ├── silver_validation.png
│   └── gold_validation.png
└── README.md
```

---

## How to Run

### Prerequisites
- Azure subscription with free credits or pay-as-you-go
- Azure Data Lake Storage Gen2 account
- Azure Data Factory instance
- Azure Databricks workspace (Premium tier — Classic)
- Azure Logic Apps (Consumption plan)

### Steps
1. Create ADLS Gen2 with 3 containers: `bronze`, `silver`, `gold`
2. Create ADF instance and import pipeline JSON files
3. Create Databricks workspace and cluster (Single Node, auto-terminate 20 min)
4. Create a Databricks secret scope named `azure` and store the ADLS access key as `storage-account-key`; the notebooks retrieve it with `dbutils.secrets.get`
5. Run `00_Mount_Storage` to verify ADLS connection
6. Run `PL_Master_Ingestion` in ADF — full end-to-end pipeline executes automatically

---

## Project Owner

**Pallapu Gopi Chandu**
[LinkedIn](https://www.linkedin.com/in/pallapu-gopi-chandu-0ba8b525a/) | [GitHub](https://github.com/pallapugopichandu2-bit)

---

*Built as part of a 11-day Azure Data Engineering learning series — documenting the full journey from zero to production pipeline.*
