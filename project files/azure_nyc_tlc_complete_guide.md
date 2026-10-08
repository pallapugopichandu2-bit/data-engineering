# Azure ADF + Databricks: NYC TLC Trip Analytics
### A Complete Beginner-to-End Guide — Every Step in the Exact Order You Must Do Them

> **Dataset links validated live from nyc.gov on June 3, 2026**
> **Tech Stack:** Azure Data Factory · Databricks · Delta Lake · Blob Storage · PySpark · Logic Apps

---

## Before You Begin — Read This First

This guide follows a **strict execution order**. Every step depends on the step before it.
Do not skip steps or jump ahead. Here is the complete map of what you will build and in what order:

```
YOUR LOCAL COMPUTER
│
├── STEP 1  → Create your free Azure account
├── STEP 2  → Set a billing budget alert (do this before anything else)
└── STEP 3  → Download 7 dataset files to your laptop
                        │
                        ▼
AZURE PORTAL (portal.azure.com)
│
├── STEP 4  → Create a Resource Group (the container for everything)
├── STEP 5  → Create Blob Storage + 4 containers
├── STEP 6  → Upload your 7 downloaded files into Blob Storage   ← needs step 5
├── STEP 7  → Create Databricks workspace + cluster
└── STEP 8  → Create Azure Data Factory
                        │
                        ▼
DATABRICKS — Write & Test Notebooks One by One
│
├── STEP 9  → Notebook: Mount Blob Storage        ← needs steps 5 + 7
├── STEP 10 → Notebook: Explore Bronze Data       ← needs steps 6 + 9
├── STEP 11 → Notebook: Bronze → Silver           ← needs step 10
├── STEP 12 → Notebook: Silver → Gold             ← needs step 11
├── STEP 13 → Notebook: Register Tables           ← needs step 12
└── STEP 14 → Notebook: Delta Advanced Features   ← needs step 11
                        │
                        ▼
AZURE DATA FACTORY — Configure Connections + Pipelines
│
├── STEP 15 → Create Blob Storage Linked Service  ← needs step 8
├── STEP 16 → Create Databricks Linked Service    ← needs steps 7 + 8
├── STEP 17 → Create Source Datasets              ← needs step 15
├── STEP 18 → Create Bronze Sink Dataset          ← needs step 15
├── STEP 19 → Build 3 Ingestion Pipelines         ← needs steps 17 + 18
└── STEP 20 → Test Ingestion Pipelines            ← needs steps 6 + 19
                        │
                        ▼
LOGIC APPS — Create + Configure Email Alerting
│
├── STEP 21 → Create Logic App
└── STEP 22 → Configure email alert workflow + save HTTP URL   ← needs step 21
                        │
                        ▼
ADF — Build Master Orchestration Pipeline
│
├── STEP 23 → Build master pipeline (connects all notebooks)   ← needs steps 13 + 16
├── STEP 24 → Wire in failure alert                            ← needs step 22
├── STEP 25 → Add schedule trigger
└── STEP 26 → Publish everything
                        │
                        ▼
FINAL VALIDATION
│
├── STEP 27 → Run full end-to-end pipeline + check row counts
└── STEP 28 → Test the failure alert email
                        │
                        ▼
PORTFOLIO WRAP-UP (no Azure work needed)
└── STEP 29 → Resume bullets, GitHub README, interview story
```

**Time estimate:** 2–3 days working a few hours each day.
**Cost estimate:** $15–25 of your $200 free credit if you manage Databricks carefully.

---

## Section A — Pre-Work (Your Local Computer)

> You do not need Azure yet for these steps. This is all on your own laptop.

---

### STEP 1 — Create Your Free Azure Account

**What to do:**
1. Go to https://azure.microsoft.com/en-us/free
2. Click **"Start free"**
3. Sign in with a Microsoft account (or create one — it's free)
4. Complete the sign-up form. You will need:
   - A phone number (for verification)
   - A credit card (for identity only — you will NOT be charged during the free tier)
5. Once signed up, you will land on the Azure Portal at https://portal.azure.com

**What you get:**
- $200 USD free credit valid for 30 days
- 12 months of free services after the 30 days

> **💡 Note:** Azure asks for a credit card to verify identity, but will NOT charge you automatically. You have to manually upgrade to "Pay as you go" before any charges happen. If you use all $200 or the 30 days expire, your services simply pause — you won't receive a surprise bill.

---

### STEP 2 — Set a Billing Budget Alert (Do This Before Anything Else)

**What to do:**
1. In the Azure Portal, search **"Cost Management + Billing"** in the top search bar
2. Click **"Cost Management"** from the results
3. In the left menu, click **"Budgets"**
4. Click **"+ Add"**
5. Fill in:
   - **Name:** `trip-project-budget`
   - **Budget amount:** `$50`
   - **Reset period:** Monthly
6. Click **"Next"**
7. Set two alerts:
   - Alert 1: **50% of budget** → enter your email address
   - Alert 2: **80% of budget** → enter your email address
8. Click **"Create"**

> **💡 Note:** This is the single most important setup step for anyone using free credits. The Databricks cluster is the only real cost in this project — a single forgotten overnight run can cost $3–4. These email alerts warn you before costs spiral. Do this before creating any other resource.

---

### STEP 3 — Download the Dataset Files to Your Laptop

**What to do:**
1. Create a folder on your desktop called `nyc_tlc_raw`
2. Inside it, create 3 subfolders: `yellow`, `green`, `zones`
3. Download each file below by clicking the link and saving it to the correct subfolder:

#### Into the `yellow` subfolder — download all 3:

| File | Download Link | Size |
|---|---|---|
| January 2024 | https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet | ~47 MB |
| February 2024 | https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet | ~47 MB |
| March 2024 | https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet | ~52 MB |

#### Into the `green` subfolder — download all 3:

| File | Download Link | Size |
|---|---|---|
| January 2024 | https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet | ~2 MB |
| February 2024 | https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-02.parquet | ~2 MB |
| March 2024 | https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-03.parquet | ~2 MB |

#### Into the `zones` subfolder — download 1:

| File | Download Link | Size |
|---|---|---|
| Taxi Zone Lookup | https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv | <1 MB |

**Your folder should now look like this:**
```
nyc_tlc_raw/
├── yellow/
│   ├── yellow_tripdata_2024-01.parquet
│   ├── yellow_tripdata_2024-02.parquet
│   └── yellow_tripdata_2024-03.parquet
├── green/
│   ├── green_tripdata_2024-01.parquet
│   ├── green_tripdata_2024-02.parquet
│   └── green_tripdata_2024-03.parquet
└── zones/
    └── taxi_zone_lookup.csv
```

**Reference links (open these in your browser to understand the columns):**

| Dictionary | Link |
|---|---|
| Yellow Taxi column guide | https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf |
| Green Taxi column guide | https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf |
| Official TLC data page | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |

> **💡 Note:** These are `.parquet` files — a compressed binary format that Spark reads extremely efficiently. You cannot open them in Excel. That is fine — you will explore them inside Databricks in Step 10. The zone lookup file is a plain `.csv` you can open in Excel to preview.

---

## Section B — Azure Infrastructure Setup

> Everything in this section is done inside the Azure Portal at https://portal.azure.com

---

### STEP 4 — Create a Resource Group

**What is a Resource Group?**
Think of it as a project folder in Azure. Every Azure service you create (storage, Databricks, Data Factory) will go inside this folder. When the project is done, you delete one folder and everything inside disappears — no manual cleanup.

**What to do:**
1. In the Azure Portal, click the search bar at the top
2. Type **"Resource groups"** and click it from the dropdown
3. Click **"+ Create"** (blue button, top left)
4. Fill in:
   - **Subscription:** Your subscription name (already selected)
   - **Resource group name:** `rg-nyc-trip-analytics`
   - **Region:** `East US`
5. Click **"Review + Create"** → then **"Create"**
6. Wait a few seconds → click **"Go to resource group"**

> **💡 Note:** The region (`East US`) is important — create all your resources in the same region. Mixing regions causes slower data transfer and sometimes extra costs. `East US` is the cheapest and most common region for learning projects.

---

### STEP 5 — Create Azure Blob Storage and 4 Containers

**What is Blob Storage?**
It is Azure's file storage — like a hard drive in the cloud. This is where all your data files will live: the raw uploads, the bronze Parquet files, the silver Delta tables, and the gold Delta tables.

**What to do — Part A: Create the Storage Account:**
1. Search **"Storage accounts"** in the top search bar → click it
2. Click **"+ Create"**
3. Fill in:
   - **Subscription:** Your subscription
   - **Resource group:** `rg-nyc-trip-analytics`
   - **Storage account name:** `nyclctripstore` *(must be lowercase letters and numbers only, globally unique — if this name is taken, try `nyclctripstore2` or add your initials)*
   - **Region:** `East US`
   - **Performance:** Standard
   - **Redundancy:** Locally-redundant storage (LRS) ← cheapest option
4. Click **"Review"** → **"Create"**
5. When deployment completes, click **"Go to resource"**

**What to do — Part B: Create 4 Containers:**
1. In the left menu of your storage account, click **"Containers"**
2. Click **"+ Container"** and create each of these — one at a time:

| Container Name | What It Stores |
|---|---|
| `raw-upload` | Your downloaded Parquet files (simulates a source landing zone) |
| `bronze` | Raw copies of the data exactly as received — no changes |
| `silver` | Cleaned, standardized Delta Lake tables |
| `gold` | Final aggregated analytics tables |

For each container: type the name → set **"Public access level"** to **"Private"** → click **"Create"**

3. After creating all 4, your containers list should look like:
   ```
   raw-upload
   bronze
   silver
   gold
   ```

> **💡 Note:** The `raw-upload` container replaces the SQL Server that was mentioned earlier as a source. Since your data is already in Parquet files, uploading them to blob storage and reading from there is actually more realistic for modern data engineering projects. Most real pipelines start with files landing in object storage from upstream systems.

---

### STEP 6 — Upload Your Dataset Files to Blob Storage

> **Depends on:** STEP 5 (containers must exist first)

**What to do:**
1. Click on the `raw-upload` container (from your containers list)
2. Click **"Upload"** (top bar)
3. You need to create folders first. In the Upload panel:
   - Click **"Advanced"** to expand advanced options
   - In the **"Upload to folder"** box type: `yellow`
   - Click **"Browse for files"** → navigate to your `nyc_tlc_raw/yellow/` folder → select all 3 yellow Parquet files
   - Click **"Upload"**
4. Repeat for green files:
   - Click **"Upload"** again → **"Advanced"** → folder: `green`
   - Select all 3 green Parquet files → **"Upload"**
5. Repeat for the zone file:
   - Click **"Upload"** → **"Advanced"** → folder: `zones`
   - Select `taxi_zone_lookup.csv` → **"Upload"**

**Verify the upload worked:**
1. Go to **"Storage browser"** (left menu of your storage account)
2. Click **"Blob containers"** → **"raw-upload"**
3. You should see 3 folders: `yellow/`, `green/`, `zones/`
4. Click into each — the files should be there

> **💡 Note:** This upload step must happen before you write any Databricks notebooks, because the notebooks read from these paths. In a real production environment, files would land here automatically from source systems — but for this project, manual upload simulates that process.

---

### STEP 7 — Create Azure Databricks Workspace and Cluster

**What is Databricks?**
Databricks is a cloud platform for running Apache Spark — the distributed computing engine that processes millions of rows efficiently. You write Python (PySpark) code in notebooks inside Databricks, and it runs on a cluster of virtual machines.

**What to do — Part A: Create the Workspace:**
1. Search **"Azure Databricks"** in the portal → click it
2. Click **"+ Create"**
3. Fill in:
   - **Subscription:** Your subscription
   - **Resource group:** `rg-nyc-trip-analytics`
   - **Workspace name:** `databricks-nyc-trips`
   - **Region:** `East US`
   - **Pricing tier:** Select **"Trial (Premium — 14 days free DBUs)"** if visible, otherwise **"Standard"**
4. Click **"Review + Create"** → **"Create"**
5. Deployment takes 3–5 minutes. When done, click **"Go to resource"**
6. Click the blue **"Launch Workspace"** button — this opens Databricks in a new tab

**What to do — Part B: Create a Cluster:**
> A cluster is the group of virtual machines that runs your notebook code. You must have a running cluster to execute any notebook.

1. Inside the Databricks workspace, click **"Compute"** in the left sidebar
2. Click **"+ Create compute"**
3. Fill in:
   - **Cluster name:** `nyc-trips-cluster`
   - **Policy:** Unrestricted
   - **Single node** ← select this option (cheaper for dev work)
   - **Databricks runtime:** Select the latest **LTS** version shown (e.g. `15.4 LTS (Scala 2.12, Spark 3.5.0)`)
   - **Node type:** `Standard_DS3_v2`
   - **Auto terminate:** `20 minutes` ← CRITICAL: set this now
4. Click **"Create compute"**
5. Wait 5–8 minutes for the cluster to start. The status dot turns green when ready.

> **💡 Note:** The "20 minutes auto-terminate" setting is the most important cluster setting for a free-credit user. It means if you forget to stop the cluster after working, it automatically shuts down after 20 minutes of idle time. A running cluster costs ~$0.25–0.35 per hour. One forgotten overnight session = ~$3. Over 30 days that adds up fast.

---

### STEP 8 — Create Azure Data Factory

**What is Data Factory?**
Azure Data Factory (ADF) is your pipeline orchestration tool. It handles two jobs in this project: (1) moving raw files from `raw-upload` into the `bronze` container, and (2) triggering your Databricks notebooks to run in the correct sequence.

**What to do:**
1. Search **"Data factories"** in the portal → click it
2. Click **"+ Create"**
3. Fill in:
   - **Subscription:** Your subscription
   - **Resource group:** `rg-nyc-trip-analytics`
   - **Name:** `adf-nyc-trip-analytics`
   - **Region:** `East US`
   - **Version:** V2
4. Click **"Review + Create"** → **"Create"**
5. When deployed, click **"Go to resource"**
6. Click **"Launch Studio"** — this opens the ADF visual editor in a new tab

> **💡 Note:** ADF Studio is the visual drag-and-drop editor where you build pipelines. Think of it like a flowchart builder — you drag activities onto a canvas and connect them. Keep this tab open throughout the project.

---

## Section C — Databricks Notebooks

> **Important:** Write and run each notebook completely before moving to the next one.
> Each notebook depends on the previous one having worked correctly.
> A cluster must be running before you can execute any notebook — go to Compute and start it if the status is "Terminated".

---

### STEP 9 — Notebook: Mount Blob Storage

> **Depends on:** STEP 5 (storage exists) + STEP 7 (Databricks cluster is running)

**What does "mounting" mean?**
By default, Databricks can't see your Azure Blob Storage. Mounting creates a shortcut — it maps each container to a path like `/mnt/bronze/` so your notebooks can read and write files using simple paths instead of long complex URLs.

**What to do:**
1. In Databricks, click **"Workspace"** in the left sidebar
2. Click **"+ Create"** → **"Notebook"**
3. Fill in:
   - **Name:** `00_Mount_Storage`
   - **Default language:** Python
   - **Cluster:** Select `nyc-trips-cluster`
4. Click **"Create"**

**Get your storage account key:**
1. Open a new browser tab → go to Azure Portal
2. Search **"Storage accounts"** → click `nyclctripstore`
3. In the left menu, click **"Access keys"**
4. Click **"Show"** next to **key1** → copy the full key string

**Back in Databricks, paste this into the first cell:**

```python
# ── Configuration ─────────────────────────────────────────────
storage_account_name = "nyclctripstore"           # your storage account name
storage_account_key  = "PASTE_YOUR_KEY1_HERE"     # paste the key you just copied

# ── Mount all 4 containers ─────────────────────────────────────
containers = {
    "raw-upload" : "/mnt/raw_upload",   # note: hyphens not allowed in mount paths
    "bronze"     : "/mnt/bronze",
    "silver"     : "/mnt/silver",
    "gold"       : "/mnt/gold"
}

for container, mount_point in containers.items():
    # If already mounted, unmount first so this cell is safe to re-run
    try:
        dbutils.fs.unmount(mount_point)
    except:
        pass  # not mounted yet — that's fine

    dbutils.fs.mount(
        source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net",
        mount_point = mount_point,
        extra_configs = {
            f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net":
            storage_account_key
        }
    )
    print(f"✓ Mounted  {container}  →  {mount_point}")
```

**Run the cell:**
- Click the **▶ Run cell** button (or press `Shift + Enter`)
- You should see 4 lines each starting with `✓ Mounted`

**Verify the mount worked:**
```python
# Paste this into a new cell and run it
display(dbutils.fs.ls("/mnt/raw_upload"))
```
You should see the `yellow/`, `green/`, and `zones/` folders listed.

> **💡 Note:** Mounts persist across Databricks sessions — you only need to run this notebook once (or again if the cluster is replaced). The `try/except` block around unmount means the cell is safe to re-run without errors. The storage account key is sensitive — in a real production environment you would store it in Azure Key Vault rather than pasting it here.

---

### STEP 10 — Notebook: Explore Bronze Data

> **Depends on:** STEP 6 (files uploaded) + STEP 9 (storage mounted)

**Why explore before transforming?**
You need to understand what you have before you clean it. This step reveals the actual column names, data types, and data quality issues in both taxi datasets — issues you will fix in the next notebook.

**What to do:**
1. Create a new notebook: **"+ Create"** → **"Notebook"**
   - Name: `01_Explore_Bronze`
   - Language: Python
   - Cluster: `nyc-trips-cluster`

**Cell 1 — Explore Yellow Taxi:**
```python
# Read all 3 months of yellow taxi data at once (the * wildcard picks up all files)
df_yellow = spark.read.parquet("/mnt/raw_upload/yellow/")

print("=== YELLOW TAXI ===")
print(f"Total rows  : {df_yellow.count():,}")
print(f"Total columns: {len(df_yellow.columns)}")
print()
df_yellow.printSchema()
```

**Cell 2 — Explore Green Taxi:**
```python
df_green = spark.read.parquet("/mnt/raw_upload/green/")

print("=== GREEN TAXI ===")
print(f"Total rows  : {df_green.count():,}")
print(f"Total columns: {len(df_green.columns)}")
print()
df_green.printSchema()
```

**Cell 3 — Spot the key difference between Yellow and Green:**
```python
# Yellow uses tpep_pickup_datetime
# Green uses lpep_pickup_datetime
# This is the main schema difference you will fix in the Silver notebook

yellow_cols = set(df_yellow.columns)
green_cols  = set(df_green.columns)

print("Columns in Yellow but NOT in Green:")
for c in sorted(yellow_cols - green_cols):
    print(f"  - {c}")

print()
print("Columns in Green but NOT in Yellow:")
for c in sorted(green_cols - yellow_cols):
    print(f"  - {c}")
```

**Cell 4 — Check data quality (null rates):**
```python
from pyspark.sql import functions as F

print("=== NULL RATES — YELLOW TAXI (key columns) ===")
key_cols = ["tpep_pickup_datetime", "trip_distance", "fare_amount", "PULocationID"]

total = df_yellow.count()
for col_name in key_cols:
    nulls = df_yellow.filter(F.col(col_name).isNull()).count()
    pct   = nulls / total * 100
    print(f"  {col_name:<30} : {nulls:>8,} nulls  ({pct:.2f}%)")
```

**Cell 5 — Preview sample rows:**
```python
print("=== YELLOW TAXI — SAMPLE ROWS ===")
df_yellow.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "PULocationID",
    "DOLocationID"
).show(5)
```

**Cell 6 — Explore Zone Lookup:**
```python
df_zones = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/raw_upload/zones/taxi_zone_lookup.csv")

print("=== TAXI ZONE LOOKUP ===")
print(f"Total zones: {df_zones.count()}")
df_zones.show(10)
```

> **💡 Note:** The key discovery in this step is that Yellow Taxi uses `tpep_pickup_datetime` while Green Taxi uses `lpep_pickup_datetime`. If you tried to union these two datasets without fixing this, Spark would fail with a column mismatch error. The exploration step exists so you know exactly what to rename in the next notebook.

---

### STEP 11 — Notebook: Bronze to Silver Transformation

> **Depends on:** STEP 10 (you understand the schema differences from exploration)

**What happens in this notebook:**
- Read Yellow and Green Parquet files from `raw-upload`
- Rename columns so both datasets have identical column names
- Add a `taxi_type` column to know which dataset each row came from
- Remove bad rows (nulls, negative fares, impossible distances)
- Add calculated columns (trip duration, hour of day, fare per mile)
- Union both datasets into one unified table
- Write to `silver` container as Delta format

**What to do:**
1. Create notebook: `02_Bronze_to_Silver` | Language: Python

**Cell 1 — Read and standardize Yellow Taxi:**
```python
from pyspark.sql import functions as F

df_yellow_raw = spark.read.parquet("/mnt/raw_upload/yellow/")

# Rename the datetime columns to a standard name
# Add a column to identify this as yellow taxi data
df_yellow = df_yellow_raw \
    .withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime") \
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
    .withColumn("taxi_type", F.lit("yellow")) \
    .select(
        "VendorID", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance",
        "PULocationID", "DOLocationID",
        "fare_amount", "tip_amount", "total_amount",
        "payment_type", "taxi_type"
    )

print(f"Yellow rows before cleaning: {df_yellow.count():,}")
```

**Cell 2 — Read and standardize Green Taxi:**
```python
df_green_raw = spark.read.parquet("/mnt/raw_upload/green/")

# Green uses "lpep_" prefix instead of "tpep_" — rename to match Yellow
df_green = df_green_raw \
    .withColumnRenamed("lpep_pickup_datetime",  "pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
    .withColumn("taxi_type", F.lit("green")) \
    .select(
        "VendorID", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance",
        "PULocationID", "DOLocationID",
        "fare_amount", "tip_amount", "total_amount",
        "payment_type", "taxi_type"
    )

print(f"Green rows before cleaning: {df_green.count():,}")
```

**Cell 3 — Union both datasets and clean:**
```python
# Combine Yellow + Green into one dataset (they now have identical columns)
df_all = df_yellow.unionByName(df_green)
print(f"Combined rows: {df_all.count():,}")

# Remove rows with problems:
df_clean = df_all \
    .dropna(subset=["pickup_datetime", "fare_amount", "PULocationID"]) \
    .filter(F.col("fare_amount")    >  0)    \
    .filter(F.col("trip_distance")  >  0)    \
    .filter(F.col("trip_distance")  < 200)   \
    .filter(F.col("fare_amount")    < 1000)  \
    .dropDuplicates(["VendorID", "pickup_datetime", "PULocationID"])

print(f"Rows after cleaning: {df_clean.count():,}")
print(f"Rows removed: {df_all.count() - df_clean.count():,}")
```

**Cell 4 — Add enrichment columns:**
```python
df_silver = df_clean \
    .withColumn(
        "trip_duration_minutes",
        F.round(
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
            2
        )
    ) \
    .withColumn("pickup_hour",      F.hour("pickup_datetime")) \
    .withColumn("pickup_dayofweek", F.dayofweek("pickup_datetime")) \
    .withColumn("pickup_month",     F.month("pickup_datetime")) \
    .withColumn(
        "fare_per_mile",
        F.round(F.col("fare_amount") / F.col("trip_distance"), 2)
    ) \
    .withColumn("ingested_at", F.current_timestamp())

# Filter out trips with impossible duration
df_silver = df_silver \
    .filter(F.col("trip_duration_minutes") > 0) \
    .filter(F.col("trip_duration_minutes") < 300)

print("Final Silver schema:")
df_silver.printSchema()
print(f"Final Silver row count: {df_silver.count():,}")
```

**Cell 5 — Write trips to Silver as Delta:**
```python
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save("/mnt/silver/trips/")

print("✓ Silver trips written successfully")
print("  Location: /mnt/silver/trips/")
```

**Cell 6 — Write zone lookup to Silver as Delta:**
```python
df_zones = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/raw_upload/zones/taxi_zone_lookup.csv")

df_zones.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/silver/zones/")

print("✓ Silver zones written successfully")
df_zones.show(5)
```

**Cell 7 — Verify Silver was written correctly:**
```python
# Read back from Silver and confirm
df_check = spark.read.format("delta").load("/mnt/silver/trips/")
print(f"Silver trips readable: {df_check.count():,} rows")
print()
df_check.select("taxi_type", "pickup_datetime", "fare_amount", "trip_duration_minutes").show(5)
```

> **💡 Note:** Writing in **Delta format** instead of plain Parquet is the most important technical decision in this project. Delta adds a `_delta_log/` folder that tracks every write as a transaction — this gives you ACID guarantees (no partial writes), the ability to query old versions, and the ability to merge/update rows. You will see these features in action in Step 14.

---

### STEP 12 — Notebook: Silver to Gold Aggregations

> **Depends on:** STEP 11 (Silver tables must exist)

**What happens in this notebook:**
Read from the Silver Delta tables and produce 4 aggregated Gold tables that answer specific business questions.

**What to do:**
1. Create notebook: `03_Silver_to_Gold` | Language: Python

**Cell 1 — Read Silver tables and join with zones:**
```python
from pyspark.sql import functions as F

df_trips = spark.read.format("delta").load("/mnt/silver/trips/")
df_zones  = spark.read.format("delta").load("/mnt/silver/zones/")

print(f"Silver trips: {df_trips.count():,}")
print(f"Silver zones: {df_zones.count():,}")

# Join trips with zone names so we get borough and neighbourhood names
# We join twice: once for pickup zone, once for dropoff zone
df_enriched = df_trips \
    .join(
        df_zones.select(
            F.col("LocationID").alias("PULocationID"),
            F.col("Borough").alias("pickup_borough"),
            F.col("Zone").alias("pickup_zone")
        ),
        on="PULocationID",
        how="left"
    ) \
    .join(
        df_zones.select(
            F.col("LocationID").alias("DOLocationID"),
            F.col("Borough").alias("dropoff_borough"),
            F.col("Zone").alias("dropoff_zone")
        ),
        on="DOLocationID",
        how="left"
    )
```

**Cell 2 — Gold Table 1: Revenue by Borough and Taxi Type:**
```python
# Business question: Which boroughs generate the most revenue, 
# and does Yellow or Green cab dominate each borough?

gold_borough_revenue = df_enriched \
    .groupBy("pickup_borough", "taxi_type") \
    .agg(
        F.count("*")                          .alias("total_trips"),
        F.round(F.sum("fare_amount"), 2)      .alias("total_fare_revenue"),
        F.round(F.sum("tip_amount"), 2)       .alias("total_tips"),
        F.round(F.avg("fare_amount"), 2)      .alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2)    .alias("avg_distance_miles"),
        F.round(F.avg("trip_duration_minutes"), 1).alias("avg_duration_mins")
    ) \
    .orderBy(F.desc("total_fare_revenue"))

gold_borough_revenue.write \
    .format("delta").mode("overwrite") \
    .save("/mnt/gold/borough_revenue/")

print("✓ Gold: borough_revenue written")
gold_borough_revenue.show(truncate=False)
```

**Cell 3 — Gold Table 2: Peak Hours Demand:**
```python
# Business question: What are the busiest hours and days of the week?
# This tells a transport company when to have more drivers available.

gold_peak_hours = df_enriched \
    .groupBy("pickup_hour", "pickup_dayofweek", "taxi_type") \
    .agg(
        F.count("*")                          .alias("trip_count"),
        F.round(F.avg("fare_amount"), 2)      .alias("avg_fare"),
        F.round(F.sum("fare_amount"), 2)      .alias("total_revenue"),
        F.round(F.avg("passenger_count"), 1)  .alias("avg_passengers")
    ) \
    .orderBy(F.desc("trip_count"))

gold_peak_hours.write \
    .format("delta").mode("overwrite") \
    .save("/mnt/gold/peak_hours/")

print("✓ Gold: peak_hours written")
gold_peak_hours.show(10)
```

**Cell 4 — Gold Table 3: Top Pickup Zones:**
```python
# Business question: Which neighbourhoods are the most popular pickup spots?
# Useful for fleet positioning decisions.

gold_top_zones = df_enriched \
    .groupBy("pickup_zone", "pickup_borough") \
    .agg(
        F.count("*")                       .alias("total_pickups"),
        F.round(F.sum("fare_amount"), 2)   .alias("total_revenue"),
        F.round(F.avg("tip_amount"), 2)    .alias("avg_tip"),
        F.round(F.avg("trip_distance"), 2) .alias("avg_distance_miles")
    ) \
    .orderBy(F.desc("total_pickups")) \
    .limit(50)

gold_top_zones.write \
    .format("delta").mode("overwrite") \
    .save("/mnt/gold/top_zones/")

print("✓ Gold: top_zones written (top 50 zones)")
gold_top_zones.show(10)
```

**Cell 5 — Gold Table 4: Payment Behaviour:**
```python
# Business question: Do riders pay more tips by credit card vs cash?
# Does this vary by borough?

gold_payment_behaviour = df_enriched \
    .filter(F.col("payment_type").isin([1, 2])) \
    .withColumn(
        "payment_label",
        F.when(F.col("payment_type") == 1, "Credit card")
         .when(F.col("payment_type") == 2, "Cash")
    ) \
    .groupBy("payment_label", "pickup_borough", "taxi_type") \
    .agg(
        F.count("*")                                               .alias("trip_count"),
        F.round(F.avg("tip_amount"), 2)                           .alias("avg_tip"),
        F.round(F.avg("fare_amount"), 2)                          .alias("avg_fare"),
        F.round(F.sum("tip_amount") / F.sum("fare_amount") * 100, 1).alias("tip_pct_of_fare")
    ) \
    .orderBy("pickup_borough", "payment_label")

gold_payment_behaviour.write \
    .format("delta").mode("overwrite") \
    .save("/mnt/gold/payment_behaviour/")

print("✓ Gold: payment_behaviour written")
gold_payment_behaviour.show(truncate=False)
```

> **💡 Note:** Each gold table answers a specific business question. This is intentional — a well-designed gold layer never holds all metrics in one giant table. Narrow, purpose-built tables are faster to query and easier for business analysts to understand. The borough + taxi_type breakdown is your portfolio's key insight: it answers which service type should have more drivers in which area on which day.

---

### STEP 13 — Notebook: Register Tables in Metastore

> **Depends on:** STEP 12 (Gold tables must be written to storage)

**What is the Metastore?**
The metastore is Databricks' internal catalog. When you "register" a Delta table in the metastore, you give it a name that anyone can query with plain SQL — without needing to know the storage path. Think of it as adding the table to a table of contents.

**What to do:**
1. Create notebook: `04_Register_Tables` | Language: Python

```python
# Create a database to hold all gold tables
spark.sql("CREATE DATABASE IF NOT EXISTS nyc_trip_analytics")
print("✓ Database created: nyc_trip_analytics")
print()

# Register each gold table
tables = {
    "borough_revenue"   : "/mnt/gold/borough_revenue/",
    "peak_hours"        : "/mnt/gold/peak_hours/",
    "top_zones"         : "/mnt/gold/top_zones/",
    "payment_behaviour" : "/mnt/gold/payment_behaviour/"
}

for table_name, path in tables.items():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS nyc_trip_analytics.{table_name}
        USING DELTA
        LOCATION '{path}'
    """)
    count = spark.sql(
        f"SELECT COUNT(*) FROM nyc_trip_analytics.{table_name}"
    ).collect()[0][0]
    print(f"✓ Registered: nyc_trip_analytics.{table_name}  ({count:,} rows)")

print()
print("All registered tables:")
spark.sql("SHOW TABLES IN nyc_trip_analytics").show()
```

**Test a SQL query against the gold layer:**
```python
# This is how an analyst would query your gold data
spark.sql("""
    SELECT
        pickup_borough,
        taxi_type,
        total_trips,
        total_fare_revenue,
        avg_fare
    FROM nyc_trip_analytics.borough_revenue
    ORDER BY total_fare_revenue DESC
""").show(truncate=False)
```

> **💡 Note:** After registering, your gold tables are queryable from the **Databricks SQL** tab (left sidebar) and from tools like Power BI via Partner Connect — without any code. Analysts who don't know PySpark can write plain SQL queries against your gold layer. This is the production-ready end state of a medallion architecture.

---

### STEP 14 — Notebook: Delta Lake Advanced Features

> **Depends on:** STEP 11 (Silver trips Delta table must exist)

**What to do:**
1. Create notebook: `05_Delta_Advanced` | Language: Python

**Cell 1 — View the history of your Silver table:**
```python
from delta.tables import DeltaTable

# Load the Delta table object (gives you access to Delta-specific operations)
dt_trips = DeltaTable.forPath(spark, "/mnt/silver/trips/")

# View every write operation that has happened to this table
print("=== SILVER TRIPS — FULL HISTORY ===")
dt_trips.history().select(
    "version", "timestamp", "operation", "operationMetrics"
).show(10, truncate=False)
```

**Cell 2 — Time Travel: Query an older version:**
```python
# Query the table as it looked at version 0 (first write)
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/mnt/silver/trips/")

# Query the current version
df_current = spark.read.format("delta").load("/mnt/silver/trips/")

print(f"Version 0 row count  : {df_v0.count():,}")
print(f"Current row count    : {df_current.count():,}")
```

**Cell 3 — Simulate a bad data load and restore:**
```python
# Simulate a pipeline bug that writes garbage data
df_garbage = spark.createDataFrame(
    [(-999, None, None, -1, -1.0, None, None, -99.99, -1.0, -1.0, 0, "yellow",
      -1.0, -1, -1, -1, -1.0, None)],
    schema=df_current.schema
)

# Append the garbage rows to Silver
df_garbage.write.format("delta").mode("append").save("/mnt/silver/trips/")
print(f"After bad write: {spark.read.format('delta').load('/mnt/silver/trips/').count():,} rows")

# Restore back to the last good version (undo the bad write)
bad_version = dt_trips.history().select("version").first()[0]
good_version = bad_version - 1

dt_trips.restoreToVersion(good_version)
print(f"Restored to version {good_version}")
print(f"After restore: {spark.read.format('delta').load('/mnt/silver/trips/').count():,} rows")
```

**Cell 4 — Shallow Clone for a Dev Environment:**
```python
# Create a dev copy of the silver table that shares the same files
# (no data is duplicated — it's just a pointer)
spark.sql("""
    CREATE TABLE IF NOT EXISTS nyc_trip_analytics.trips_dev_clone
    SHALLOW CLONE delta.`/mnt/silver/trips/`
""")
print("✓ Dev clone created")
print("  You can now modify trips_dev_clone without affecting the real silver table")
```

**Cell 5 — Schema Evolution: Add a new column:**
```python
# Add a new business metric that was requested after the pipeline was built
df_with_new = spark.read.format("delta").load("/mnt/silver/trips/") \
    .withColumn(
        "revenue_per_minute",
        F.round(F.col("fare_amount") / F.col("trip_duration_minutes"), 3)
    )

df_with_new.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/mnt/silver/trips/")

print("✓ Schema evolved — new column 'revenue_per_minute' added")
spark.read.format("delta").load("/mnt/silver/trips/").printSchema()
```

> **💡 Note:** These three Delta features — time travel, shallow clone, and schema evolution — solve three real problems that every data engineering team faces:
> - **Time travel + restore:** A bad data load happened in production. Undo it in seconds.
> - **Shallow clone:** A data scientist wants to experiment on production data. Give them a free copy.
> - **Schema evolution:** A new business metric was requested. Add the column without breaking anything.
> Demonstrating all three in your portfolio shows operational maturity, not just pipeline-building.

---

## Section D — Azure Data Factory Configuration

> You should now have all 5 Databricks notebooks written and individually tested.
> Now configure ADF to connect to both Blob Storage and Databricks.

---

### STEP 15 — Create Blob Storage Linked Service in ADF

> **Depends on:** STEP 8 (ADF exists)

**What is a Linked Service?**
A Linked Service in ADF is a saved connection — like storing a server address and password so you don't have to type it every time. You create one linked service per external system.

**What to do:**
1. Open **ADF Studio** (you should have this tab open from Step 8)
2. Click **"Manage"** in the left sidebar (the toolbox icon at the bottom)
3. Under **"Connections"**, click **"Linked services"**
4. Click **"+ New"**
5. In the search box type **"Blob"** → select **"Azure Blob Storage"** → click **"Continue"**
6. Fill in:
   - **Name:** `LS_BlobStorage_NycTrips`
   - **Authentication method:** Account key
   - **Azure subscription:** Select your subscription
   - **Storage account name:** Select `nyclctripstore`
7. Click **"Test connection"** — must show ✅ **"Connection successful"**
8. Click **"Create"**

> **💡 Note:** If the test connection fails, check that you selected the correct storage account name. Also verify that your ADF is in the same region as your storage account. A "Connection successful" message is required before proceeding — do not skip this test.

---

### STEP 16 — Create Databricks Linked Service in ADF

> **Depends on:** STEP 7 (Databricks cluster exists) + STEP 8 (ADF exists)

**What to do — Get your Databricks access token first:**
1. In your Databricks workspace, click your **profile icon** (top right corner)
2. Click **"Settings"**
3. Click **"Developer"** in the left menu
4. Next to "Access tokens", click **"Manage"**
5. Click **"Generate new token"**
6. Comment: `adf-connection` | Lifetime: `90` days
7. Click **"Generate"** → **copy the token immediately** (you will not see it again)
8. Save this token somewhere safe (e.g. a text file on your desktop)

**Now create the Linked Service:**
1. Back in ADF Studio → **Manage** → **Linked services** → **"+ New"**
2. Search **"Databricks"** → select **"Azure Databricks"** → **"Continue"**
3. Fill in:
   - **Name:** `LS_Databricks_NycTrips`
   - **Azure subscription:** Select your subscription
   - **Databricks workspace:** Select `databricks-nyc-trips`
   - **Select cluster:** Existing interactive cluster
   - **Access token:** Paste the token you just copied
   - **Existing cluster id:** Click the dropdown → select `nyc-trips-cluster`
4. Click **"Test connection"** → must show ✅ **"Connection successful"**
5. Click **"Create"**

> **💡 Note:** The access token is like a password that lets ADF log into Databricks on your behalf and trigger notebooks. The 90-day lifetime means you will need to regenerate and update this linked service if you come back to the project after 90 days. In production, a Service Principal is used instead of a personal token.

---

### STEP 17 — Create Source Datasets in ADF

> **Depends on:** STEP 15 (Blob Storage linked service exists)

**What is a Dataset in ADF?**
A Dataset points to a specific file or folder within a Linked Service. It defines the format (Parquet, CSV, etc.) and the location. Think of it as a typed pointer to your data.

**What to do:**
1. In ADF Studio, click **"Author"** (pencil icon in left sidebar)
2. Click the **"+"** next to Datasets → **"New dataset"**

**Create Dataset 1 — Yellow Taxi Source:**
- Type: **Azure Blob Storage** → **Parquet** → Continue
- Name: `DS_Source_YellowTaxi`
- Linked service: `LS_BlobStorage_NycTrips`
- File path: Click the folder icon → Container: `raw-upload` → Directory: `yellow` → leave filename blank
- Click **"OK"**

**Create Dataset 2 — Green Taxi Source:**
- Same steps, but:
- Name: `DS_Source_GreenTaxi`
- File path: Container: `raw-upload` → Directory: `green`

**Create Dataset 3 — Zone Lookup Source:**
- Type: **Azure Blob Storage** → **DelimitedText** (this is the CSV format) → Continue
- Name: `DS_Source_ZoneLookup`
- Linked service: `LS_BlobStorage_NycTrips`
- File path: Container: `raw-upload` → Directory: `zones` → File: `taxi_zone_lookup.csv`
- First row as header: **Yes**
- Click **"OK"**

> **💡 Note:** You are creating one dataset per source type. The wildcard behaviour (reading all files in the `yellow/` folder) is built into the folder-level dataset — ADF will automatically read all 3 Parquet files in the folder as one combined read.

---

### STEP 18 — Create Bronze Sink Datasets in ADF

> **Depends on:** STEP 15 (Blob Storage linked service exists)

**What to do:**

**Create Dataset 4 — Bronze Yellow Sink:**
- Type: **Azure Blob Storage** → **Parquet** → Continue
- Name: `DS_Sink_Bronze_Yellow`
- Linked service: `LS_BlobStorage_NycTrips`
- File path: Container: `bronze` → Directory: `yellow`
- Click **"OK"**

**Create Dataset 5 — Bronze Green Sink:**
- Name: `DS_Sink_Bronze_Green`
- File path: Container: `bronze` → Directory: `green`

**Create Dataset 6 — Bronze Zones Sink:**
- Type: **Azure Blob Storage** → **DelimitedText** → Continue
- Name: `DS_Sink_Bronze_Zones`
- File path: Container: `bronze` → Directory: `zones`

> **💡 Note:** The bronze sink datasets point to the `bronze` container, not `raw-upload`. The ADF pipeline's job is to copy data from `raw-upload` (source) into `bronze` (destination). Bronze holds exact copies of the source files — no transformations happen in ADF, only movement.

---

### STEP 19 — Build 3 Ingestion Pipelines in ADF

> **Depends on:** STEP 17 (source datasets) + STEP 18 (sink datasets)

**What to do:**

**Pipeline 1 — Yellow Taxi Ingestion:**
1. Click **"+"** next to Pipelines → **"New pipeline"**
2. Name: `PL_Ingest_Yellow_to_Bronze`
3. In the Activities panel (left), expand **"Move & transform"** → drag **"Copy data"** onto the canvas
4. Click the Copy activity → go to the **"Source"** tab:
   - Source dataset: `DS_Source_YellowTaxi`
   - File path type: **Wildcard file path**
   - Wildcard file name: `*.parquet`
5. Go to the **"Sink"** tab:
   - Sink dataset: `DS_Sink_Bronze_Yellow`
6. Click **"Debug"** (top toolbar) → watch the Output panel at the bottom
7. When it shows a green checkmark, go to **Storage browser** → `bronze/yellow/` and confirm files appeared

**Pipeline 2 — Green Taxi Ingestion:**
- Same steps as above but:
- Name: `PL_Ingest_Green_to_Bronze`
- Source: `DS_Source_GreenTaxi` | Sink: `DS_Sink_Bronze_Green`

**Pipeline 3 — Zone Lookup Ingestion:**
- Name: `PL_Ingest_Zones_to_Bronze`
- Source: `DS_Source_ZoneLookup` | Sink: `DS_Sink_Bronze_Zones`

> **💡 Note:** Always use the **"Debug"** button to test a pipeline before wiring it into a larger flow. Debug runs execute immediately using your current (unsaved) configuration — you can catch mistakes before they become part of a scheduled run. Do not move to Step 20 until all 3 pipelines show green checkmarks in Debug mode.

---

### STEP 20 — Test All 3 Ingestion Pipelines

> **Depends on:** STEP 6 (files uploaded to raw-upload) + STEP 19 (pipelines built)

**What to do:**
1. Run each of the 3 pipelines in Debug mode if you haven't already
2. After each run, verify in **Storage browser** that files appeared:
   - `bronze/yellow/` → should contain Parquet files
   - `bronze/green/` → should contain Parquet files
   - `bronze/zones/` → should contain the CSV file
3. In Databricks, run a quick verification:

```python
# Run this in any Databricks notebook cell to confirm bronze files are readable
print("Bronze Yellow:", spark.read.parquet("/mnt/bronze/yellow/").count(), "rows")
print("Bronze Green :", spark.read.parquet("/mnt/bronze/green/").count(), "rows")
```

Expected output:
```
Bronze Yellow: 9,000,000+ rows
Bronze Green :   180,000+ rows
```

> **💡 Note:** If the row counts are 0 or you get a "path not found" error, the most common causes are: (1) the files didn't upload correctly in Step 6 — check Storage browser, (2) the mount paths don't match the container names — re-run notebook `00_Mount_Storage`. Fix these before continuing.

---

## Section E — Logic Apps Alerting

---

### STEP 21 — Create the Logic App

> **Depends on:** STEP 4 (Resource Group exists)

**What to do:**
1. In the Azure Portal, search **"Logic Apps"** → click it
2. Click **"+ Add"**
3. Fill in:
   - **Subscription:** Your subscription
   - **Resource group:** `rg-nyc-trip-analytics`
   - **Logic App name:** `logicapp-trip-alerts`
   - **Region:** `East US`
   - **Plan type:** **Consumption** ← important, this is the pay-per-use tier
4. Click **"Review + Create"** → **"Create"**
5. When deployed, click **"Go to resource"**

---

### STEP 22 — Configure the Email Alert Workflow

> **Depends on:** STEP 21 (Logic App exists)

**What to do:**
1. On your Logic App page, click **"Logic app designer"** in the left menu
2. Click **"Blank Logic App"**

**Set up the trigger (what starts the workflow):**
1. In the search box, type **"HTTP"**
2. Select **"When a HTTP request is received"** (under Triggers)
3. In the **"Request Body JSON Schema"** box, paste this:
```json
{
  "type": "object",
  "properties": {
    "pipelineName":  { "type": "string" },
    "errorMessage":  { "type": "string" },
    "runTime":       { "type": "string" },
    "stepFailed":    { "type": "string" }
  }
}
```

**Add the email action:**
1. Click **"+ New step"**
2. Search **"Send an email"**
3. Choose **"Send an email (V2)"** under Office 365 Outlook — OR — **"Send email"** under Gmail
4. Click **"Sign in"** → sign in with your email account
5. Fill in the email form:
   - **To:** your email address
   - **Subject:** (click inside the box, then click the lightning bolt icon to add dynamic values)
     Type: `⚠️ ADF Pipeline Failed: ` then click **"pipelineName"** from the dynamic content list
   - **Body:**
     ```
     A pipeline has failed. Details:

     Pipeline Name : [click pipelineName]
     Step Failed   : [click stepFailed]
     Error Message : [click errorMessage]
     Time          : [click runTime]

     Go to ADF Monitor to investigate.
     ```

6. Click **"Save"** (top left)

**Copy the HTTP trigger URL:**
1. Click on the **"When a HTTP request is received"** trigger box at the top
2. You will see a field called **"HTTP POST URL"** — it contains a long URL
3. Click **"Copy"** next to it
4. **Paste this URL into a text file and save it** — you need it in Step 24

> **💡 Note:** This URL is the "address" of your Logic App. When ADF sends a request to this URL with pipeline failure details, Logic Apps triggers the email automatically. Keep this URL safe — anyone with this URL can trigger the workflow.

---

## Section F — ADF Master Orchestration Pipeline

> All Databricks notebooks are written and tested. Logic App is ready.
> Now wire everything together into one master pipeline.

---

### STEP 23 — Build the Master Orchestration Pipeline

> **Depends on:** STEP 13 (all notebooks done) + STEP 16 (Databricks linked service exists)

**What this pipeline does:**
When triggered, it runs your Databricks notebooks in the correct sequence:
Mount Storage → Bronze to Silver → Silver to Gold → Register Tables

**What to do:**
1. In ADF Studio → **Author** → click **"+"** next to Pipelines → **"New pipeline"**
2. Name: `PL_Master_NycTrips`
3. From the Activities panel, expand **"Databricks"** → drag **"Notebook"** onto the canvas 4 times
4. Rename and configure each one by clicking it and going to the **"Azure Databricks"** tab:

**Activity 1:**
- Name: `ACT_Mount_Storage`
- Linked service: `LS_Databricks_NycTrips`
- Notebook path: Click **"Browse"** → navigate to and select `00_Mount_Storage`

**Activity 2:**
- Name: `ACT_Bronze_to_Silver`
- Linked service: `LS_Databricks_NycTrips`
- Notebook path: Browse → select `02_Bronze_to_Silver`

**Activity 3:**
- Name: `ACT_Silver_to_Gold`
- Linked service: `LS_Databricks_NycTrips`
- Notebook path: Browse → select `03_Silver_to_Gold`

**Activity 4:**
- Name: `ACT_Register_Tables`
- Linked service: `LS_Databricks_NycTrips`
- Notebook path: Browse → select `04_Register_Tables`

**Connect them in sequence:**
- Hover over `ACT_Mount_Storage` → a small arrow appears on the right edge → drag it to `ACT_Bronze_to_Silver`
- Drag from `ACT_Bronze_to_Silver` to `ACT_Silver_to_Gold`
- Drag from `ACT_Silver_to_Gold` to `ACT_Register_Tables`

These arrows are **green success arrows** — each activity only starts if the previous one succeeded.

> **💡 Note:** The order matters. `ACT_Bronze_to_Silver` must complete before `ACT_Silver_to_Gold` starts — otherwise Silver doesn't have data yet. The green arrow dependency enforces this. Do NOT connect them all in parallel.

---

### STEP 24 — Add Failure Alert to the Master Pipeline

> **Depends on:** STEP 22 (Logic App URL is saved) + STEP 23 (master pipeline exists)

**What to do:**
1. In your `PL_Master_NycTrips` pipeline canvas, from the Activities panel drag a **"Web"** activity onto the canvas
2. Name it: `ACT_Send_Failure_Alert`
3. Now connect failure arrows:
   - Hover over `ACT_Bronze_to_Silver` → drag the **red X arrow** to `ACT_Send_Failure_Alert`
   - Hover over `ACT_Silver_to_Gold` → drag the **red X arrow** to `ACT_Send_Failure_Alert`
   - Hover over `ACT_Register_Tables` → drag the **red X arrow** to `ACT_Send_Failure_Alert`

> To get the red arrow: when you hover over an activity, a small arrow appears. Click it and you'll see options for "Success", "Failure", "Completion", "Skipped". Choose **Failure**.

4. Click `ACT_Send_Failure_Alert` → go to the **"Settings"** tab:
   - **URL:** Paste the Logic App HTTP POST URL you saved in Step 22
   - **Method:** POST
   - **Headers:** Click **"+ New"** → Name: `Content-Type` → Value: `application/json`
   - **Body:**
```json
{
  "pipelineName": "@{pipeline().Pipeline}",
  "stepFailed": "@{pipeline().Pipeline}",
  "errorMessage": "A step in the pipeline failed. Check ADF Monitor for details.",
  "runTime": "@{utcNow()}"
}
```

> **💡 Note:** Red arrows mean "run this activity only when the connected activity FAILS". This is the correct pattern — the alert only fires on failure, not on success. If you accidentally used a green arrow, the alert would fire on every successful run, which is noise.

---

### STEP 25 — Add a Schedule Trigger

> **Depends on:** STEP 24 (pipeline is fully wired)

**What to do:**
1. In the `PL_Master_NycTrips` pipeline, click **"Add trigger"** in the top toolbar
2. Click **"New/Edit"**
3. Click **"+ New"** in the "Choose trigger" dropdown
4. Fill in:
   - **Name:** `TR_Daily_NycTrips`
   - **Type:** Schedule
   - **Start date:** Today's date
   - **Time zone:** Your time zone
   - **Recurrence:** Every `1` Day at `02:00 AM`
5. Click **"OK"** → **"OK"**

> **💡 Note:** 2:00 AM is chosen so the pipeline runs at night when you are not working and the cluster can terminate afterwards without interrupting you. For a project using static historical data, you can skip this trigger and just run manually — but setting it shows orchestration knowledge in your portfolio.

---

### STEP 26 — Publish Everything in ADF

> **Depends on:** STEP 25

**What to do:**
1. In ADF Studio, click the **"Publish all"** button at the top of the page (blue button)
2. A panel slides in showing all unpublished changes
3. Click **"Publish"**
4. Wait for the success message: **"Publishing completed"**

> **💡 Note:** In ADF, everything you create (linked services, datasets, pipelines, triggers) exists only as a draft until you publish. Publishing saves your work permanently and activates any schedule triggers. If you close ADF Studio without publishing, your unpublished changes are still saved as drafts — but triggers won't activate and the pipeline won't be accessible from outside ADF until you publish.

---

## Section G — Final Validation

---

### STEP 27 — Run the Full Pipeline End-to-End and Validate

> **Depends on:** STEP 26 (everything published)

**What to do:**

**Part A — Trigger the pipeline manually:**
1. In ADF Studio, open `PL_Master_NycTrips`
2. Click **"Debug"** (top toolbar) — this runs immediately without waiting for the schedule
3. Watch the **Output** panel at the bottom of the screen
4. Each activity shows a spinning icon while running and a green checkmark when done
5. The full run takes approximately 10–20 minutes (Databricks processes 9M rows)

**Part B — Verify storage output:**
1. Go to Azure Portal → your storage account → **"Storage browser"**
2. Check each container:

| What to Check | What You Should See |
|---|---|
| `bronze/yellow/` | Parquet files |
| `bronze/green/` | Parquet files |
| `bronze/zones/` | CSV file |
| `silver/trips/` | A `_delta_log/` folder AND Parquet files |
| `silver/zones/` | A `_delta_log/` folder AND Parquet files |
| `gold/borough_revenue/` | A `_delta_log/` folder AND Parquet files |
| `gold/peak_hours/` | A `_delta_log/` folder AND Parquet files |
| `gold/top_zones/` | A `_delta_log/` folder AND Parquet files |
| `gold/payment_behaviour/` | A `_delta_log/` folder AND Parquet files |

> The `_delta_log/` folder is the proof that Delta format is working — it is Delta's transaction log.

**Part C — Run validation notebook in Databricks:**
1. Create a new notebook: `06_Final_Validation` | Language: Python

```python
print("=" * 55)
print("  NYC TLC MEDALLION ARCHITECTURE — VALIDATION REPORT")
print("=" * 55)
print()

# ── BRONZE LAYER ───────────────────────────────────────────────
bronze_yellow = spark.read.parquet("/mnt/bronze/yellow/").count()
bronze_green  = spark.read.parquet("/mnt/bronze/green/").count()
bronze_total  = bronze_yellow + bronze_green

print("BRONZE LAYER (raw source copies)")
print(f"  Yellow Taxi rows : {bronze_yellow:>12,}")
print(f"  Green Taxi rows  : {bronze_green:>12,}")
print(f"  Total            : {bronze_total:>12,}")
print()

# ── SILVER LAYER ───────────────────────────────────────────────
silver_trips = spark.read.format("delta").load("/mnt/silver/trips/").count()
silver_zones = spark.read.format("delta").load("/mnt/silver/zones/").count()

print("SILVER LAYER (cleaned + unified Delta tables)")
print(f"  Unified trips    : {silver_trips:>12,}  ← should be ≤ bronze total")
print(f"  Zone lookup      : {silver_zones:>12,}")
removed = bronze_total - silver_trips
print(f"  Rows removed     : {removed:>12,}  (nulls, duplicates, bad values)")
print()

# ── GOLD LAYER ─────────────────────────────────────────────────
print("GOLD LAYER (aggregated analytics tables)")
gold_tables = ["borough_revenue", "peak_hours", "top_zones", "payment_behaviour"]
for t in gold_tables:
    count = spark.sql(f"SELECT COUNT(*) FROM nyc_trip_analytics.{t}").collect()[0][0]
    print(f"  {t:<25} : {count:>8,} rows")

print()
print("=" * 55)
print("  VALIDATION COMPLETE — Your pipeline is working!")
print("=" * 55)
```

**Expected output:**
```
=======================================================
  NYC TLC MEDALLION ARCHITECTURE — VALIDATION REPORT
=======================================================

BRONZE LAYER (raw source copies)
  Yellow Taxi rows :    9,100,000+
  Green Taxi rows  :      180,000+
  Total            :    9,280,000+

SILVER LAYER (cleaned + unified Delta tables)
  Unified trips    :    8,950,000+  ← should be ≤ bronze total
  Zone lookup      :          265
  Rows removed     :      330,000+  (nulls, duplicates, bad values)

GOLD LAYER (aggregated analytics tables)
  borough_revenue       :       10 rows
  peak_hours            :      336 rows
  top_zones             :       50 rows
  payment_behaviour     :       60 rows

=======================================================
  VALIDATION COMPLETE — Your pipeline is working!
=======================================================
```

> **💡 Note:** If Silver rows are GREATER than Bronze rows, you have a bug — likely the silver notebook ran twice and appended instead of overwriting. If any Gold table shows 0 rows, the Silver to Gold notebook failed silently — re-run Step 12 manually in Databricks and check for error messages.

---

### STEP 28 — Test the Failure Alert

> **Depends on:** STEP 22 (Logic App configured) + STEP 24 (failure wired in ADF)

**What to do:**
1. In ADF Studio, open `PL_Master_NycTrips`
2. Click on `ACT_Bronze_to_Silver` → go to the **"Azure Databricks"** tab → **"Notebook path"**
3. Change the notebook path to something that doesn't exist, e.g. `/Users/test/does_not_exist`
4. Click **"Debug"** — the pipeline will run and fail on that activity
5. Check your email inbox — you should receive a failure alert within 1–2 minutes
6. Confirm the email contains the pipeline name and a timestamp
7. Fix the notebook path back to the correct path (`02_Bronze_to_Silver`)
8. Click **"Publish all"**

> **💡 Note:** Never skip this test. An untested alert is useless — you only find out it's broken when a real failure happens and you don't get notified. Confirming the email arrives with correct information takes 5 minutes and completes the project properly.

---

## Section H — Portfolio Wrap-Up

> No Azure work needed for this section. This is documentation for your job search.

---

### STEP 29 — Portfolio Materials

**Resume Bullet Points (copy and adapt these):**

```
• Built a multi-source trip analytics platform on Azure ingesting NYC TLC Yellow and
  Green Taxi data (~9.2M rows) using Azure Data Factory and Azure Databricks

• Implemented medallion architecture (Bronze → Silver → Gold) with Delta Lake;
  applied schema harmonization across heterogeneous Parquet sources in PySpark

• Engineered 4 gold-layer analytical tables (borough revenue, peak hours, top zones,
  payment behaviour) by joining trip data with geospatial zone lookup dimensions

• Configured automated failure alerting using Azure Logic Apps, sending contextual
  email notifications including pipeline name, step, and timestamp on any failure

• Demonstrated Delta Lake advanced features: time travel for production data recovery,
  shallow clone for dev environment provisioning, schema evolution for new metrics
```

**GitHub README — What to Include:**

1. Architecture diagram (screenshot your ADF pipeline canvas and Databricks notebooks)
2. Dataset source: NYC TLC (link to nyc.gov) — state row count and date range
3. Tech stack badges
4. The 4 business questions your gold layer answers (with a sample output screenshot)
5. Folder structure of your notebooks
6. How to run (steps 1–3 of this guide)

**30-Second Interview Story:**

> "I built a trip analytics platform using medallion architecture on Azure. I ingested NYC taxi data — about 9 million rows across Yellow and Green cab types — through Azure Data Factory pipelines into a raw bronze zone in Blob Storage. The interesting challenge was schema harmonization: Yellow and Green taxis use different column names for the same timestamps, so in Databricks I unified both sources into one clean Silver Delta table. From Silver, I built four Gold tables that answer specific business questions like peak demand by borough and payment behaviour by neighbourhood. I also wired in Logic Apps to email me on pipeline failures. The Delta Lake features I'm most proud of demonstrating are time travel — where I simulated a bad data load and restored to a previous version in seconds — and shallow cloning for safe dev environment provisioning."

---

## Quick Reference — All Resources Created

| Resource | Name | Type |
|---|---|---|
| Resource Group | `rg-nyc-trip-analytics` | Container for all resources |
| Storage Account | `nyclctripstore` | Azure Blob Storage |
| Container 1 | `raw-upload` | Source landing zone |
| Container 2 | `bronze` | Raw copies layer |
| Container 3 | `silver` | Cleaned Delta layer |
| Container 4 | `gold` | Aggregated Delta layer |
| Databricks Workspace | `databricks-nyc-trips` | Spark compute |
| Databricks Cluster | `nyc-trips-cluster` | Single-node, auto-terminate 20 min |
| Data Factory | `adf-nyc-trip-analytics` | Orchestration engine |
| Logic App | `logicapp-trip-alerts` | Email alerting |

## Quick Reference — All Notebooks Created

| Notebook | Purpose | Run Order |
|---|---|---|
| `00_Mount_Storage` | Connect Databricks to Blob Storage | 1st — run once |
| `01_Explore_Bronze` | Understand raw data before transforming | 2nd |
| `02_Bronze_to_Silver` | Clean + unify + write Silver Delta | 3rd |
| `03_Silver_to_Gold` | Aggregate + write Gold Delta | 4th |
| `04_Register_Tables` | Make Gold tables queryable via SQL | 5th |
| `05_Delta_Advanced` | Time travel, clone, schema evolution | After step 4 |
| `06_Final_Validation` | Verify row counts across all layers | Last |

---

*Dataset links validated live from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page on June 3, 2026*
*Built with: Azure Data Factory · Azure Databricks · Delta Lake · Azure Blob Storage · PySpark · Logic Apps*
