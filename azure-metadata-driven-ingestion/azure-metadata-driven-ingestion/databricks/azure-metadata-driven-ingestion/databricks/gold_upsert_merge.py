# =========================================
# Gold Layer UPSERT (Delta MERGE)
# Author: Rashmi Tomar
# Purpose:
#   Read a CSV from ADLS (raw/bronze) and MERGE into a Gold Delta table
#   using primary key(s) passed from ADF as parameters (widgets).
# =========================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# -----------------------------
# 1) Widgets (ADF -> Databricks parameters)
# -----------------------------
dbutils.widgets.text("storage_account", "")   # e.g. mystorageacct
dbutils.widgets.text("vendor", "acme")
dbutils.widgets.text("subject", "sales")
dbutils.widgets.text("file_name", "")         # e.g. sales_2026-01-25.csv
dbutils.widgets.text("merge_keys", "order_id")# e.g. order_id OR order_id,line_id
dbutils.widgets.text("raw_container", "raw")
dbutils.widgets.text("gold_container", "gold")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("run_date", "")

storage_account = dbutils.widgets.get("storage_account").strip()
vendor         = dbutils.widgets.get("vendor").strip()
subject        = dbutils.widgets.get("subject").strip()
file_name      = dbutils.widgets.get("file_name").strip()
merge_keys     = dbutils.widgets.get("merge_keys").strip()
raw_container  = dbutils.widgets.get("raw_container").strip()
gold_container = dbutils.widgets.get("gold_container").strip()
run_id         = dbutils.widgets.get("run_id").strip()
run_date       = dbutils.widgets.get("run_date").strip()

if not storage_account or not file_name or not merge_keys:
    raise ValueError(
        f"Missing required params. storage_account={storage_account}, "
        f"file_name={file_name}, merge_keys={merge_keys}"
    )

keys = [k.strip() for k in merge_keys.split(",") if k.strip()]
if len(keys) == 0:
    raise ValueError("merge_keys parsed empty. Provide e.g. 'order_id' or 'order_id,line_id'.")

# -----------------------------
# 2) Build ADLS paths
# Adjust folder patterns to match your org standards
# -----------------------------
raw_path = (
    f"abfss://{raw_container}@{storage_account}.dfs.core.windows.net/"
    f"vendor/{vendor}/{subject}/{file_name}"
)

gold_path = (
    f"abfss://{gold_container}@{storage_account}.dfs.core.windows.net/"
    f"{subject}/{vendor}/"
)

print("RAW PATH :", raw_path)
print("GOLD PATH:", gold_path)
print("MERGE KEYS:", keys)

# -----------------------------
# 3) Read Source CSV
# -----------------------------
src = (spark.read
       .option("header", "true")
       .option("inferSchema", "true")
       .csv(raw_path))

if src.rdd.isEmpty():
    raise ValueError(f"No data found at: {raw_path}")

# Validate keys exist
missing = [k for k in keys if k not in src.columns]
if missing:
    raise ValueError(f"Merge key(s) missing in source: {missing}. Columns: {src.columns}")

# -----------------------------
# 4) Add audit columns
# -----------------------------
src_enriched = (
    src.withColumn("_ingest_run_id", F.lit(run_id))
       .withColumn("_ingest_run_date", F.lit(run_date))
       .withColumn("_ingest_ts_utc", F.current_timestamp())
)

# -----------------------------
# 5) Create Gold table if not exists
# -----------------------------
if not DeltaTable.isDeltaTable(spark, gold_path):
    print("Gold Delta table does not exist yet. Creating initial Delta table...")
    (src_enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path)
    )

# -----------------------------
# 6) Build MERGE condition
# Example: t.order_id = s.order_id AND t.line_id = s.line_id
# -----------------------------
cond = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])

# -----------------------------
# 7) MERGE (UPSERT)
# Matched -> update all columns
# Not matched -> insert all columns
# -----------------------------
try:
    delta_t = DeltaTable.forPath(spark, gold_path)

    (delta_t.alias("t")
        .merge(src_enriched.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("✅ Gold MERGE completed successfully.")

except Exception as e:
    print("❌ Gold MERGE failed.")
    raise e
