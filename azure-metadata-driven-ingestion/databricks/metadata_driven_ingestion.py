

# =========================================
# Metadata-Driven Ingestion Framework
# Author: Rashmi Tomar
# Description:
# Generic Databricks ingestion using metadata
# =========================================

from pyspark.sql import functions as F

# -----------------------------
# 1. Sample metadata (normally from SQL / Delta)
# -----------------------------
metadata = {
    "source_format": "csv",
    "source_path": "abfss://raw@<storage>.dfs.core.windows.net/sales/",
    "target_path": "abfss://bronze@<storage>.dfs.core.windows.net/sales/",
    "header": "true",
    "delimiter": ",",
    "load_type": "full"
}

# -----------------------------
# 2. Generic read logic
# -----------------------------
def read_source(meta: dict):
    reader = spark.read.format(meta["source_format"])
    
    if meta["source_format"] == "csv":
        reader = (
            reader.option("header", meta.get("header", "true"))
                  .option("delimiter", meta.get("delimiter", ","))
        )
    
    return reader.load(meta["source_path"])

# -----------------------------
# 3. Read data using metadata
# -----------------------------

try:
    df = read_source(metadata)

    df_audit = (
        df.withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_source_system", F.lit("sales_system"))
    )

    (
        df_audit.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(metadata["target_path"])
    )

    print("✅ Metadata-driven ingestion completed successfully.")

except Exception as e:
    print("❌ Ingestion failed")
    raise e

