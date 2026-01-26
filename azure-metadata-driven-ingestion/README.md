# Azure Metadata-Driven Ingestion (ADF + Databricks + Delta Lake)

This repo demonstrates a simple metadata-driven ingestion pattern:
- ADF orchestrates the run and passes parameters
- Databricks reads metadata/config and ingests data into Delta tables
- Bronze/Silver/Gold layers follow the Medallion architecture

## Architecture (high level)
1. ADF pipeline triggers a Databricks notebook
2. ADF passes runtime values (paths, file name, run id, load type) as Base parameters
3. Databricks notebook reads a CSV from ADLS (Raw/Bronze)
4. Databricks writes Delta to Bronze/Silver and performs UPSERT (MERGE) into Gold

## Folder Structure
azure-metadata-driven-ingestion/
databricks/
metadata_driven_ingestion.py
gold_upsert_merge.py
docs/
adf-parameters.md

markdown
Copy code

## Key Features
- Metadata-driven config (format, source path, target path)
- Audit columns for traceability
- Delta Lake write patterns
- Gold UPSERT using MERGE

## Tech Stack
- Azure Data Factory (orchestration)
- Azure Databricks (PySpark)
- ADLS Gen2
- Delta Lake
