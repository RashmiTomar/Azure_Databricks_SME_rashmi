# ADF → Databricks Notebook Orchestration (Parameters)

This document explains how Azure Data Factory (ADF) triggers a Databricks notebook
and passes runtime parameters using **Base parameters**, which are read inside the
notebook using **Databricks widgets**.

## Why widgets?
ADF sends parameters as key-value pairs. Databricks notebooks receive these values
through `dbutils.widgets`.

Flow:
ADF Pipeline Params / Variables → Databricks Notebook Activity (Base parameters) → Notebook Widgets → Notebook Logic

---

## ADF Databricks Notebook Activity (Settings)

### Notebook path
Example:
- `/Workspace/Shared/adf/azure-metadata-driven-ingestion/gold_upsert_merge`

### Base parameters (example mapping)

| ADF Base Parameter Name | Example Value (ADF dynamic content) | Notebook Widget |
|---|---|---|
| storage_account | `@pipeline().parameters.p_storage_account` | `storage_account` |
| vendor | `@pipeline().parameters.p_vendor` | `vendor` |
| subject | `@pipeline().parameters.p_subject` | `subject` |
| file_name | `@pipeline().parameters.p_file_name` | `file_name` |
| merge_keys | `@pipeline().parameters.p_merge_keys` | `merge_keys` |
| raw_container | `raw` | `raw_container` |
| gold_container | `gold` | `gold_container` |
| run_id | `@pipeline().RunId` | `run_id` |
| run_date | `@formatDateTime(utcNow(),'yyyy-MM-dd')` | `run_date` |

> Note: Base parameter **Name** must exactly match the widget name (case-sensitive).

---

## Databricks Notebook (widgets example)

```python
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("vendor", "")
dbutils.widgets.text("subject", "")
dbutils.widgets.text("file_name", "")
dbutils.widgets.text("merge_keys", "order_id")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("run_date", "")

storage_account = dbutils.widgets.get("storage_account")
vendor = dbutils.widgets.get("vendor")
subject = dbutils.widgets.get("subject")
file_name = dbutils.widgets.get("file_name")
merge_keys = dbutils.widgets.get("merge_keys")
run_id = dbutils.widgets.get("run_id")
run_date = dbutils.widgets.get("run_date")
