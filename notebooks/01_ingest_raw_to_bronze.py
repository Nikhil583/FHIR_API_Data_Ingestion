# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingest Raw FHIR API -> Bronze Delta

# COMMAND ----------

import json
from datetime import datetime, timezone

from databricks.src.fhir_client import fetch_incremental_pages, get_target_days

CONFIG = {
 "base_url": "https://hapi.fhir.org/baseR4",
 "resources": ["Patient", "Encounter", "Observation", "Condition"],
 "days": 3,
 "page_size": 100,
 "raw_base_path": "dbfs:/FileStore/fhir/raw",
 "catalog": "hive_metastore",
 "schema": "fhir_assignment",
}

def utc_now() -> str:
 return datetime.now(timezone.utc).isoformat()

try:
 spark # type: ignore[name-defined]
except NameError:
 spark = None

try:
 dbutils # type: ignore[name-defined]
except NameError:
 dbutils = None

if spark is None or dbutils is None:
 print("This notebook is intended to run inside Databricks runtime.")
else:
 spark.sql(f"CREATE DATABASE IF NOT EXISTS {CONFIG['catalog']}.{CONFIG['schema']}")

 for resource in CONFIG["resources"]:
 for day in get_target_days(CONFIG["days"]):
 day_key = day.isoformat()
 page_no = 0
 for page in fetch_incremental_pages(
 base_url=CONFIG["base_url"],
 resource=resource,
 day=day,
 page_size=CONFIG["page_size"],
 ):
 page_no += 1
 saved_at = utc_now()
 raw_path = f"{CONFIG['raw_base_path']}/{resource}/{day_key}/page_{page_no}_{saved_at.replace(':', '-')}.json"
 dbutils.fs.put(raw_path, json.dumps(page.bundle), overwrite=True)

 rows = []
 for e in page.bundle.get("entry", []):
 r = e.get("resource", {})
 if not r:
 continue
 rows.append(
 {
 "id": r.get("id"),
 "resourceType": r.get("resourceType"),
 "resource_json": json.dumps(r),
 "extraction_timestamp": utc_now(),
 "api_url_or_params": page.request_url,
 "called_at": page.called_at,
 "saved_at": saved_at,
 }
 )

 if rows:
 df = spark.createDataFrame(rows)
 (
 df.write.mode("append")
 .format("delta")
 .saveAsTable(f"{CONFIG['catalog']}.{CONFIG['schema']}.bronze_{resource.lower()}")
 )

 print("Raw + Bronze ingestion complete")