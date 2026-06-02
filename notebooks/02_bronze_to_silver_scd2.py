# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze -> Silver (SCD Type 2)

# COMMAND ----------

CONFIG = {
 "resources": ["Patient", "Encounter", "Observation", "Condition"],
 "catalog": "hive_metastore",
 "schema": "fhir_assignment",
}

try:
 spark # type: ignore[name-defined]
except NameError:
 spark = None

if spark is None:
 print("This notebook is intended to run inside Databricks runtime.")
else:
 for resource in CONFIG["resources"]:
 bronze_tbl = f"{CONFIG['catalog']}.{CONFIG['schema']}.bronze_{resource.lower()}"
 silver_tbl = f"{CONFIG['catalog']}.{CONFIG['schema']}.silver_{resource.lower()}"
 spark.sql(
 f"""
 CREATE OR REPLACE TABLE {silver_tbl}
 USING DELTA
 AS
 WITH ranked AS (
 SELECT
 *,
 sha2(resource_json, 256) AS resource_hash,
 row_number() OVER (PARTITION BY id ORDER BY saved_at DESC) AS row_rank
 FROM {bronze_tbl}
 )
 SELECT
 id,
 resourceType,
 resource_json,
 extraction_timestamp,
 api_url_or_params,
 called_at,
 saved_at,
 resource_hash,
 CASE WHEN row_rank = 1 THEN TRUE ELSE FALSE END AS is_current,
 saved_at AS valid_from,
 CASE WHEN row_rank = 1 THEN NULL ELSE saved_at END AS valid_to
 FROM ranked
 """
 )

 print("Silver SCD2 build complete")