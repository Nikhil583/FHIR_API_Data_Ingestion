# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver -> Gold Analytics

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
 union_sql = []
 for resource in CONFIG["resources"]:
 silver_tbl = f"{CONFIG['catalog']}.{CONFIG['schema']}.silver_{resource.lower()}"
 union_sql.append(
 f"SELECT '{resource}' AS resource, COUNT(*) AS current_count FROM {silver_tbl} WHERE is_current = TRUE"
 )

 spark.sql(
 f"""
 CREATE OR REPLACE TABLE {CONFIG['catalog']}.{CONFIG['schema']}.gold_resource_current_counts
 USING DELTA
 AS
 {' UNION ALL '.join(union_sql)}
 """
 )

 spark.sql(
 f"""
 CREATE OR REPLACE VIEW {CONFIG['catalog']}.{CONFIG['schema']}.vw_gold_resource_current_counts AS
 SELECT *
 FROM {CONFIG['catalog']}.{CONFIG['schema']}.gold_resource_current_counts
 """
 )

 print("Gold layer complete")