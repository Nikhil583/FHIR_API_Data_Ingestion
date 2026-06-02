# FHIR_API_Data_Ingestion

# Databricks Pipeline and Table Relationships

This document explains the **execution pipeline**, **data flow across Medallion layers**, and **table relationships** for the Databricks implementation.

---

## 1) Pipeline orchestration flow

Primary job definition: `databricks/jobs/fhir_medallion_job.yaml` 
(JSON version also exists at `databricks/jobs/fhir_medallion_job.json`)

Task order:

1. `ingest_raw_bronze`
2. `bronze_to_silver` (depends on step 1)
3. `silver_to_gold` (depends on step 2)

Execution DAG:

`ingest_raw_bronze -> bronze_to_silver -> silver_to_gold`

---

## 2) Notebook responsibility by stage

### Task 1: `ingest_raw_bronze`
Notebook: `databricks/notebooks/01_ingest_raw_to_bronze.py`

What it does:
- Calls FHIR API incrementally for 3 days with pagination.
- Resources processed:
 - `Patient`
 - `Encounter`
 - `Observation`
 - `Condition`
- Saves raw payloads to:
 - `dbfs:/FileStore/fhir/raw/<Resource>/<YYYY-MM-DD>/page_<n>_<timestamp>.json`
- Writes Bronze Delta tables:
 - `hive_metastore.fhir_assignment.bronze_patient`
 - `hive_metastore.fhir_assignment.bronze_encounter`
 - `hive_metastore.fhir_assignment.bronze_observation`
 - `hive_metastore.fhir_assignment.bronze_condition`

Added metadata columns in Bronze:
- `extraction_timestamp`
- `api_url_or_params`
- `called_at`
- `saved_at`

---

### Task 2: `bronze_to_silver`
Notebook: `databricks/notebooks/02_bronze_to_silver_scd2.py`

What it does:
- Reads each Bronze table.
- Computes `resource_hash = sha2(resource_json, 256)`.
- Applies latest-row logic per `id` using `row_number() over (partition by id order by saved_at desc)`.
- Writes Silver Delta tables:
 - `hive_metastore.fhir_assignment.silver_patient`
 - `hive_metastore.fhir_assignment.silver_encounter`
 - `hive_metastore.fhir_assignment.silver_observation`
 - `hive_metastore.fhir_assignment.silver_condition`

Silver SCD2-style columns:
- `resource_hash`
- `is_current`
- `valid_from`
- `valid_to`

---

### Task 3: `silver_to_gold`
Notebook: `databricks/notebooks/03_silver_to_gold.py`

What it does:
- Reads all Silver tables.
- Computes current-row counts (`is_current = TRUE`) per resource.
- Writes Gold Delta table:
 - `hive_metastore.fhir_assignment.gold_resource_current_counts`
- Creates Gold view:
 - `hive_metastore.fhir_assignment.vw_gold_resource_current_counts`

---

## 3) Medallion table lineage

Per resource lineage:

`Raw JSON (DBFS)` -> `bronze_<resource>` -> `silver_<resource>` -> `gold_resource_current_counts`

Concrete examples:

- `dbfs:/FileStore/fhir/raw/Patient/...` -> `bronze_patient` -> `silver_patient` -> `gold_resource_current_counts`
- `dbfs:/FileStore/fhir/raw/Encounter/...` -> `bronze_encounter` -> `silver_encounter` -> `gold_resource_current_counts`
- `dbfs:/FileStore/fhir/raw/Observation/...` -> `bronze_observation` -> `silver_observation` -> `gold_resource_current_counts`
- `dbfs:/FileStore/fhir/raw/Condition/...` -> `bronze_condition` -> `silver_condition` -> `gold_resource_current_counts`

---

## 4) Business-level entity relationships (FHIR)

These are logical FHIR relationships available inside `resource_json`:

- `Encounter.subject.reference` -> `Patient`
- `Observation.subject.reference` -> `Patient`
- `Observation.encounter.reference` -> `Encounter`
- `Condition.subject.reference` -> `Patient`
- `Condition.encounter.reference` -> `Encounter`

These references can be normalized in future Gold dimensional/fact models.

---

## 5) Key table schemas

### Bronze (`bronze_<resource>`)
- `id`
- `resourceType`
- `resource_json`
- `extraction_timestamp`
- `api_url_or_params`
- `called_at`
- `saved_at`

### Silver (`silver_<resource>`)
- Bronze columns +
- `resource_hash`
- `is_current`
- `valid_from`
- `valid_to`

### Gold (`gold_resource_current_counts`)
- `resource`
- `current_count`

---

## 6) Dependency and quality checkpoints

- `bronze_to_silver` runs only after successful ingestion.
- `silver_to_gold` runs only after successful Silver creation.
- `max_concurrent_runs: 1` in JSON job prevents overlapping runs.
- YAML bundle config uses queued execution (`queue.enabled: true`).