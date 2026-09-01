# Migration Verification & Troubleshooting Guide

### DAG 1 (MapR/HDFS → S3) and DAG 2 (Hive → Apache Iceberg)

This document describes how to **manually verify** and **troubleshoot** the outcomes of the two migration DAGs by:

1. Querying their **Iceberg tracking tables** (run this SQL in **Superset → SQL Lab**), and
2. Inspecting the **actual migrated databases/tables on S3** (run this SQL in **Spark SQL via the JupyterHub terminal**).

**Tracking tables:** Each DAG writes its progress and results to a small set of **Iceberg tables** in a dedicated _tracking database_ (see §0.3). They record, per run and per table, what was discovered, copied, created, and validated — row/partition/schema checks, timings, and any error messages. They are the audit trail behind the emailed HTML report and the single source of truth for verifying and troubleshooting a run after it finishes.

It is intended to be used alongside the HTML report that each DAG emails at the end of a run. The HTML report gives the summary; the queries below let you drill into **any run, database, or individual table**, and explain what every field and value means.

---

## 0. Where to run each query (IMPORTANT)

There are **three different places** to run queries, depending on _what_ you are querying:

| You are querying…                                                                                                            | Run it in…                                  | Why                                                                                            |
| ---------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| **Tracking tables** (`migration_runs`, `migration_table_status`, `iceberg_migration_runs`, `iceberg_migration_table_status`) | **Superset → SQL Lab**                      | These are audit/tracking tables. Superset SQL Lab is the reporting interface for reading them. |
| **The migrated database/table itself** (`DESCRIBE FORMATTED`, `SELECT COUNT(*)`, `SHOW PARTITIONS`, Iceberg metadata tables) | **Spark SQL, from the JupyterHub terminal** | These run against the live Hive metastore / Iceberg catalog and need a Spark session.          |
| **The source database/table**                                                                                                | Edge node                                   | These run against the source database / table on edge node                                     |

Throughout this document every query block is labelled:

> **▶ Run in: Superset SQL Lab** — for tracking-table queries
> **▶ Run in: Spark SQL (JupyterHub terminal)** — for queries against the migrated data
> **▶ Run in: Edge node** — for queries against the source data

### 0.1 Starting Spark SQL from the JupyterHub terminal

Open a terminal in JupyterHub and start a Spark SQL session:

```bash
# Option A: Spark SQL shell
spark-sql

# Option B: PySpark (then use spark.sql("..."))
pyspark --master local[*]
```

In PySpark, wrap each statement:

```python
spark.sql("DESCRIBE FORMATTED <dest_db>.<table>").show(truncate=False)
spark.sql("SELECT COUNT(*) FROM <dest_db>.<table>").show()
```

### 0.2 Tracking database name

All tracking tables live in one tracking database, referred to here as:

```
migration_tracking_db
```

> **Note:** This name is configurable (env var `MIGRATION_TRACKING_DATABASE` / Airflow Variable `migration_tracking_database`; default `migration_tracking`). Substitute your environment’s actual name if different.

### 0.3 The two DAGs and their tracking tables

Every run appends a **run-level** row (one per DAG run) and one **table-level** row per table processed, so you can audit both the overall run and each individual table.

| DAG                                    | Purpose                                                                                                | Run-level table          | Table-level table                |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------ | ------------------------ | -------------------------------- |
| **DAG 1** – `migration_dag_mapr_to_s3` | Copy Hive tables from MapR-FS/HDFS to S3 (DistCp) and re-create them as **external Hive tables** on S3 | `migration_runs`         | `migration_table_status`         |
| **DAG 2** – `migration_dag_iceberg`    | Convert Hive tables (already on S3) into **Apache Iceberg** tables                                     | `iceberg_migration_runs` | `iceberg_migration_table_status` |

Both table-level tracking tables are **partitioned by `source_database`**.

### 0.4 How to find your Run ID

Each DAG run generates a unique `run_id`:

- DAG 1: `run_YYYYMMDD_HHMMSS_<hash>`
- DAG 2: `iceberg_run_YYYYMMDD_HHMMSS_<hash>`

List recent runs (§1.1 / §3.1) to find it, then use that `run_id` in every table-level query.

---

# PART A — DAG 1: MapR/HDFS → S3 Migration

## 1. Run-level verification — `migration_runs`

> **▶ Run in: Superset SQL Lab**

### 1.1 List recent runs (find your Run ID)

```sql
SELECT *
FROM   migration_tracking_db.migration_runs
ORDER BY started_at DESC
LIMIT 20;
```

**Field meanings and values:**

| Field                         | Meaning                                                               | Possible values / what they mean                                   |
| ----------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `run_id`                      | Unique ID of this DAG run                                             | `run_YYYYMMDD_HHMMSS_<hash>`                                       |
| `dag_run_id`                  | Airflow’s internal run identifier                                     | Airflow-generated string; use it to find the run in the Airflow UI |
| `excel_file_path`             | S3 path of the Excel config that drove this run                       | `s3a://.../config.xlsx` — confirm the _right_ config was used      |
| `started_at` / `completed_at` | Run start/end timestamps (UTC)                                        | `completed_at` is `NULL` while `RUNNING` or if the run crashed     |
| `status`                      | Overall run outcome                                                   | See table below                                                    |
| `total_tables`                | Number of tables the run attempted                                    | Integer                                                            |
| `successful_tables`           | Count that succeeded (incl. not-found/empty, treated as non-failures) | Integer                                                            |
| `failed_tables`               | Count that failed                                                     | Integer > 0 → investigate at table level (§2.7)                    |

**`status` (run-level) values:**

| Value                     | Category    | Meaning                                                        | What to do                                                                             |
| ------------------------- | ----------- | -------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `RUNNING`                 | In progress | Run is executing, **or** crashed before writing a final status | If it’s not actually running in Airflow, treat as failed — inspect table-level rows    |
| `COMPLETED`               | ✅          | All tables migrated & validated successfully                   | Nothing                                                                                |
| `COMPLETED_WITH_MISSING`  | ⚠️          | Succeeded, but some source tables/databases were not found     | Check §2.7 for `TABLE_NOT_FOUND` / `DATABASE_NOT_FOUND`; usually an Excel config issue |
| `COMPLETED_WITH_FAILURES` | ❌          | Some tables failed migration or validation                     | Drill into failures (§2.7)                                                             |
| `FAILED`                  | ❌          | Run failed                                                     | Inspect table-level rows and Airflow task logs                                         |

### 1.2 Full detail for one run

```sql
SELECT * FROM migration_tracking_db.migration_runs WHERE run_id = '<RUN_ID>';
```

---

## 2. Table-level verification — `migration_table_status`

> **▶ Run in: Superset SQL Lab**

This is the primary table for per-table verification. The fields are grouped below by migration phase, each with its meaning, possible values, and troubleshooting notes.

### 2.1 Field reference — Identity

| Field                              | Meaning                                | Values / notes                                                                                                                    |
| ---------------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `run_id`                           | Run this row belongs to                | Match to your Run ID                                                                                                              |
| `source_database` / `source_table` | Source Hive DB/table                   | —                                                                                                                                 |
| `dest_database`                    | Destination database name on S3        | Defaults to source DB name if not overridden in Excel                                                                             |
| `dest_bucket`                      | Destination S3 bucket                  | `s3a://<bucket>`                                                                                                                  |
| `dest_location`                    | Full S3 path of the destination table  | `s3a://<bucket>/<db>/<table>`                                                                                                     |
| `source_location`                  | Original MapR/HDFS path                | Used by DistCp                                                                                                                    |
| `file_format`                      | Detected storage format                | `PARQUET` / `ORC` / `AVRO` / `TEXTFILE` / `UNKNOWN`. `UNKNOWN` → format detection failed; verify with `DESCRIBE FORMATTED` (§5.1) |
| `table_type`                       | Source table type                      | `EXTERNAL` / `MANAGED`                                                                                                            |
| `is_partitioned`                   | Whether the table is partitioned       | `true` / `false`                                                                                                                  |
| `partition_columns`                | Comma-separated partition column names | e.g. `year,month`                                                                                                                 |
| `partition_count`                  | Number of partitions to migrate        | Integer                                                                                                                           |
| `partition_filter`                 | Partition filter from Excel (if any)   | Empty/`NULL` = whole table migrated; otherwise only matching partitions                                                           |
| `filtered_partition_count`         | Partitions selected by the filter      | `NULL` if no filter applied                                                                                                       |

### 2.2 Field reference — Discovery phase

| Field              | Meaning                                     | Values / what to check |
| ------------------ | ------------------------------------------- | ---------------------- |
| `discovery_status` | Did discovery find & read the source table? | `COMPLETED` = found;   |

`TABLE_NOT_FOUND` = table absent on source (check Excel `table` column);

`DATABASE_NOT_FOUND` = DB absent (check Excel `database` column) |
| `discovery_completed_at` | When discovery finished | Timestamp |
| `discovery_duration_seconds` | Discovery time | Double |
| `source_row_count` | Rows in source (filtered count if a filter is applied) | Compare with `dest_hive_row_count` later |
| `source_total_size_bytes` | Source data size | Bytes |
| `source_file_count` | Number of source files | Integer |
| `source_partition_count` | Source partition count used for validation | Integer |
| `unregistered_partitions` | Partitions exist on disk but not in the metastore | `true` → they won’t be recreated automatically; run `MSCK REPAIR TABLE` (Issue 4) |

### 2.3 Field reference — DistCp (data copy) phase

| Field           | Meaning               | Values / what to check |
| --------------- | --------------------- | ---------------------- |
| `distcp_status` | Result of the S3 copy | `COMPLETED` = copied;  |

`FAILED` = copy error (see `error_message`, `yarn_application_id`);

`EMPTY_SOURCE` = nothing to copy;

`SKIPPED` = skipped (e.g. filter matched 0 partitions, or upstream not-found) |
| `distcp_started_at` / `distcp_completed_at` | Copy start/end | Timestamps |
| `distcp_duration_seconds` | Copy duration | Double |
| `distcp_is_incremental` | Was this an incremental (`-update`) copy? | `true` on re-runs where the target already had data |
| `distcp_bytes_copied` / `distcp_files_copied` | Data actually transferred this run | On an incremental re-run with no changes, these can be `0` — that is normal |
| `yarn_application_id` | YARN application ID(s) of the DistCp job(s) | Normally a single ID. When a partition filter is used, this can be a comma-separated list — one DistCp job per partition.\*Run `yarn logs -applicationId <id>` for each ID to debug a failed copy |
| `s3_total_size_bytes_before` / `s3_file_count_before` | S3 state before copy | For incremental reconciliation |
| `s3_total_size_bytes_after` / `s3_file_count_after` | S3 state after copy | Compare with source size/count |
| `file_size_match` | Source size == S3 size after copy | `false` → size discrepancy; check §2.7 and filesystem (§7) |
| `file_count_match` | Source file count == S3 file count | `false` → files missing/extra on S3 |

### 2.4 Field reference — Table-creation phase

| Field                 | Meaning                                    | Values / what to check |
| --------------------- | ------------------------------------------ | ---------------------- |
| `table_create_status` | Was the external Hive table created on S3? | `COMPLETED` = created; |

`FAILED` = DDL error (see `error_message`) |
| `table_create_completed_at` | When it finished | Timestamp |
| `table_create_duration_seconds` | Duration | Double |
| `table_already_existed` | Did the S3 table already exist (re-run)? | `true` on incremental re-runs — expected |

### 2.5 Field reference — Validation phase

| Field               | Meaning                   | Values / what to check |
| ------------------- | ------------------------- | ---------------------- |
| `validation_status` | Was validation performed? | `COMPLETED` = ran;     |

`FAILED` = validation errored;

`SKIPPED` = not run (e.g. upstream failed) |
| `validation_completed_at` | When validation finished | Timestamp |
| `validation_duration_seconds` | Duration | Double |
| `dest_hive_row_count` | Rows in the destination S3 Hive table | Compare with `source_row_count` |
| `dest_partition_count` | Partitions registered on destination | Compare with `source_partition_count` |
| `row_count_match` | Source rows == destination rows | `false` → **Issue 3** |
| `partition_count_match` | Source partitions == destination partitions | `false` → Issue 4 (`MSCK REPAIR TABLE`) |
| `schema_match` | Do source & destination schemas match? | `false` → inspect `schema_differences` |
| `schema_differences` | Text description of schema diffs | Empty when `schema_match = true` |

### 2.6 Field reference — Overall verdict

| Field            | Meaning                        | Values                                     |
| ---------------- | ------------------------------ | ------------------------------------------ |
| `overall_status` | Final per-table verdict        | See status table below                     |
| `error_message`  | Failure/skip detail            | Read this first when troubleshooting a row |
| `updated_at`     | Last time this row was updated | Timestamp                                  |

**`overall_status` (per-table) values:**

| Value                     | Category | Meaning                                                          | What to do                                                                              |
| ------------------------- | -------- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `VALIDATED`               | ✅       | Copied, table created, all validation checks passed              | Nothing                                                                                 |
| `VALIDATED_WITH_WARNINGS` | ⚠️       | Created, but a row-count or partition-count mismatch was flagged | Check `row_count_match` / `partition_count_match`; often fixed with `MSCK REPAIR TABLE` |
| `TABLE_CREATED`           | ✅       | Table created (validation not flagged VALIDATED yet)             | Usually fine; re-check validation fields                                                |
| `EMPTY_SOURCE`            | ⚠️ Info  | Source had no data to copy                                       | Confirm the source is genuinely empty                                                   |
| `TABLE_NOT_FOUND`         | ⚠️ Info  | Source table did not exist — skipped                             | Fix Excel `table` column if it _should_ exist                                           |
| `DATABASE_NOT_FOUND`      | ⚠️ Info  | Source database did not exist — skipped                          | Fix Excel `database` column                                                             |
| `VALIDATION_FAILED`       | ❌       | Data copied/created but validation failed                        | Inspect row/partition/schema fields + `error_message`                                   |
| `FAILED`                  | ❌       | Migration failed (discovery / distcp / table-create)             | Read `error_message`; use `yarn_application_id` for DistCp failures                     |

### 2.7 Ready-to-use queries

**All tables in a run (status overview):**

```sql
SELECT source_database, source_table, dest_database, file_format,
       discovery_status, distcp_status, table_create_status,
       validation_status, overall_status, error_message
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>'
ORDER BY source_database, source_table;
```

**Run summary — count by outcome:**

```sql
SELECT overall_status, COUNT(*) AS table_count
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>'
GROUP BY overall_status
ORDER BY table_count DESC;
```

**Only failures (troubleshooting starting point):**

```sql
SELECT source_database, source_table, overall_status,
       distcp_status, table_create_status, validation_status, error_message
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>'
  AND  overall_status IN ('FAILED', 'VALIDATION_FAILED')
ORDER BY source_database, source_table;
```

**One specific table, end-to-end:**

```sql
SELECT *
FROM   migration_tracking_db.migration_table_status
WHERE  source_table = '<TABLE>';
```

**Row / partition / schema reconciliation:**

```sql
SELECT source_database, source_table,
       source_row_count, dest_hive_row_count, row_count_match,
       source_partition_count, dest_partition_count, partition_count_match,
       schema_match, schema_differences
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>'
ORDER BY row_count_match, partition_count_match;
```

**Data-transfer reconciliation (source vs S3):**

```sql
SELECT source_database, source_table,
       source_total_size_bytes, s3_total_size_bytes_after, file_size_match,
       source_file_count, s3_file_count_after, file_count_match,
       distcp_bytes_copied, distcp_files_copied, distcp_is_incremental,
       yarn_application_id
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>'
ORDER BY file_size_match, file_count_match;
```

---

# PART B — DAG 2: Hive → Iceberg Migration

## 3. Run-level verification — `iceberg_migration_runs`

> **▶ Run in: Superset SQL Lab**

### 3.1 List recent Iceberg runs

```sql
SELECT *
FROM   migration_tracking_db.iceberg_migration_runs
ORDER BY started_at DESC
LIMIT 20;
```

Field Differences (DAG 2 vs DAG 1)

Field meanings are largely the same as DAG 1's `migration_runs` (§1.1), except that `run_id` is prefixed `iceberg_run_` and DAG 2 never writes `COMPLETED_WITH_MISSING` — a missing database or table is a **per-table `SKIPPED` row**, not a run-level status.

<aside>

> ⚠️ **A run where every table was SKIPPED still reports `COMPLETED`.** `failed_tables = 0` is the only condition for `COMPLETED`, and skipped tables are counted in `total_tables` but not in `successful_tables`. A green run status does not mean anything was migrated — check the skipped count (§4.5) before treating a run as done.

</aside>

---

## 4. Table-level verification — `iceberg_migration_table_status`

> **▶ Run in: Superset SQL Lab**

### 4.1 Field reference — Identity & strategy

| Field                                        | Meaning             | Values / what to check                                                                                           |
| -------------------------------------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `run_id`                                     | The Iceberg run     | `iceberg_run_...`                                                                                                |
| `source_database` / `source_table`           | Source Hive table   | For discovery-level skips, `source_table` holds the **Excel token** (e.g. a `*` pattern), not a real table name. |
| `destination_database` / `destination_table` | Iceberg destination | For SNAPSHOT this is a new DB (e.g. `<src>_iceberg`);                                                            |

For INPLACE it equals the source |
| `migration_type` | Conversion strategy | `INPLACE` = source table converted in place (zero-copy, via `system.migrate`);

`INPLACE_CTAS` = source table converted in place via a full copy plus a name swap (text tables only, see Rulebook Rule 6);

`SNAPSHOT` = new Iceberg table created alongside the source |
| `table_location` | Final Iceberg table location | `s3a://...` |

### 4.2 Field reference — Migration

| Field                         | Meaning                                                          | Values / what to check                                                                                                                                                                                                                                   |
| ----------------------------- | ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `status`                      | Per-table outcome                                                | See status table below                                                                                                                                                                                                                                   |
| `migration_duration_seconds`  | Time taken                                                       | Double                                                                                                                                                                                                                                                   |
| `error_message`               | Failure/skip reason, **always formatted `[REASON_CODE] detail`** | e.g. `[TEXT_FORMAT_INPLACE_UNSUPPORTED] sales_prod.logs stores its data as text …`. The code identifies the category (§4.4); the detail names the table and the underlying error. Read this first on any `SKIPPED` / `FAILED` / `VALIDATION_FAILED` row. |
| `started_at` / `completed_at` | Migration start/end                                              | Timestamps                                                                                                                                                                                                                                               |

> **Every non-migrated table gets a row.** Tables that fail discovery — wrong database name, wrong table name, a wildcard that matched nothing — are written to `iceberg_migration_table_status` with a `SKIPPED` status and a reason code. Nothing disappears silently, so the row count in tracking always reconciles against the Excel config. A **missing row** means the Excel file never listed that table at all.

### 4.3 Field reference — Validation

| Field                               | Meaning                                    | Values / what to check                                                                                                  |
| ----------------------------------- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| `source_hive_row_count`             | Rows in source Hive table                  | Compare with Iceberg count                                                                                              |
| `destination_iceberg_row_count`     | Rows in new Iceberg table                  | —                                                                                                                       |
| `row_count_match`                   | Row counts equal                           | `false` → **Issue 3**                                                                                                   |
| `source_hive_partition_count`       | Source partitions **containing data**      | Iceberg only registers partitions that have data files                                                                  |
| `source_hive_total_partition_count` | All partitions registered in the metastore | May exceed the non-empty count if there are empty partitions                                                            |
| `dest_iceberg_partition_count`      | Iceberg partitions with data               | Compare with `source_hive_partition_count`                                                                              |
| `partition_count_match`             | Partition counts equal                     | `false` → **Issue 4**                                                                                                   |
| `schema_match`                      | Schemas match                              | `tinyint`/`smallint` → `int` promotion is **expected** and _not_ flagged; any real diff appears in `schema_differences` |
| `schema_differences`                | Text description of diffs                  | Empty when matched                                                                                                      |
| `validation_status`                 | Was validation performed?                  | `COMPLETED` / `FAILED` / `SKIPPED`                                                                                      |
| `validation_duration_seconds`       | Validation time                            | Double                                                                                                                  |

**`status` (per-table) values:**

| Value               | Category | Meaning                                                         | What to do                    |
| ------------------- | -------- | --------------------------------------------------------------- | ----------------------------- |
| `VALIDATED`         | ✅       | Converted and all validation checks passed                      | Nothing                       |
| `COMPLETED`         | ✅       | Converted (not yet flagged VALIDATED)                           | Re-check validation fields    |
| `SKIPPED`           | ⚠️       | Known limitation **or config problem** — see `error_message`    | See §4.4 and **Issue 8**      |
| `VALIDATION_FAILED` | ❌       | Converted but validation failed (row/partition/schema mismatch) | Inspect reconciliation fields |
| `FAILED`            | ❌       | Conversion failed                                               | Read `error_message`          |

> **Why tables get SKIPPED or FAILED:** the exact reason is encoded in `error_message` as `[REASON_CODE]`. See §4.4 for the full code list and Issue 8 for the triage procedure.

### 4.4 — Reason Codes: Why a Table Was Skipped or Failed

The first token of `error_message` is a reason code, also shown under the status badge in the HTML report. Find your code here, then follow the linked issue.

| Code                              | What it means                                                                                        | Go to                                                               |
| --------------------------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| `ALREADY_ICEBERG`                 | Already migrated by an earlier run — nothing to convert.                                             | **No action.** This is success, not a problem.                      |
| `CONCURRENT_COMMIT_CONFLICT`      | An Iceberg commit clashed with another writer (already retried 6 times).                             | Issue 9 — stop other writers, re-run                                |
| `DATABASE_NOT_FOUND`              | The `database` in the Excel config is not in the metastore. One row per table token in that group.   | Issue 10                                                            |
| `DATA_PATH_MISSING`               | The metastore points at a path that no longer holds data.                                            | Issue 9                                                             |
| `DESTINATION_EXISTS`              | The destination Iceberg table already exists and could not be replaced.                              | Issue 9 — drop it (metadata-only) and re-run                        |
| `FORMAT_UNDETECTED_INPLACE`       | Storage format could not be read, so in-place conversion is unsafe.                                  | Issue 8                                                             |
| `INPLACE_CTAS_BACKUP_CONFLICT`    | `<table>_backup_` already exists and can't be confirmed as this migration's own backup.              | Issue 8                                                             |
| `INPLACE_CTAS_SWAP_INCOMPLETE`    | A previous in-place text copy was interrupted between the two renames.                               | Issue 8b                                                            |
| `INPLACE_CTAS_VERIFY_FAILED`      | The in-place text copy didn't match the source on row count or schema. The original is untouched.    | Issue 8                                                             |
| `MANAGED_TEXT_INPLACE_UNSUPPORTED`| Text table not confirmed `EXTERNAL` — renaming a managed table would move its data directory.        | Issue 8                                                             |
| `METADATA_READ_ERROR`             | `DESCRIBE FORMATTED` failed, so format/location/partitions are unknown.                              | Issue 9                                                             |
| `MIGRATION_ERROR`                 | Fallback — the error matched no known pattern. Read the detail text.                                 | Issue 9                                                             |
| `NO_TABLES_MATCHED_PATTERN`       | A `*` pattern in the Excel config matched no table.                                                  | Issue 10                                                            |
| `PERMISSION_DENIED`               | Access denied on the table, the metastore, or the storage path.                                      | Issue 9                                                             |
| `RESOURCE_ERROR`                  | Spark ran out of memory or disk.                                                                     | Issue 9 — raise executor resources, or migrate fewer tables per run |
| `SOURCE_IS_VIEW`                  | The Excel row names a view, not a physical table.                                                    | Issue 8                                                             |
| `SOURCE_NOT_V1_TABLE`             | Source is a DataSource V2 table, usually already Iceberg.                                            | Issue 8                                                             |
| `TABLE_NOT_FOUND`                 | The `table` in the Excel config is not in the metastore.                                             | Issue 8                                                             |
| `TASK_DID_NOT_PROCESS`            | The task ended before reaching this table, so no per-table result was captured.                      | **Re-run the DAG** for that database                                |
| `TEXT_FORMAT_INPLACE_UNSUPPORTED` | Text/CSV table with `iceberg_inplace_text_ctas` disabled — in-place `migrate` only registers Parquet/ORC/Avro data files. | Issue 8                                                             |
| `UNSUPPORTED_DATA_TYPE`           | A column type has no Iceberg equivalent.                                                             | Issue 6                                                             |
| `UNSUPPORTED_SOURCE_FORMAT`       | Source data files are in a format Iceberg cannot read.                                               | Issue 9                                                             |
| `VALIDATION_ERROR`                | Migration succeeded but validation did not pass. The detail names the failing check and the numbers. | Issue 3 (rows) · Issue 4 (partitions) · Issue 6 (schema)            |

### 4.5 Ready-to-use queries

**All tables in a run:**

```sql
SELECT source_database, source_table, migration_type,
       destination_database, destination_table,
       status, validation_status, error_message
FROM   migration_tracking_db.iceberg_migration_table_status
WHERE  run_id = '<RUN_ID>'
ORDER BY source_database, source_table;
```

**Run summary — count by outcome:**

```sql
SELECT status, COUNT(*) AS table_count
FROM   migration_tracking_db.iceberg_migration_table_status
WHERE  run_id = '<RUN_ID>'
GROUP BY status
ORDER BY table_count DESC;
```

**Only failures and skips:**

```sql
SELECT source_database, source_table, migration_type, status,
       regexp_extract(error_message, '^\[([A-Z0-9_]+)\]', 1) AS reason_code,
       error_message
FROM   migration_tracking_db.iceberg_migration_table_status
WHERE  run_id = '<RUN_ID>'
  AND  status IN ('FAILED', 'VALIDATION_FAILED', 'SKIPPED')
ORDER BY reason_code, source_database, source_table;
```

**Row / partition / schema reconciliation:**

```sql
SELECT source_database, source_table,
       source_hive_row_count, destination_iceberg_row_count, row_count_match,
       source_hive_partition_count, dest_iceberg_partition_count, partition_count_match,
       schema_match, schema_differences
FROM   migration_tracking_db.iceberg_migration_table_status
WHERE  run_id = '<RUN_ID>'
ORDER BY row_count_match, partition_count_match;
```

**One specific table, end-to-end:**

```sql
SELECT *
FROM   migration_tracking_db.iceberg_migration_table_status
WHERE  source_table = '<TABLE>';
```

**Reason-code breakdown for a run**

**The fastest read on why a run under-delivered:**

```sql
SELECT regexp_extract(error_message, '^\[([A-Z0-9_]+)\]', 1) AS reason_code,
       status,
       COUNT(*) AS tables
FROM   migration_tracking_db.iceberg_migration_table_status
WHERE  run_id = '<RUN_ID>'
  AND  error_message IS NOT NULL
GROUP BY 1, 2
ORDER BY tables DESC;
```

# PART C — Verifying the Migrated Data on S3

---

> **▶ Run in: Spark SQL (JupyterHub terminal)**
> These run against the live catalog/metastore, **not** the tracking tables. Start Spark SQL as shown in §0.1.

## 5. Inspect table format, location, and type

### 5.1 DESCRIBE FORMATTED — location, format, table type

```sql
DESCRIBE FORMATTED <dest_db>.<table>;
```

What to look for in the output, and what it means:

| Output row                      | DAG 1 (Hive on S3)                                          | DAG 2 (Iceberg)                                     |
| ------------------------------- | ----------------------------------------------------------- | --------------------------------------------------- |
| `Location`                      | `s3a://<bucket>/<db>/<table>` — confirms data is on S3      | Iceberg table path (warehouse or in-place location) |
| `Type`                          | `EXTERNAL` — dropping the table does **not** delete S3 data | Managed by the Iceberg catalog                      |
| `Provider`                      | `hive` / `parquet` / `orc`                                  | `iceberg` — confirms it’s an Iceberg table          |
| `InputFormat` / `Serde Library` | Confirms `PARQUET` / `ORC` / `AVRO` / `TEXT`                | n/a for Iceberg                                     |

**If `Location` is not on S3** → the table wasn’t repointed correctly; re-check DAG 1 `dest_location`.
**If `Provider` is not `iceberg`** (DAG 2) → the conversion didn’t take; re-check DAG 2 `status`/`error_message`.

> If `DESCRIBE FORMATTED` reports no storage descriptor rows at all, that is exactly the condition DAG 2 records as `FORMAT_UNDETECTED_INPLACE` (§4.4).

### 5.2 Confirm a table is really Iceberg (DAG 2)

```sql
DESCRIBE FORMATTED <dest_db>.<table>;      -- 'Provider' should be 'iceberg'

-- Iceberg-only metadata tables (these resolve ONLY if the table is truly Iceberg):
SELECT * FROM <dest_db>.<table>.snapshots;
SELECT * FROM <dest_db>.<table>.files      LIMIT 20;
SELECT * FROM <dest_db>.<table>.partitions;
SELECT * FROM <dest_db>.<table>.history;
```

If `.snapshots` returns rows, the table is a valid Iceberg table. If these queries error with “table or view not found” / “not an Iceberg table”, the conversion did not complete.

> **In Superset (Trino)** the metadata-table syntax differs — use `SELECT * FROM <dest_db>."<table>$snapshots";` with the dollar sign and double quotes.

### 5.3 Show schema

```sql
DESCRIBE <dest_db>.<table>;
-- Full Spark type detail:
DESCRIBE EXTENDED <dest_db>.<table>;
```

### 5.4 Show partitions

```sql
-- Hive table (DAG 1)
SHOW PARTITIONS <dest_db>.<table>;

-- Iceberg table (DAG 2) — SHOW PARTITIONS returns nothing for Iceberg; use:
SELECT * FROM <dest_db>.<table>.partitions;
```

## 6. Row-count spot checks

```sql
-- Destination count
SELECT COUNT(*) FROM <dest_db>.<table>;

-- Source count (run against the source cluster/metastore) **▶ Edge node**
SELECT COUNT(*) FROM <source_db>.<table>;

-- A single partition
SELECT COUNT(*) FROM <dest_db>.<table> WHERE <part_col> = '<value>';
```

These should match the `*_row_count` values in the tracking tables. A mismatch here that the tracking table did **not** flag usually means data changed after migration, or `MSCK REPAIR TABLE` is needed.

## 7. Verify files landed on S3 (filesystem level)

> **▶ Run in: JupyterHub terminal (shell)**

```bash
# List the table's files on S3
s3Cli ls s3a://<bucket>/<db>/<table>/
```

---

# PART D — Troubleshooting by Issue

Each issue below is **self-contained**: it lists the symptom, then the diagnostic steps across **all three environments** (tracking table in Superset, source data on the edge node, destination data in Spark SQL) in the order you should run them, followed by the resolution. Each step is labelled with where to run it.

Placeholders: `<RUN_ID>`, `<DB>`/`<TABLE>` (source), `<dest_db>`/`<table>` (destination), `<wh>` (source warehouse path), `<hs2>` (HiveServer2 host), `<bucket>`.

---

## Issue 1 — A table or database is missing from the destination

**Symptom:** An expected table is not on S3 / not in the report, or the run status is `COMPLETED_WITH_MISSING`.

**Step 1 — Tracking table**   **▶ Superset SQL Lab**

```sql
SELECT source_table, overall_status, discovery_status, error_message
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>' AND source_table = '<TABLE>';
```

- `overall_status = TABLE_NOT_FOUND` → table name in the Excel config doesn’t exist on source.
- `overall_status = DATABASE_NOT_FOUND` → source database name wrong in Excel `database` column.
- **No row at all** → the table was never listed; confirm the correct Excel file was used (`excel_file_path` in `migration_runs`).

**Step 2 — Confirm on the source**   **▶ Edge node**

```sql
SHOW DATABASES LIKE '<db>';           -- database present?
SHOW TABLES IN <db> LIKE '<table>';   -- table present?
```

- **No row returned** → the name genuinely doesn’t exist on source; the `_NOT_FOUND` status is correct. Fix the Excel `database` / `table` column and re-run.
- **Row returned** → the object exists; the mismatch is spelling/case, or the DAG pointed at a different metastore. Re-check the Excel entry and the DAG’s Hive connection.

**Resolution:** Correct the Excel config (or the source name) and re-trigger the DAG for that database/table.

> For DAG 2, the equivalent is Issue 10 — the reason code lives in `error_message`, not in a dedicated status column.

---

## Issue 2 — Table reported EMPTY_SOURCE

**Symptom:** `overall_status = EMPTY_SOURCE` (DAG 1) or a table migrated with 0 rows.

**Step 1 — Tracking table**   **▶ Superset SQL Lab**

```sql
SELECT source_table, overall_status, source_row_count,
       source_file_count, source_partition_count, unregistered_partitions
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>' AND source_table = '<TABLE>';
```

**Step 2 — Confirm on the source**   **▶ Edge node**
Check **both** the metastore row count **and** the files on disk — a table can have a schema but zero data files.

```sql
SELECT COUNT(*) FROM <db>.<table>;    -- metastore/engine row count
```

```bash
hadoop fs -du -s -h <wh>/<db>.db/<table>/     # total size; 0 = no data
hadoop fs -count    <wh>/<db>.db/<table>/     # dir_count  file_count  size
```

**Interpretation:**

- `COUNT(*) = 0` **and** `file_count = 0` → genuinely empty. `EMPTY_SOURCE` is correct; nothing to migrate.
- `COUNT(*) = 0` **but files exist** → partition-registration or padded/unpadded path problem — the data is on disk but the metastore can’t see it. Go to **Issue 4**.
- `COUNT(*) > 0` on source → the DAG likely read a different metastore/table; confirm the Excel `database`/`table` and the DAG’s Hive connection.

**Resolution:** If genuinely empty, no action. If data exists on disk but not in the metastore, fix registration on the source (Issue 4), then re-run.

---

## Issue 3 — Row counts don’t match (`row_count_match = false`)

**Symptom:** Tracking shows `row_count_match = false`, or `overall_status`/`status` = `VALIDATION_FAILED`.

**Step 1 — Tracking table**   **▶ Superset SQL Lab**

```sql
-- DAG 1
SELECT source_table, source_row_count, dest_hive_row_count,
       partition_count_match, unregistered_partitions, error_message
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>' AND row_count_match = false;

-- DAG 2
SELECT source_table, source_hive_row_count, destination_iceberg_row_count,
       partition_count_match, error_message
FROM   migration_tracking_db.iceberg_migration_table_status
WHERE  run_id = '<RUN_ID>' AND row_count_match = false;
```

For DAG 2 the `error_message` on these rows starts with `[VALIDATION_ERROR]` and names the exact counts that disagreed.

**Step 2 — Establish the true source count**   **▶ Edge node**

```sql
SELECT COUNT(*) FROM <db>.<table>;                         -- whole table

SELECT <part_col>, COUNT(*)                                -- per-partition
FROM   <db>.<table> GROUP BY <part_col> ORDER BY <part_col>;

SELECT COUNT(*) FROM <db>.<table> WHERE <part_col> >= '<value>';  -- if a filter was used
```

**Step 3 — Count the destination**   **▶ Spark SQL (JupyterHub terminal)**

```sql
SELECT COUNT(*) FROM <dest_db>.<table>;
```

**Interpretation (compare the three counts):**

- Source == tracking `source_row_count` == destination → counts actually match; the flag may be stale — re-run validation.
- Source **!=** `source_row_count` → the source changed after discovery, or has unregistered partitions on disk (Issue 4). Fix the source first, then re-run the DAG.

**Resolution:** Register missing partitions on source (Issue 4), confirm `distcp_status = COMPLETED`, then re-validate.

---

## Issue 4 — Partition counts don’t match / unregistered partitions / MSCK

**Symptom:** `partition_count_match = false`, `unregistered_partitions = true`, or a partitioned table shows `COUNT(*) = 0`.

**Step 1 — Tracking table**   **▶ Superset SQL Lab**

```sql
SELECT source_table, source_partition_count, dest_partition_count,
       unregistered_partitions, error_message
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>' AND partition_count_match = false;
```

**Step 2 — Compare the two partition views on the source**   **▶ Edge node**
There are **two independent partition views** and they can disagree:

```sql
-- (a) Metastore partitions — what Hive has REGISTERED
SHOW PARTITIONS <db>.<table>;
```

```bash
# (b) Filesystem partitions — directories that ACTUALLY EXIST on disk
hadoop fs -ls    <wh>/<db>.db/<table>/                    # 1 level (e.g. dt=...)
hadoop fs -ls -R <wh>/<db>.db/<table>/ | grep -E '=' | grep -v '/\.'   # all levels
hadoop fs -ls    <wh>/<db>.db/<table>/ | grep -c '='      # on-disk partition count
```

| Metastore (`SHOW PARTITIONS`)           | Filesystem (`hadoop fs -ls`)          | Meaning                                                                                               | Fix                                                                                        |
| --------------------------------------- | ------------------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Missing a partition that exists on disk | Directory present with data           | **Unregistered partition** — data on disk, not registered. DAG flags `unregistered_partitions = true` | `MSCK REPAIR TABLE` on the **source** (Step 3), then re-run the DAG                        |
| Partition registered                    | Directory missing / empty             | Registered but no data — contributes to `EMPTY_SOURCE` / count mismatch                               | If stale, `ALTER TABLE <db>.<table> DROP PARTITION (...)`                                  |
| `month=1` registered                    | Directory is `month=01` (zero-padded) | **Path format mismatch** — metastore spec points to a non-existent path, so `COUNT(*) = 0`            | Standardise on **unpadded** integers (`month=1`), or re-register to match the on-disk path |

**Step 3 — Fix registration**   **▶ Edge node (source)**

```sql
-- On the SOURCE, if the source metastore is missing on-disk partitions:
MSCK REPAIR TABLE <db>.<table>;
SHOW PARTITIONS <db>.<table>;          -- the two lists should now agree
```

**Resolution:** Register the missing partitions on the source (or fix zero-padded paths on the source), then re-run the DAG / re-validate. Fixing the **source** before re-running resolves most row _and_ partition mismatches at the root.

> Iceberg (DAG 2) only registers partitions that contain data files, so compare against `source_hive_partition_count` (non-empty), not `source_hive_total_partition_count`.

---

## Issue 5 — DistCp (data copy) failed (`distcp_status = FAILED`)

**Symptom:** `distcp_status = FAILED` and/or `overall_status = FAILED`.

**Step 1 — Tracking table**   **▶ Superset SQL Lab**

```sql
SELECT source_table, distcp_status, yarn_application_id, error_message
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>' AND distcp_status = 'FAILED';
```

**Step 2 — Pull the DistCp job logs**   **▶ Edge node**

```bash
# yarn_application_id may hold MULTIPLE comma-separated IDs when a partition
# filter was used (one DistCp job per partition) — run this for EACH id.
yarn logs -applicationId <yarn_application_id> | tail -100
```

**Step 3 — Confirm source readability (if the log points at the source)**   **▶ Edge node**

```bash
hadoop fs -ls -R <wh>/<db>.db/<table>/ | head       # path exists & readable?
hadoop fs -du -s -h <wh>/<db>.db/<table>/
```

**Step 4 — Check for a log-directory permission error** **▶ Airflow task log**

If the error mentions _permission denied_ on a path rather than on the source or
the S3 destination, DistCp could not write its own log file. The log directory
lives on the **cluster filesystem**, not the edge node's local disk, and is scoped
per service account:

```
<cluster_distcp_log_root>/<cluster_user>/distcp_logs/<run_id>/
```

The resolved value is printed in the `cluster_login_setup` task log as
`DISTCP_LOG_DIR=`. Verify the account can write there: **▶ Edge node**

```bash
# as the tenant service account
maprlogin print                                   # confirm the active ticket user
hadoop fs -ls -d /tmp/$(maprlogin print 2>/dev/null | sed -n 's/.*user = \([^,]*\),.*/\1/p' | head -1)
```

**Typical cause:** an older directory owned by a _different_ tenant's service
account sitting at the configured path. Under impersonation each tenant runs as
its own account, so a directory created by one cannot be written to by another —
and granting `777` on the parent does not help, because the conflict is on the
directory itself. Confirm ownership with `hadoop fs -ls`, and either remove the
stale directory or point `cluster_distcp_log_root` somewhere the account owns.

**Resolution:** Common causes are S3 credentials/endpoint, source path permissions, DistCp log directory permissions (Step 4), or network. Fix the underlying cause, then re-run the DAG — it resumes incrementally (`-update`), so already-copied tables aren’t recopied.

---

## Issue 6 — Schema mismatch (`schema_match = false`)

**Symptom:** `schema_match = false` with entries in `schema_differences`.

**Step 1 — Tracking table**   **▶ Superset SQL Lab**

```sql
-- DAG 1
SELECT source_table, schema_match, schema_differences
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>' AND schema_match = false;

-- DAG 2
SELECT source_table, schema_match, schema_differences
FROM   migration_tracking_db.iceberg_migration_table_status
WHERE  run_id = '<RUN_ID>' AND schema_match = false;
```

**Step 2 — Compare schemas on both sides**

```sql
-- Source   ▶ Edge node
DESCRIBE <db>.<table>;

-- Destination   ▶ Spark SQL (JupyterHub terminal)
DESCRIBE <dest_db>.<table>;
```

**Interpretation & resolution:**

- **DAG 2 only:** `tinyint`/`smallint` → `int` promotion is **expected** for Iceberg and is _not_ flagged. Any value in `schema_differences` is a _genuine_ difference.
- Compare column names/types side by side; a real mismatch usually means the source schema changed after discovery, or a type isn’t representable on the destination. Re-run discovery for that table or adjust and re-migrate.
- If the migration itself failed on a type, the reason code is `UNSUPPORTED_DATA_TYPE` (§4.4) rather than a schema-comparison diff.

---

## Issue 7 — Wrong or UNKNOWN data format

**Symptom:** Tracked `file_format = UNKNOWN`, or you need to confirm the storage format that actually landed.

**Step 1 — Tracking table**   **▶ Superset SQL Lab**

```sql
SELECT source_table, file_format, table_type
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>' AND source_table = '<TABLE>';
```

**Step 2 — Confirm the source format**   **▶ Edge node**

```sql
DESCRIBE FORMATTED <db>.<table>;
```

Look at `InputFormat` / `SerDe Library` → `PARQUET` / `ORC` / `AVRO` / `TEXT` (LazySimpleSerDe = text), the `Location`, and (for text) `field.delim` / `serialization.null.format`.

**Step 3 — Confirm the destination format**   **▶ Spark SQL (JupyterHub terminal)**

```sql
DESCRIBE FORMATTED <dest_db>.<table>;   -- InputFormat/Serde (DAG 1) or Provider=iceberg (DAG 2)
```

**Resolution:** If tracking says `UNKNOWN`, trust the live `DESCRIBE FORMATTED` output. Note: text-format tables *can* be in-place-migrated to Iceberg, via CTAS, when `iceberg_inplace_text_ctas` is on and the table is confirmed `EXTERNAL` (Rulebook Rule 6). Otherwise they surface as `SKIPPED` in DAG 2 — `TEXT_FORMAT_INPLACE_UNSUPPORTED` if the flag is off, `MANAGED_TEXT_INPLACE_UNSUPPORTED` if the table can't be confirmed `EXTERNAL` (Issue 8).

---

## Issue 8 — Iceberg Table SKIPPED (DAG 2)

**Symptom:** `status = SKIPPED` in `iceberg_migration_table_status`, or amber SKIPPED badges in the HTML report.

`SKIPPED` is never a crash. It means the DAG decided in advance that the table could not be migrated as configured, recorded why, and carried on with the rest. Most skips are either **a typo in the Excel config** or **a table that cannot be converted in place** — and some are not problems at all.

**Step 1 — List the skips with their reason codes** ▶ Superset SQL Lab

```sql
SELECT source_table, migration_type,
       regexp_extract(error_message, '^\[([A-Z0-9_]+)\]', 1) AS reason_code,
       error_message
FROM   migration_tracking_db.iceberg_migration_table_status
WHERE  run_id = '<RUN_ID>' AND status = 'SKIPPED'
ORDER BY reason_code, source_table;
```

**Step 2 — Set aside the skips that need no action**

Before troubleshooting anything, remove these from the list:

- **`ALREADY_ICEBERG`** — the table was migrated by an earlier run. This is the expected result of re-running the DAG, and is how you confirm a re-run is safe.
- **`SOURCE_NOT_V1_TABLE`** — almost always the same situation, reported by Spark rather than by the pre-check.

Confirm either one is genuinely Iceberg with `DESCRIBE FORMATTED <db>.<table>` in Spark SQL: `Provider` should read `iceberg` (§5.2). If it does, there is nothing to fix.

**Step 3 — Triage the rest**

| Reason code                                     | What happened                                                                                                                           | What to do                                                                                                                                                       |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TEXT_FORMAT_INPLACE_UNSUPPORTED`               | Text/CSV table (LazySimpleSerDe) with `inplace_migration = T` and `iceberg_inplace_text_ctas` disabled.                                 | Confirm the format (Step 4), then either enable `iceberg_inplace_text_ctas` to migrate it in place under the same name, or set `inplace_migration = F` to accept the namespace change and migrate it via snapshot CTAS. |
| `MANAGED_TEXT_INPLACE_UNSUPPORTED`              | Text table not confirmed `EXTERNAL`. Renaming a managed table would move its data directory.                                            | Confirm with `DESCRIBE FORMATTED` (Step 4); convert to `EXTERNAL`, or use `inplace_migration = F`.                                                               |
| `INPLACE_CTAS_VERIFY_FAILED`                    | The in-place copy did not match the source on row count or schema. The original table is untouched.                                     | Almost always a concurrent write. Freeze writers and re-run.                                                                                                    |
| `INPLACE_CTAS_BACKUP_CONFLICT`                  | `<table>_backup_` exists and cannot be confirmed as ours.                                                                               | Rename or drop that table, then re-run.                                                                                                                         |
| `INPLACE_CTAS_SWAP_INCOMPLETE`                  | A previous in-place text copy was interrupted between the two renames.                                                                  | Issue 8b, below.                                                                                                                                                 |
| `FORMAT_UNDETECTED_INPLACE`                     | The storage format could not be read from the table definition, so in-place conversion is unsafe.                                       | Confirm the format (Step 4). If it is genuinely Parquet/ORC/Avro, the metastore entry is damaged — recreate the table. If it is text, treat it as the row above. |
| `SOURCE_IS_VIEW`                                | The Excel row names a view, not a physical table.                                                                                       | Remove the row from the Excel config. Recreate the view over the migrated Iceberg tables once the migration is done.                                             |
| `TABLE_NOT_FOUND`                               | The table name in the Excel `table` column is not in the metastore.                                                                     | Check the spelling, then confirm on the source (Issue 1, Step 2). If the table exists on the source but not on S3, run DAG 1 for it first.                       |
| `DATABASE_NOT_FOUND`                            | The whole database was missing — **every table token in that Excel group** carries this row.                                            | Issue 10                                                                                                                                                         |
| `NO_TABLES_MATCHED_PATTERN`                     | A `*` pattern matched no table.                                                                                                         | Issue 10                                                                                                                                                         |
| `TASK_DID_NOT_PROCESS` (on `validation_status`) | The table migrated correctly, but validation never ran for it.                                                                          | Re-run the DAG. The Iceberg table itself is intact.                                                                                                              |

**Step 4 — Confirm the source format** ▶ Edge node

Needed only for `TEXT_FORMAT_INPLACE_UNSUPPORTED`, `MANAGED_TEXT_INPLACE_UNSUPPORTED` and `FORMAT_UNDETECTED_INPLACE`.

```sql
DESCRIBE FORMATTED <db>.<table>;
```

`SerDe Library = LazySimpleSerDe`, or `InputFormat` ending in `TextInputFormat`, confirms text. If the storage rows are missing from the output altogether, that is exactly what `FORMAT_UNDETECTED_INPLACE` is reporting.

**Step 5 — Apply the fix and re-run**

1. Find the Excel config this run used:

```sql
SELECT excel_file_path FROM migration_tracking_db.iceberg_migration_runs
WHERE  run_id = '<RUN_ID>';
```

1. Edit that file (correct the `database` / `table` value, or set `inplace_migration = F`) and upload it back to the same S3 path.
2. Re-trigger the DAG with that `excel_file_path`.

**Step 6 — Confirm the fix worked** ▶ Superset SQL Lab

Using the **new** run's `run_id`:

```sql
SELECT source_table, status, validation_status, error_message
FROM   migration_tracking_db.iceberg_migration_table_status
WHERE  run_id = '<NEW_RUN_ID>' AND source_table = '<TABLE>';
```

`status = VALIDATED` means the table is migrated and its row counts, partitions and schema all match the source. Anything else — go back to Step 1 with the new reason code.

---

## Issue 8b — Table missing after an interrupted in-place text copy

**Symptom:** discovery reports `INPLACE_CTAS_SWAP_INCOMPLETE` (`FAILED`) for a table named literally in the Excel config (not a `*` pattern), and **both** `<db>.<table>_backup_` and `<db>.<table>__ice_staging` exist in the metastore while `<db>.<table>` does not.

Both are required. A `_backup_` table on its own is also what `system.migrate` leaves behind when `iceberg_drop_backup` is false, so a missing table beside one of those is reported as `TABLE_NOT_FOUND` (`SKIPPED`) with a note naming the backup — not as an interrupted swap.

This means an in-place text CTAS died between the two renames: the source had already been renamed to `<table>_backup_`, but the verified copy was never promoted to `<table>`. A retry **inside the same Airflow run** repairs this automatically — this manual fix is only needed after the whole run died and you're triggering a fresh one.

**Fix** ▶ Spark SQL (JupyterHub terminal)

```sql
ALTER TABLE <db>.<table>_backup_ RENAME TO <db>.<table>;
DROP TABLE IF EXISTS <db>.<table>__ice_staging PURGE;
```

The `PURGE` is safe here: `<table>__ice_staging` (if it still exists) was written by the failed run and is referenced by nothing else — restoring the backup does not touch it.

Re-trigger the DAG once the table is restored.

**Variant — the backup could not be confirmed.** The same reason code is recorded by the migration task (not discovery) when `<db>.<table>` is missing and `<db>.<table>_backup_` exists but its `Location` does not match the location discovery recorded for the source. The DAG refuses to rename it back, because doing so could resurrect an unrelated table under the production name. Compare the two by hand before running the fix above:

```sql
DESCRIBE FORMATTED <db>.<table>_backup_;   -- Location must be the source's original path
```

If the backup is the source, run the fix above. If it is something else, find where the source went from the failed run's logs before touching either table.

**Check for orphaned files.** The copy is written to `<source_location>_iceberg`. If a CTAS died after writing data files but before the metastore commit, no table ever existed to `DROP ... PURGE`, so those files stay behind and the next attempt writes a fresh table alongside them. After any interrupted in-place copy, list that prefix and remove anything the current table's snapshot does not reference:

```bash
hdfs dfs -ls -R s3a://<bucket>/<path>/<table>_iceberg
```

---

## Issue 9 — Run reports COMPLETED_WITH_FAILURES (get the exact list)

**Symptom:** Run status is `COMPLETED_WITH_FAILURES` and you need the precise set of failed tables.

**Step 1 — List the failures**   **▶ Superset SQL Lab**

```sql
-- DAG 1
SELECT source_database, source_table, overall_status, error_message
FROM   migration_tracking_db.migration_table_status
WHERE  run_id = '<RUN_ID>' AND overall_status IN ('FAILED','VALIDATION_FAILED');

-- DAG 2
SELECT source_database, source_table, status,
       regexp_extract(error_message, '^\[([A-Z0-9_]+)\]', 1) AS reason_code,
       error_message
FROM   migration_tracking_db.iceberg_migration_table_status
WHERE  run_id = '<RUN_ID>' AND status IN ('FAILED','VALIDATION_FAILED');
```

**Step 2 — Triage each failure** by its `error_message`/status, then jump to the matching issue: DistCp → Issue 5, row/partition → Issue 3 / Issue 4, schema → Issue 6, skipped → Issue 8, whole database missing → Issue 10. For DAG 2, the reason code (§4.4) names the cause directly.

> Remember that a DAG 2 run reporting plain `COMPLETED` can still have migrated nothing if every table was `SKIPPED` (§3.1). Check the skipped count before closing out a run.

---

## Issue 10 — A Whole Database or Wildcard Produced No Tables (DAG 2)

**Symptom:** an Excel row for a database yields no migrated tables, and the report shows `SKIPPED` rows named after the _pattern_ — a row literally called `*` — rather than after real tables.

This is a config problem, not a migration problem. The DAG records it per Excel entry instead of failing the task, so the run can still report `COMPLETED`.

**Step 1 — Identify which Excel entry failed** ▶ Superset SQL Lab

```sql
SELECT source_database, source_table AS excel_token, status, error_message
FROM   migration_tracking_db.iceberg_migration_table_status
WHERE  run_id = '<RUN_ID>'
  AND (error_message LIKE '[DATABASE_NOT_FOUND]%'
    OR error_message LIKE '[NO_TABLES_MATCHED_PATTERN]%');
```

On these rows `source_table` holds the **Excel entry** (e.g. `*` or `trans*`), not a real table name.

**Step 2 — Check what the metastore actually has** ▶ Spark SQL (JupyterHub terminal)

```sql
SHOW DATABASES LIKE '<db>';
SHOW TABLES IN <db>;
```

**Step 3 — Match the result to the fix**

| Result                                    | Meaning                                                                                                      | Fix                                                                |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------ |
| Database absent                           | DAG 2 reads the **S3/Hive database created by DAG 1**, not the MapR source.                                  | Run DAG 1 for that database first, then re-run DAG 2.              |
| Database present, tables present          | Name mismatch — spelling, case, or a missing `_s3` suffix.                                                   | Correct the `database` value in the Excel config.                  |
| Database present, only `*_backup_` tables | Discovery deliberately excludes `*_backup_` / `*__BACKUP__`. The detail message says how many were excluded. | Expected after an in-place run — there is nothing left to migrate. |
| Database present, pattern too narrow      | The wildcard matched nothing, e.g. `trans*` where no table starts with `trans`.                              | Widen the pattern, or list the tables explicitly.                  |

**Resolution:** correct the Excel `database` / `table` value, upload it to the same S3 path, and re-trigger the DAG. Confirm with Issue 8 Step 6.

---

## Issue quick-reference: source-side check sequence (copy/paste)

For any single table under investigation, run this on the **edge node** and compare with the tracking row:

```bash
# 1. Exists?
beeline -u jdbc:hive2://<hs2>:10000 -e "SHOW TABLES IN <db> LIKE '<table>';"

# 2. Row count (metastore/engine)
beeline -u jdbc:hive2://<hs2>:10000 -e "SELECT COUNT(*) FROM <db>.<table>;"

# 3. Registered partitions
beeline -u jdbc:hive2://<hs2>:10000 -e "SHOW PARTITIONS <db>.<table>;"

# 4. On-disk partitions + size + file count
hadoop fs -ls  <wh>/<db>.db/<table>/
hadoop fs -du -s -h <wh>/<db>.db/<table>/
hadoop fs -count    <wh>/<db>.db/<table>/

# 5. Format / location / serde
beeline -u jdbc:hive2://<hs2>:10000 -e "DESCRIBE FORMATTED <db>.<table>;"

# 6. If step 3 (registered) < step 4 (on-disk), register on the source:
beeline -u jdbc:hive2://<hs2>:10000 -e "MSCK REPAIR TABLE <db>.<table>;"
```

---

## Appendix A — Quick Status Cheat-Sheet

**DAG 1 run status:** `RUNNING` · `COMPLETED` · `COMPLETED_WITH_MISSING` · `COMPLETED_WITH_FAILURES` · `FAILED`

**DAG 1 per-table `overall_status`:**
✅ `VALIDATED`, `TABLE_CREATED`
⚠️ `VALIDATED_WITH_WARNINGS`, `EMPTY_SOURCE`, `TABLE_NOT_FOUND`, `DATABASE_NOT_FOUND`
❌ `VALIDATION_FAILED`, `FAILED`

**DAG 1 phase statuses** (`discovery_status`, `distcp_status`, `table_create_status`, `validation_status`): each `COMPLETED` / `FAILED` / `SKIPPED` (+ `EMPTY_SOURCE` / `TABLE_NOT_FOUND` / `DATABASE_NOT_FOUND` on distcp).

**DAG 2 run status:** `RUNNING` · `COMPLETED` · `COMPLETED_WITH_FAILURES` · `FAILED`

(no `COMPLETED_WITH_MISSING` — missing objects are per-table `SKIPPED` rows)

**DAG 2 per-table `status`:**

- ✅ `VALIDATED`, `COMPLETED`
- ⚠️ `SKIPPED`
- ❌ `VALIDATION_FAILED`, `FAILED`

**DAG 2 reason codes** — first token of `error_message`, format `[CODE] detail`:

- _Config / discovery_ (`SKIPPED`): `DATABASE_NOT_FOUND` · `TABLE_NOT_FOUND` · `NO_TABLES_MATCHED_PATTERN`
- _In-place not possible_ (`SKIPPED`): `ALREADY_ICEBERG` · `SOURCE_IS_VIEW` · `TEXT_FORMAT_INPLACE_UNSUPPORTED` · `MANAGED_TEXT_INPLACE_UNSUPPORTED` · `INPLACE_CTAS_BACKUP_CONFLICT` · `FORMAT_UNDETECTED_INPLACE` · `SOURCE_NOT_V1_TABLE`
- _Migration failures_ (`FAILED`): `DATA_PATH_MISSING` · `DESTINATION_EXISTS` · `UNSUPPORTED_SOURCE_FORMAT` · `UNSUPPORTED_DATA_TYPE` · `PERMISSION_DENIED` · `METADATA_READ_ERROR` · `RESOURCE_ERROR` · `CONCURRENT_COMMIT_CONFLICT` · `INPLACE_CTAS_VERIFY_FAILED` · `INPLACE_CTAS_SWAP_INCOMPLETE` · `MIGRATION_ERROR`
- _Never reached_: `TASK_DID_NOT_PROCESS` · `VALIDATION_ERROR`

Full descriptions and actions: §4.4. Triage procedure: Issue 8.

---

## Appendix B — Where-to-run & table quick reference

| Query target                      | Where to run                        | Tables / commands                                                                                      |
| --------------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Tracking tables                   | **Superset SQL Lab**                | `migration_runs`, `migration_table_status`, `iceberg_migration_runs`, `iceberg_migration_table_status` |
| Migrated data (destination on S3) | **Spark SQL (JupyterHub terminal)** | `DESCRIBE FORMATTED`, `SELECT COUNT(*)`, `SHOW PARTITIONS`, `<tbl>.snapshots`, `MSCK REPAIR TABLE`     |
| S3 filesystem                     | **JupyterHub terminal (shell)**     | `s3Cli ls`                                                                                             |
| **Source data (MapR/HDFS)**       | **Edge node** (SSH)                 | `beeline`/`hive`/`spark-sql` for metastore                                                             |

`hadoop fs`/`hdfs dfs` for filesystem

``yarn logs` for DistCp job logs`

`MSCK REPAIR TABLE` on source |

| DAG | Run table                                      | Table-level table                                      | Partitioned by    |
| --- | ---------------------------------------------- | ------------------------------------------------------ | ----------------- |
| 1   | `migration_tracking_db.migration_runs`         | `migration_tracking_db.migration_table_status`         | `source_database` |
| 2   | `migration_tracking_db.iceberg_migration_runs` | `migration_tracking_db.iceberg_migration_table_status` | `source_database` |

> Replace `migration_tracking_db` with your configured tracking database name, and `<RUN_ID>`, `<DB>`, `<TABLE>`, `<dest_db>`, `<source_db>`, `<bucket>` placeholders with actual values.
