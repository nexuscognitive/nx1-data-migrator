# DAG 5: Iceberg Rewrite Table Path Migration

## Prerequisite Conditions

Before using this DAG, ensure the following conditions are met:

- Both **data files** and **metadata files** for all tables to be migrated have already been copied to the destination S3 bucket.
- The destination Spark/Iceberg environment is running Apache Iceberg **1.4.0 or newer** and supports the `rewrite_table_path` stored procedure.
- You have the required S3 credentials and access permissions for both reading and writing to the relevant S3 paths.
- The configuration Excel file is available in S3 and contains the necessary columns: `database`, `table` (optional), `source_s3_prefix`, and `dest_s3_prefix`.
- Any required Airflow Variables or environment variables (such as tracking/report locations and S3 credentials) are set and accessible by the DAG.

**Note:**

This DAG supports both full snapshot and incremental migrations. For incremental loads, you must manually re-copy any new or changed data files and the corresponding updated metadata files to the destination S3 before each run. The DAG will then rewrite all metadata paths for the current state at the destination. Automatic detection or syncing of incremental changes is not handled by this DAG; it operates on whatever data and metadata are present at the destination when triggered.


Iceberg-to-Iceberg migration using the `rewrite_table_path` stored procedure. Use this DAG when data **and** metadata have already been copied to the destination S3 bucket and snapshot history / partition transform fidelity must be preserved.

---



## Strategy: `iceberg_rewrite_table_path`

The procedure reads the existing Iceberg metadata at the destination — which still references the old source S3 path prefixes — and rewrites every path reference via `CALL spark_catalog.system.rewrite_table_path`. The procedure writes the rewritten metadata directly to `dest_path/metadata` (derived by applying the same prefix rewrite to the table's `location` field). The table is then permanently registered in HMS via `register_table` using that new metadata.

**Preserved by this approach:**
- Full snapshot history and time-travel
- All partition transforms (`year`, `month`, `bucket`, `truncate`, etc.)
- Exact schema types — no DDL type translation
- All table properties

---

## Requirements

- Both **data files** and **metadata files** must be pre-copied to the destination S3 path before running this DAG.
- The destination Spark/Iceberg environment must support `rewrite_table_path` — Apache Iceberg **1.4+** required.
- `source_s3_prefix` and `dest_s3_prefix` must be provided in the Excel config (see below).

---

### Required Variables

| Airflow Variable key | Env var fallback | Description | Example |
|---|---|---|---|
| `migration_tracking_database` | `MIGRATION_TRACKING_DATABASE` | Database for Iceberg tracking tables | `migration_tracking` |
| `migration_tracking_location` | `MIGRATION_TRACKING_LOCATION` | S3 location for tracking tables | `s3a://data-lake/migration_tracking` |
| `migration_report_location` | `MIGRATION_REPORT_LOCATION` | S3 location where HTML reports are written | `s3a://data-lake/migration_reports` |

### Optional Variables

| Airflow Variable key | Env var fallback | Default | Description |
|---|---|---|---|
| `migration_email_recipients` | `MIGRATION_EMAIL_RECIPIENTS` | _(empty)_ | Comma-separated list of report recipients; email is skipped if empty |
| `migration_smtp_conn_id` | `MIGRATION_SMTP_CONN_ID` | `smtp_default` | Airflow SMTP connection ID |

### S3 Credentials

DAG 5 only accesses the **destination** S3 bucket at runtime. The `source_s3_prefix` is used purely as a text prefix to construct the source metadata file path — Spark never opens a connection to source S3. Source S3 credentials are not required.

**Destination credentials** — set the dest-specific variables; the global variables are used as fallback if they are not set:

| Airflow Variable key | Env var fallback | Description |
|---|---|---|
| `s3_dest_endpoint` | `S3_DEST_ENDPOINT` | Destination S3 endpoint URL |
| `s3_dest_access_key` | `S3_DEST_ACCESS_KEY` | Destination S3 access key |
| `s3_dest_secret_key` | `S3_DEST_SECRET_KEY` | Destination S3 secret key |

**Global S3 fallback** (used when dest-specific variables are not set):

| Airflow Variable key | Env var fallback | Description |
|---|---|---|
| `s3_endpoint` | `S3_ENDPOINT` | Global S3 endpoint URL |
| `s3_access_key` | `S3_ACCESS_KEY` | Global S3 access key |
| `s3_secret_key` | `S3_SECRET_KEY` | Global S3 secret key |

Credentials are applied at the per-bucket level internally via Hadoop's `fs.s3a.bucket.<name>.*` properties — no additional configuration is needed beyond setting the variables above.

### DAG Parameter

| Parameter | Required | Description | Example |
|---|---|---|---|
| `excel_file_path` | Yes | S3 path to Excel config file | `s3a://config-bucket/iceberg_rewrite_migration.xlsx` |

---

## Excel Configuration Format

| Column | Required | Description | Example |
|---|---|---|---|
| `database` | **Yes** | HMS database name (same for source and destination) | `analytics` |
| `table` | No | Table name(s) — single, comma-separated, or wildcard; defaults to `*` | `orders` or `trans*` or `*` |
| `source_s3_prefix` | **Yes** | Original source S3 prefix (the prefix embedded in the pre-copied metadata files) | `s3a://source-bucket/warehouse` |
| `dest_s3_prefix` | **Yes** | Destination S3 prefix (what the metadata paths should be rewritten to) | `s3a://dest-bucket/warehouse` |

Rows are grouped by `(database, source_s3_prefix, dest_s3_prefix)`. Multiple rows for the same group accumulate their table tokens.

**Example:**

```
| database  | table      | source_s3_prefix            | dest_s3_prefix             |
|-----------|------------|-----------------------------|----------------------------|
| analytics | orders     | s3a://source-bucket/warehouse | s3a://dest-bucket/warehouse |
| analytics | customers  | s3a://source-bucket/warehouse | s3a://dest-bucket/warehouse |
| reporting | *          | s3a://source-bucket/warehouse | s3a://dest-bucket/warehouse |
```

---

## Task Flow

```
init_tracking_tables
    ↓
create_migration_run
    ↓
parse_excel
    ↓
┌───────────────────────────────────────────────────────────────────┐
│  Dynamic Task Mapping (one set of tasks per database config)      │
│                                                                   │
│  validate_data_presence  [trigger: all_done]                      │
│    ↓                                                              │
│  update_data_presence_in_tracking  [trigger: all_done]            │
│    ↓                                                              │
│  discover_tables (PySpark)  [trigger: all_done]                   │
│    ↓                                                              │
│  update_discovered_tables_in_tracking  [trigger: all_done]        │
│    ↓                                                              │
│  rewrite_and_register_tables  [trigger: all_done]                          │
│    ↓                                                              │
│  update_rewrite_and_register_in_tracking  [trigger: all_done]             │
│    ↓                                                              │
│  validate_dest_tables  [trigger: all_done, max=3]                 │
│    ↓                                                              │
│  update_validation_in_tracking  [trigger: all_done]               │
└───────────────────────────────────────────────────────────────────┘
    ↓
generate_html_report  [trigger: all_done]
    ↓
send_report_email  [trigger: all_done]
    ↓
finalize_run  [trigger: all_done]
```

---

## Task Summaries

### Step 0 — `init_tracking_tables`

**Type:** PySpark

- Creates the tracking database if it does not exist.
- Creates two Iceberg tracking tables if they do not exist:
  - `rewrite_migration_runs` — run-level metadata (run ID, status, counts, timestamps)
  - `rewrite_migration_table_status` — table-level tracking partitioned by `source_database`

---

### Step 1 — `create_migration_run`

**Type:** PySpark

- Generates a unique run ID in the format `rewrite_run_{YYYYMMDD_HHMMSS}_{uuid8}`.
- Inserts an initial `RUNNING` record into `rewrite_migration_runs`.
- Returns the `run_id` used by all downstream tasks.

---

### Step 2 — `parse_excel`

**Type:** PySpark

- Reads the Excel file from S3 using `pyspark.pandas.read_excel`.
- Normalizes column names (lowercase, strip whitespace).
- Groups rows by `(database, source_s3_prefix, dest_s3_prefix)`.
- Supports single table names, comma-separated lists, and wildcard patterns in the `table` column.
- Returns a list of database-config dicts for dynamic task mapping.
- Raises if no valid rows are found.

---

### Step 3 — `validate_data_presence`

**Type:** PySpark (mapped per database config) · trigger: `all_done`

- Takes `db_config` (the `parse_excel` output dict) directly.
- Lists table directories under `dest_s3_prefix` using `_list_iceberg_tables` and filters by `table_tokens` via `_match_tokens`.
- For each matched table, uses the Hadoop FileSystem API to verify:
  - Path must exist.
  - A `metadata/` subdirectory must be present (required for `rewrite_table_path`).
  - At least one file must be present.
- Each presence result includes a `dest_path` key used by `discover_tables` downstream.
- Sets status `CONFIRMED`, `MISSING`, or `FAILED` per table.
- Raises only on `FAILED` (API/connectivity errors); `MISSING` tables continue and are tracked.

---

### Step 4 — `update_data_presence_in_tracking`

**Type:** PySpark (mapped per database config) · trigger: `all_done`

- Inserts the initial tracking records into `rewrite_migration_table_status` (no records exist yet at this stage).
- On reruns, checks for existing rows first — UPDATEs if found, INSERTs otherwise (idempotent).
- Populated fields at this stage: `run_id`, `source_database`, `source_table`, `dest_database`, `dest_bucket`, `source_s3_location`, `dest_s3_location`, `source_s3_prefix`, `dest_s3_prefix`, `data_presence_status`, `data_presence_checked_at`, `data_presence_file_count`, `data_presence_size_bytes`, `overall_status` (`DATA_CONFIRMED`, `DATA_MISSING`, or `FAILED`).

---

### Step 5 — `discover_tables`

**Type:** PySpark (mapped per database config) · trigger: `all_done` · **@track_duration**

- Takes `presence_result` (the `update_data_presence_in_tracking` output) directly.
- Iterates over `presence_results` where `status == 'CONFIRMED'` — no additional filesystem listing needed.
- Reads schema, partition spec, row count, and file stats from `metadata.json` at the destination S3 path for each confirmed table (not from HMS — HMS may not have the table registered yet and would not preserve partition transform details).
- Returns `{**presence_result, 'tables': metadata_list}` so `presence_results` remains available to downstream tasks.
- On per-table failure, records an error entry and raises after processing all tables.

---

### Step 6 — `update_discovered_tables_in_tracking`

**Type:** PySpark (mapped per database config) · trigger: `all_done`

- UPDATEs existing records in `rewrite_migration_table_status` (records were already inserted by `update_data_presence_in_tracking`).
- Sets `discovery_status`, `discovery_completed_at`, `discovery_duration_seconds`, `source_s3_location`, `dest_s3_location`, `file_format`, `table_type`, `source_row_count`, `source_file_count`, `source_total_size_bytes`, `partition_count`, `source_partition_count`, `schema_json`, `partitions_json`, `is_partitioned`, `partition_columns`, and `overall_status` (set to `DISCOVERED` for successful tables).

---

### Step 7 — `rewrite_and_register_tables`

**Type:** PySpark (mapped per database config) · trigger: `all_done` · **@track_duration**

Skips tables where data presence is not `CONFIRMED`. For each confirmed table, executes a 5-step pipeline:

1. **Drop from HMS** if already registered (no `PURGE` — data files are preserved).
2. **Temporarily register** using the **source** metadata file: constructs the equivalent source path by swapping `dest_s3_prefix` → `source_s3_prefix` in `dest_path`, resolves the metadata file there, and calls `CALL spark_catalog.system.register_table(table, source_metadata_file)`. This is required so `rewrite_table_path` sees a `location` field that starts with `source_s3_prefix` and knows which paths to rewrite.
3. **Rewrite metadata**: `CALL spark_catalog.system.rewrite_table_path(table, source_prefix, target_prefix, staging_location => '{dest_path}/metadata')` — rewrites all `source/` path references to `dest/` across every snapshot (full history). New manifest and metadata files are written to `dest_path/metadata`. The `latest_version` filename is read from the procedure result row and used directly to construct the new metadata file path — `version-hint.text` is not consulted because it is not updated by the procedure.
4. **Drop the temporary registration**.
5. **Permanently register in HMS** via `CALL spark_catalog.system.register_table(table, metadata_file)` using `{dest_path}/metadata/{latest_version}` from the rewrite result.

The task raises after all tables are processed if any failed.

---

### Step 8 — `update_rewrite_and_register_in_tracking`

**Type:** PySpark (mapped per database config) · trigger: `all_done`

- Updates `table_create_status`, `table_create_duration_seconds`, `table_already_existed`, and `overall_status` for each table.
- Applies a catch-all update to mark any unprocessed `CONFIRMED` tables as `FAILED`.

---

### Step 9 — `validate_dest_tables`

**Type:** PySpark (mapped per database config) · trigger: `all_done` · max 3 concurrent · **@track_duration**

For each table that was successfully created:
- Queries `SELECT COUNT(*)` and `.partitions` on the destination table.
- Compares against source row count and partition count stored in the tracking table.
- Performs schema comparison between source `metadata.json` schema and `DESCRIBE` output.
- Reads the destination metadata JSON and verifies the current snapshot's `manifest-list` field references `dest_s3_prefix`, not `source_s3_prefix` (`path_rewrite_verified`).
- Partition count mismatches are treated as warnings.
- Schema, row count, and path rewrite mismatches are failures.

---

### Step 10 — `update_validation_in_tracking`

**Type:** PySpark (mapped per database config) · trigger: `all_done`

- Updates `validation_status`, `row_count_match`, `partition_count_match`, `schema_match`, `path_rewrite_verified`, `schema_differences`, and `overall_status` (`VALIDATED` or `VALIDATION_FAILED`).
- Applies catch-all updates for tables that were not processed by the validation task.

---

### Step 11 — `generate_html_report`

**Type:** PySpark · trigger: `all_done`

Generates an HTML report and writes it to `{report_output_location}/{run_id}_rewrite_report.html`.

Report sections:
1. **Migration Summary** — total/validated/failed/missing tables, source rows, destination data size
2. **Data Presence & Metadata Check** — per-table presence status, file count, size
3. **Table Migration Details** — per-table overall status, format, partitioning, and task durations
4. **Validation Results** — row count, partition count, and schema match per table

---

### Step 12 — `send_report_email`

**Type:** PySpark · trigger: `all_done`

- Reads the HTML report from S3 and sends it as an email attachment.
- Subject: `Iceberg Rewrite Migration Report — {run_id}`.
- Skips silently if `email_recipients` is not configured.
- Uses the `smtp_conn_id` Airflow connection (default: `smtp_default`).

---

### Step 13 — `finalize_run`

**Type:** PySpark · trigger: `all_done`

- Aggregates final counts from `rewrite_migration_table_status`.
- Updates `rewrite_migration_runs` with `completed_at` and final status.

**Final run statuses:**

| Status | Meaning |
|---|---|
| `COMPLETED` | All tables validated, no failures, no missing |
| `COMPLETED_WITH_MISSING` | No failures but some tables had no data at destination |
| `COMPLETED_WITH_FAILURES` | One or more tables failed at any stage |
| `FAILED` | Run-level error (tracking query failed) |

---

## Status Progression

**Per-table `overall_status`:**

```
DATA_CONFIRMED  (metadata and data files found at destination)
    ↓
DISCOVERED  (metadata read from destination, tracking record updated)
    ↓
TABLE_CREATED  (rewrite_table_path + register_table executed)
    ↓
VALIDATED  (row count, partition count, schema all match)

DATA_MISSING → skipped in all downstream steps, visible in report
(Any stage) → FAILED or VALIDATION_FAILED
```

| Status | Meaning |
|---|---|
| `DATA_CONFIRMED` | Data and metadata files present at destination S3 — initial tracking record inserted |
| `DISCOVERED` | Metadata read from destination, tracking record updated with schema/partition info |
| `DATA_MISSING` | No files or missing `metadata/` directory — skipped |
| `TABLE_CREATED` | `rewrite_table_path` + `register_table` completed, validation pending |
| `VALIDATED` | All validations passed — migration success |
| `VALIDATION_FAILED` | Row count, partition count, or schema mismatch |
| `FAILED` | Error at data presence check, discovery, or table creation |

---

## Tracking Tables

| Table | Description |
|---|---|
| `{tracking_database}.rewrite_migration_runs` | One row per DAG run — run ID, status, counts, timestamps, config snapshot |
| `{tracking_database}.rewrite_migration_table_status` | One row per table per run — discovery, data presence, table creation, and validation results; partitioned by `source_database` |

---

## DAG ID & Tags

| Property | Value |
|---|---|
| DAG ID | `iceberg_catalog_migration` |
| Tags | `migration`, `iceberg`, `rewrite-table-path` |
| Max active runs | 5 |
| Schedule | Manual (`None`) |
| Retries | 2, 5-minute delay |
