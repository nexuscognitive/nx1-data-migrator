# Source (MapR-FS/HDP-HDFS) to S3 Migration DAG

An automated **Airflow TaskFlow-based migration pipeline** consisting of four independent DAGs for orchestrating large-scale Hive table migrations from source (MapR-FS or HDP-HDFS) to S3 and converting existing tables to Iceberg format.

---

## Overview

This implementation provides four independent but complementary migration DAGs:

1. **`source_to_s3_migration`** - Migrates Hive tables from source (MapR-FS/HDP-HDFS) to S3
2. **`iceberg_migration`** - Converts existing Hive tables in S3 to Apache Iceberg format
3. **`folder_only_data_copy`** - Copies raw folders from source cluster (MapR-FS/HDP-HDFS) to S3 via DistCp — no Hive metadata
4. **`parquet_hms_registration`** - Registers plain parquet files on S3 as external Hive Metastore tables — schema inferred, partitions auto-discovered

---

## Configuration Variables

The DAGs rely on Airflow Variables for configuration. Set these before running:

### Required Variables

| Variable                      | Description                                     | Example                              | Applies To                                        |
| ----------------------------- | ----------------------------------------------- | ------------------------------------ | ------------------------------------------------- |
| `cluster_ssh_conn_id`         | Airflow SSH connection ID for cluster edge node | `cluster_edge_ssh`                   | `source_to_s3_migration`, `folder_only_data_copy` |
| `migration_default_s3_bucket` | Default S3 bucket for migrations                | `s3a://data-lake`                    | `source_to_s3_migration`                          |
| `migration_tracking_database` | Database name for tracking tables               | `migration_tracking`                 | All DAGs                                          |
| `migration_tracking_location` | S3 location for tracking tables                 | `s3a://data-lake/migration_tracking` | All DAGs                                          |
| `migration_report_location`   | S3 location for HTML reports                    | `s3a://data-lake/migration_reports`  | All DAGs                                          |
| `migration_spark_conn_id`     | Airflow Spark connection ID                     | `spark_default`                      | Currently unused                                  |

### Authentication Variables

| Variable                   | Description                                       | Required For  | Applies To                                        |
| -------------------------- | ------------------------------------------------- | ------------- | ------------------------------------------------- |
| `auth_method`              | Authentication method: `mapr`, `kinit`, or `none` | MapR/Kerberos | `source_to_s3_migration`, `folder_only_data_copy` |
| `mapr_user`                | MapR username used to validate existing ticket    | MapR auth     | `source_to_s3_migration`, `folder_only_data_copy` |
| `mapr_ticketfile_location` | MapR ticket file path                             | MapR auth     | `source_to_s3_migration`, `folder_only_data_copy` |
| `cluster_type`             | Display label for reports: `MapR`, `HDP`, etc.    | HTML reports  | `source_to_s3_migration`                          |

### Optional Variables

| Variable                           | Default          | Description                                                                                                                                     | Applies To                                        |
| ---------------------------------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `cluster_edge_temp_path`           | `/tmp/migration` | Temporary directory on edge node                                                                                                                | `source_to_s3_migration`, `folder_only_data_copy` |
| `cluster_hive_scratch_dir`         | `/tmp/hive`      | Hive scratch dir for PySpark sessions started on the **source edge node**. Literal path only (no `${USER}`).                                    | `source_to_s3_migration`                          |
| `migration_tenant_profiles`        | `{}`             | Per-tenant config groups as JSON, selected at trigger time via the `tenant` DAG param                                                           | `source_to_s3_migration`, `folder_only_data_copy` |
| `s3_endpoint`                      | _(empty)_        | Default S3 endpoint URL (all buckets)                                                                                                           | `source_to_s3_migration`, `folder_only_data_copy` |
| `s3_access_key`                    | _(empty)_        | Default S3 access key (all buckets)                                                                                                             | `source_to_s3_migration`, `folder_only_data_copy` |
| `s3_secret_key`                    | _(empty)_        | Default S3 secret key (all buckets)                                                                                                             | `source_to_s3_migration`, `folder_only_data_copy` |
| `migration_distcp_mappers`         | `50`             | Number of DistCp mappers                                                                                                                        | `source_to_s3_migration`, `folder_only_data_copy` |
| `migration_distcp_bandwidth`       | `100`            | Bandwidth limit per mapper (MB/s)                                                                                                               | `source_to_s3_migration`, `folder_only_data_copy` |
| `migration_distcp_preserve_delete` | `true`           | DistCp delete-preservation mode for partition-filtered copies (see [DistCp partition copy modes](#distcp-partition-copy-modes))                 | `source_to_s3_migration`                          |
| `migration_include_db_in_path`     | `true`           | When `true` (default), destination S3 path is `{bucket}/{database}/{table}`. When `false`, path is `{bucket}/{table}` (database folder omitted) | `source_to_s3_migration`                          |
| `s3_listing_tool`                  | `hadoop`         | Tool for S3 listing: `hadoop` or `boto3`                                                                                                        | Currently unused                                  |
| `migration_smtp_conn_id`           | `smtp_default`   | Airflow SMTP connection ID for email reports                                                                                                    | All DAGs                                          |
| `migration_email_recipients`       | _(empty)_        | Comma-separated email addresses for reports                                                                                                     | All DAGs                                          |
| `hdfs_nameservice`                 | _(empty)_        | HDFS HA nameservice (e.g. `mycluster`); leave empty for MapR                                                                                    | `source_to_s3_migration`, `folder_only_data_copy` |

#### Hive scratch dir on the edge node

`hive-site.xml` on the edge node points `hive.exec.scratchdir` at the login
user's own directory. That works when the DAG runs as the MapR superuser or DAG
owner, but fails for a **tenant service account**, which has no write access
there — discovery aborts in `validate_prerequisites` with:

    AnalysisException: ... does not have access to /user/<dag-owner>/...

`cluster_hive_scratch_dir` is applied as `spark.hadoop.hive.exec.scratchdir` and
`spark.hadoop.hive.exec.local.scratchdir` on every Spark session the DAG starts
over SSH on the source edge node (`validate_prerequisites` and
`discover_tables_via_spark_ssh`), overriding whatever `hive-site.xml` sets.

The default `/tmp/hive` is Hive's own default scratch root. Hive creates a per-user
subdirectory beneath it (`/tmp/hive/<user>`), so each tenant service account gets
its own writable space without further setup. Only override this if `/tmp` is
restricted on your cluster.

Placeholders are **not** supported. The value is baked into a Python script that
is written over SFTP, which does not expand shell variables, so a `${USER}` here
would be created as a literal directory of that name. Use a literal path.

#### Per-run configuration via tenant profiles

The edge node, service account and temp paths differ per tenant. Rather than
editing the global Variables between runs — which repoints any run already in
flight, and makes it impossible to migrate two tenants at once — keep one
Variable holding all tenants and pick one at trigger time.

**Airflow Variable `migration_tenant_profiles`** — a JSON object keyed by
tenant or environment name:

```json
{
  "tenant_1": {
    "ssh_conn_id": "cluster_edge_ssh_tenant_1",
    "mapr_user": "mapr_tenant_1",
    "edge_temp_path": "/user/svc_tenant_1/tmp/migration"
  },
  "tenant_2": {
    "ssh_conn_id": "cluster_edge_ssh_tenant_2",
    "mapr_user": "mapr_tenant_2"
  }
}
```

**Trigger with just the tenant name:**

```json
{
  "excel_file_path": "s3a://config-bucket/tenant_1.xlsx",
  "tenant": "tenant_1"
}
```

Only include the keys that actually differ per tenant. Anything omitted falls
through to the global Variable, so `tenant_2` above uses
`cluster_edge_temp_path` as normal.

Accepted keys in a profile: `ssh_conn_id`, `mapr_user`,
`mapr_ticketfile_location`, `auth_method`, `edge_temp_path`,
`hive_scratch_dir`.

**Resolution order** for these settings:

1. Key supplied directly in the trigger config (e.g. `{"ssh_conn_id": "..."}`)
2. Tenant profile, when `tenant` is set
3. Run-scoped Variable `<variable_name>__<run_id>`
4. Global Variable
5. Environment variable
6. Built-in default

An unknown `tenant`, malformed JSON, or an unrecognised key inside a profile
**fails the run** rather than falling back to the global Variables — a typo
must not silently migrate a tenant against another tenant's edge node. The
resolved tenant and connection are logged by every task on the `[get_config]`
line, and the full config is recorded in the `migration_runs` tracking row.

Leaving `tenant` blank resolves exactly as before, so single-tenant setups need
no change.

### Multi-Tenant S3 Credentials (endpoint-based overrides)

When destination buckets live on **different S3 tenant managers** you can route
each row to the correct tenant by adding an `endpoint` column to the Excel
configuration file. Rows without an `endpoint` value continue to use the global
`s3_access_key` / `s3_secret_key` / `s3_endpoint` Variables.

#### How it works

For each Excel row that has a non-empty `endpoint` value:

1. The endpoint URL is used directly as the Hadoop S3A endpoint for that destination bucket.
2. Credentials are looked up by the **hostname** of that endpoint URL, with `_access_key` and `_secret_key` suffixes.

| Airflow Variable (set as masked) | Env var equivalent         | Description                          |
| -------------------------------- | -------------------------- | ------------------------------------ |
| `<ep-hostname>_access_key`       | `<EP_HOSTNAME>_ACCESS_KEY` | Access key for the endpoint (masked) |
| `<ep-hostname>_secret_key`       | `<EP_HOSTNAME>_SECRET_KEY` | Secret key for the endpoint (masked) |

The hostname slug is derived from the `endpoint` value: dots and hyphens become underscores for the env-var form.

**Example** — Excel row has `endpoint = https://s3.tenant-a.example.com`:

```
Airflow Variables:
  s3.tenant-a.example.com_access_key  →  AKIA...
  s3.tenant-a.example.com_secret_key  →  abc123...

Equivalent env vars (fallback):
  S3_TENANT_A_EXAMPLE_COM_ACCESS_KEY=AKIA...
  S3_TENANT_A_EXAMPLE_COM_SECRET_KEY=abc123...
```

#### Credential resolution order per row

1. **Endpoint provided in Excel** → credentials looked up via `<ep-hostname>_access_key` / `_secret_key` Variable (or env var), endpoint used as-is
2. **No endpoint in Excel** → global `s3_access_key` / `s3_secret_key` / `s3_endpoint` from Airflow Variables
3. Hadoop's own credential chain (e.g. IAM instance role) as final fallback

Rows without an `endpoint` value are unaffected — no changes required for
single-tenant setups.

> **Same bucket name on two tenants:** Because credential lookup is keyed on
> the _endpoint hostname_ (not the bucket name), two buckets both named
> `data-lake` on different tenants are fully supported as long as each row in the
> Excel file has the correct `endpoint` value.

> **Security note:** Create `_access_key` and `_secret_key` Variables with
> **"Mask Variable value"** checked in the Airflow UI so secrets are never
> exposed in task logs.

---

## DAG Parameter Details

| DAG   | Parameter         | Required | Description                   | Example                                      |
| ----- | ----------------- | -------- | ----------------------------- | -------------------------------------------- |
| DAG 1 | `excel_file_path` | Yes      | S3 path to Excel config       | `s3a://config-bucket/migration.xlsx`         |
| DAG 1 | `tenant`          | No       | Tenant profile key (optional) | `tenant_1`                                   |
| DAG 2 | `excel_file_path` | Yes      | S3 path to Iceberg config     | `s3a://config-bucket/iceberg_migration.xlsx` |
| DAG 3 | `excel_file_path` | Yes      | S3 path to folder copy config | `s3a://config-bucket/folder_copy.xlsx`       |
| DAG 3 | `tenant`          | No       | Tenant profile key (optional) | `tenant_1`                                   |
| DAG 4 | `excel_file_path` | Yes      | S3 path to parquet HMS config | `s3a://config-bucket/parquet_hms.xlsx`       |

---

## Key Features of all DAGs

- **Parallel Processing** - Dynamic task mapping for concurrent migrations
- **Comprehensive Tracking** - All operations tracked in Iceberg tables with detailed metrics
- **Incremental Support** - Resume and update existing migrations
- **Error Recovery** - Per-table error handling with detailed tracking
- **Duration Tracking** - Automatic tracking of task execution times via XCom decorator

---

## Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────┐
│ DAG 1: Source (MapR-FS/HDP-HDFS) to S3                              │
│                                                             │
│ Source MapR-FS/HDP-HDFS (Hive Tables)                               │
│ │                                                           │
│ │ [PySpark: Metadata Discovery]                             │
│ ▼                                                           │
│ Metadata Discovery                                          │
│ │                                                           │
│ │ [SSH: DistCp - 24h timeout]                               │
│ ▼                                                           │
│ S3 (Raw Data Files)                                         │
│ │                                                           │
│ │ [PySpark: Hive DDL]                                       │
│ ▼                                                           │
│ S3 (Queryable via Hive)                                     │
│ │                                                           │
│ │ [Validation: Row counts, partitions, schema]              │
│ ▼                                                           │
│ Validated & Tracked                                         │
└─────────────────────────────────────────────────────────────┘
│
│ (Independent, typically run after)
▼
┌─────────────────────────────────────────────────────────────┐
│ DAG 2: Iceberg Migration                                    │
│                                                             │
│ S3 (Hive Tables)                                            │
│ │                                                           │
│ │ [PySpark: Table Discovery]                                │
│ ▼                                                           │
│ Hive Metadata Discovery                                     │
│ │                                                           │
│ │ [Spark Procedures: migrate/snapshot]                      │
│ ▼                                                           │
│ S3 (Iceberg Format)                                         │
│ │                                                           │
│ │ [Validation: Row counts, partitions, schema]              │
│ ▼                                                           │
│ Validated & Tracked                                         │
└─────────────────────────────────────────────────────────────┘
```

---

### Migration Strategy Decision Tree

```
Do you need to migrate from source cluster (MapR-FS/HDFS) to S3?
│
├─ YES → Run DAG 1 (source_to_s3_migration)
│ │
│ │
│ └─ Need Iceberg format?
│    │
│    └─ YES → Run DAG 2 (iceberg_migration)
│       │
│       └─ No Hive, Only Iceberg → Inline migration
│       │
│       └─ Both Hive and Iceberg → Snapshot migration
│
└─ NO → Already in S3, need Iceberg?
   │
   └─ YES → Run DAG 2 (iceberg_migration) only
```

---

## DAG 1: Source Cluster to S3 Migration

### Purpose

Orchestrates the complete migration of Hive tables from a source cluster (MapR-FS or HDP/HDFS) to S3, including data transfer, metadata recreation, and validation.

---

### Key Features

- **SSH Operations** - All source (MapR/HDP) cluster interactions via SSH to edge node
- **Beeline Discovery** - Automated metadata extraction using HiveServer2
- **Hadoop DistCp** - Efficient bulk data transfer with 24-hour timeout
- **Incremental Support** - Automatic detection and `update` flag usage
- **Partition Support** - Automatic partition discovery and repair
- **Format Preservation** - Supports Parquet, ORC, and Avro
- **Comprehensive Validation** - Row counts, partition counts, schema comparison

---

### Duration Tracking

Tasks decorated with `@track_duration` automatically capture execution time:

- **Mechanism**: Decorator wraps task function and measures start/end time
- **Storage**: Adds `_task_duration` field to task result dictionary
- **XCom**: Duration flows through task dependencies via XCom
- **Tracking**: Saved to tracking tables in `*_duration_seconds` columns

**Tracked tasks:**

- `discover_tables_via_spark_ssh` → `discovery_duration_seconds`
- `run_distcp_ssh` → `distcp_duration_seconds`
- `create_hive_tables` → `table_create_duration_seconds`
- `validate_destination_tables` → `validation_duration_seconds`

---

### Excel Configuration Format

**Required Columns:**

| Column             | Required | Description                                                                                                                                                                                         | Example                                  |
| ------------------ | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `database`         | **Yes**  | Source database name                                                                                                                                                                                | `sales_data`                             |
| `table`            | No       | Table pattern: supports \* wildcards, comma-separated table names (e.g. table1,table2), or one table per row for same database (rows are combined internally)                                       | `transactions_*` or `*`                  |
| `dest database`    | No       | Destination database (defaults to source)                                                                                                                                                           | `sales_data_s3`                          |
| `bucket`           | No       | S3 bucket (defaults to variable)                                                                                                                                                                    | `s3a://data-lake`                        |
| `endpoint`         | No       | S3 endpoint URL for a non-default tenant; credentials resolved via `<hostname>_access_key/secret_key` Variables (see Multi-Tenant section)                                                          | `https://s3.tenant-a.example.com`        |
| `partition_filter` | No       | Migrate only specific partitions. Comma-separated expressions. Supports: exact match (`year=2023/month=01`), prefix wildcard (`year=2023/*`), comparison (`year>=2022`), and `last_n_partitions=N`. | `year=2023/month=01, year=2023/month=02` |

---

### Task Flow

```
validate_prerequisites (SSH: connectivity, cluster auth, PySpark+Hive, Hadoop FS checks)
↓
init_tracking_tables
↓
create_migration_run
↓
parse_excel
↓
cluster_login_setup (SSH: cluster authentication)
↓
┌───────────────────────────────────────────────┐
│ Dynamic Task Mapping (per database config)    │
│                                               │
│ discover_tables_via_spark_ssh (SSH: PySpark)  │
│ ↓                                             │
│ record_discovered_tables                      │
│ ↓                                             │
│ run_distcp_ssh (SSH: DistCp, 24h timeout)     │
│ ↓                                             │
│ update_distcp_status                          │
│ ↓                                             │
│ create_hive_tables (PySpark: DDL/Repair)      │
│ ↓                                             │
│ update_table_create_status   │
│ ↓                                             │
│ validate_destination_tables (PySpark)         │
│ ↓                                             │
│ update_validation_status                      │
└───────────────────────────────────────────────┘
↓
generate_html_report
↓
send_migration_report_email (PySpark: Email report)
↓
finalize_run
↓
cleanup_edge (SSH: Cleanup temp files)
```

---

### Task Summaries

#### Step 0.1 - `validate_prerequisites`

**Type:** SSH  
**Purpose:** Validate all required components are available before starting migration

- Connects to the cluster edge node via SSH
- Runs four sequential checks:
  1. **SSH Connectivity** - Verifies SSH connection works with a simple echo command
  2. **Cluster Authentication** - Verifies a valid ticket/TGT exists before attempting any cluster operations:
     - `mapr`: `maprlogin print | grep -q <mapr_user>` — confirms a valid MapR ticket for the configured user
     - `kinit`: `klist -s` — confirms a valid Kerberos TGT in the ccache (populated by the login shell)
     - `none`: skipped (auto-passes)
  3. **PySpark + Hive Metastore** - Starts a real `SparkSession` with `enableHiveSupport()` and runs `SHOW DATABASES`. The Hive scratch dir is overridden to `cluster_hive_scratch_dir` so the check passes under a tenant service account
  4. **Hadoop FS** - Runs `hadoop fs -ls /` (MapR) or `hadoop fs -ls hdfs://<nameservice>/` (HDFS HA) to confirm filesystem access
- Sources `/etc/profile.d/*.sh` via a bash login shell before each check to ensure cluster auth and environment variables are loaded
- If **all four checks pass**, proceeds with migration
- If **any check fails**, raises an exception with a detailed summary of which checks failed and why, halting the DAG before any tracking tables or run records are created

---

#### Step 0.2 - `init_tracking_tables`

**Type:** PySpark  
**Purpose:** Initialize the migration tracking infrastructure

- Creates the `migration_tracking` database if it doesn't exist
- Creates two Iceberg tables for tracking:
  - `migration_runs` - Run-level metadata (run ID, status, counts, timestamps)
  - `migration_table_status` - Table-level tracking (discovery, DistCp, table creation)
- Ensures tracking tables persist across all migration runs

---

#### Step 1 - `create_migration_run`

**Type:** PySpark  
**Purpose:** Generate unique run identifier and initialize run record

- Creates a unique run ID with timestamp and UUID
- Inserts initial record into `migration_runs` table with status `RUNNING`
- Stores DAG configuration snapshot for audit trail
- Returns run ID for use in downstream tasks

---

#### Step 2 - `parse_excel`

**Type:** PySpark  
**Purpose:** Read and parse Excel configuration file from S3

- Reads Excel file from S3 using `pyspark.pandas.read_excel`
- Normalizes column names (lowercase, strip whitespace, replace spaces with underscores)
- Validates and defaults configuration values:
  - `dest_database` defaults to source database name
  - `bucket` defaults to `migration_default_s3_bucket` variable
  - `table` pattern defaults to `*` (all tables); supports comma-separated table names and multi-row input for the same database (rows are combined into a single database record internally)
  - `partition_filter` is optional; if set, only the matching partitions are migrated for that row's table(s)
- Expands to list of database configurations for dynamic task mapping
- Filters out rows with empty database names

---

#### Step 3 - `cluster_login_setup`

**Type:** SSH  
**Purpose:** Authenticate to the source cluster (MapR or Kerberos) and prepare edge node environment

- Connects to the cluster edge node via SSH
- Authenticates using one of the following methods, based on configuration:
  1. **Kerberos authentication** - Assumes a valid Kerberos ticket sourced via the login shell
  2. **Existing MapR ticket** - Validates existing valid ticket with `maprlogin print`
- Verifies ticket validity with `maprlogin print` or `klist`
- Creates temporary working directory on edge node (`/tmp/migration/{run_id}`)
- Ensures all subsequent SSH operations can access the source filesystem

---

#### Step 4 - `discover_tables_via_spark_ssh`

**Type:** SSH
**Purpose:** Discover table metadata from Hive using PySpark

- Executes on edge node via SSH on each database in Excel config
- Starts the Spark session with the Hive scratch dir set to `cluster_hive_scratch_dir`
- Discovers tables matching the pattern (supports `*` wildcards)
- For each table, extracts:
  - **Schema** - Column names and data types
  - **Location** - Source filesystem path (MapR-FS or HDFS)
  - **Format** - Parquet, ORC, or Avro (detected from InputFormat)
  - **Partitions** - Partition spec and count (via `DESCRIBE FORMATTED`)
  - **Partition columns** - Extracted from table metadata
  - **Partition filter** - If `partition_filter` is set in the Excel config, only matching partitions are included; full-table row and partition counts are also captured as a baseline
- Generates JSON output with all discovered metadata
- Determines S3 destination path: `{bucket}/{dest_database}/{table_name}` (default) or `{bucket}/{table_name}` when `migration_include_db_in_path=false`

---

#### Step 5 - `record_discovered_tables`

**Type:** PySpark (mapped per database)  
**Purpose:** Persist discovered table metadata in Iceberg tracking table

- Inserts or updates records in `migration_table_status` for each discovered table
- Uses `MERGE` statement to handle both new discoveries and re-runs
- Stores comprehensive metadata: schema JSON, partition list, file format, location
- Sets initial status to `DISCOVERED`
- Enables downstream tasks to access table metadata without re-querying Hive

---

#### Step 6 - `run_distcp_ssh`

**Type:** SSH (mapped per database)  
**Purpose:** Copy data from MapR-FS/HDFS to S3 using Hadoop DistCp

- Executes DistCp via SSH for each table discovered in previous step
- **Incremental detection:**
  - Checks if S3 destination already exists using `hadoop fs -test -d`
  - If exists, runs `hadoop distcp -update` (incremental sync)
  - If new, runs full copy
- **DistCp configuration:**
  - Configurable mapper count (default: 50)
  - Bandwidth limit per mapper (default: 100 MB/s)
  - Dynamic strategy for load balancing
  - S3 credentials passed via `-D` properties
- **Partition-filtered copy:** When a `partition_filter` is active, only the matched partitions are copied instead of the whole table root. The copy mode is controlled by `migration_distcp_preserve_delete` (see below).
- Captures success/failure status per table
- **File metrics tracking:**
  - Calculates S3 metrics BEFORE DistCp: file count and total size
  - Calculates S3 metrics AFTER DistCp: file count and total size
  - Computes transferred bytes and files (delta between before/after)
  - Compares with source MapR/HDFS metrics:
    - `file_size_match`: True if within 1% tolerance
    - `file_count_match`: True if exact match
  - These metrics help detect incomplete copies even when DistCp reports success
- Logs written to `{temp_dir}/distcp_{run_id}_{src_db}.log`
- **Timeout:** 24 hours per table (configurable via `SSH_COMMAND_TIMEOUT`)

##### DistCp partition copy modes

When `partition_filter` is active, the `migration_distcp_preserve_delete` variable controls how filtered partitions are copied. For full-table copies (no `partition_filter`), this setting has no effect — DistCp always uses `-update -delete` against the table root.

| `preserve_delete` | Strategy                          | Flags             | Speed           | Cleans up stale files? |
| ----------------- | --------------------------------- | ----------------- | --------------- | ---------------------- |
| `true` (default)  | One DistCp per partition          | `-update -delete` | Slower (N jobs) | Yes                    |
| `false`           | Single DistCp with `-f` path-list | `-update` only    | Faster (1 job)  | No                     |

**Note:** Single DistCp with -f having multiple partition paths to a common destination + -delete option, lead to a exit code 25 - because the destination folder already exists after the first partition is copied. Hence this is an infeasible option.

**Important:** The `migration_distcp_preserve_delete` setting only affects partition-filtered copies (when `partition_filter` is set in the Excel config). For full-table (bulk) loads, this setting has no effect—DistCp always uses `-update -delete` against the table root.

**How to choose the setting for partition-filtered runs:**

- Use `true` (default) for incremental re-syncs of mutable data, or when you want to remove any files in S3 that no longer exist in the source partitions.
- Use `false` for one-time or phased loads where you do not need to clean up orphans immediately (e.g., append-only data, phased migration steps). You can always run a full-table copy later to clean up.

If unsure, leave the default (`true`).

##### Partition filter: safe run sequences

All run sequences are safe — no combination corrupts data:

- **Partition-filtered → Full table** — full run copies everything and cleans up with `-delete`
- **Full table → Partition-filtered** — only named partitions are re-synced, rest stays untouched
- **`preserve_delete=false` → `true` or full table** — next run cleans up any orphans
- **Different partition filters across runs** — each operates on its own partitions

**What to watch for:**

- `preserve_delete=false` with repeated re-syncs of mutable partitions will accumulate orphaned files. Switch to `true` or do a full-table run to clean up.
- A partition-filtered run only touches the named partitions. To remove partitions dropped at source, run a full-table copy.

---

#### Step 7 - `update_distcp_status`

**Type:** PySpark (mapped per database)  
**Purpose:** Update tracking table with DistCp results

- Updates `migration_table_status` for each table with:
  - `distcp_status` - COMPLETED or FAILED
  - `distcp_completed_at` - Timestamp
  - `distcp_is_incremental` - Boolean flag
  - `overall_status` - Updated to COPIED or FAILED
  - `error_message` - Error details if failed (truncated to 2000 chars)
- Enables monitoring of data copy progress
- Allows restart of failed tables in subsequent runs

---

#### Step 8 - `create_hive_tables`

**Type:** PySpark (mapped per database)  
**Purpose:** Create or repair Hive external tables pointing to S3 data

- Creates destination database if it doesn't exist
- For each table:
  - **If table doesn't exist:** Creates new external Hive table
    - Infers schema from discovered metadata or S3 files
    - Applies partition columns if table is partitioned
    - Sets location to S3 path
    - Uses correct file format (Parquet/ORC/Avro)
  - **If table exists (incremental run):** Runs `MSCK REPAIR TABLE`
    - Discovers new partitions added since last run
    - Updates Hive metastore without recreating table
- Handles both partitioned and non-partitioned tables
- Generates proper DDL with escaped column names and types

---

#### Step 9 - `update_table_create_status`

**Type:** PySpark (mapped per database)  
**Purpose:** Update tracking table with table creation results

- Updates `migration_table_status` for each table with:
  - `table_create_status` - COMPLETED, FAILED, or SKIPPED
  - `table_create_completed_at` - Timestamp
  - `table_already_existed` - Boolean flag
  - `overall_status` - Updated to TABLE_CREATED or FAILED
  - `error_message` - Error details if failed
- Tracks whether table was newly created or repaired
- Enables visibility into table creation/repair operations

---

#### Step 10 - `validate_destination_tables`

**Type:** PySpark (mapped per database)  
**Purpose:** Validate destination Hive tables: row counts, partition counts, schema comparison

- For each table:
  - Check if source validation succeeded
  - Get destination row count
  - Get destination partition count
  - Perform schema comparison
  - Perform validation checks

---

#### Step 11 - `update_validation_status`

**Type:** PySpark (mapped per database)  
**Purpose:** Update Iceberg tracking with validation results

- For each table in validation results:
  - Skip if validation not completed
  - Escape and truncates error/schema differences
- Update tracking table
- Determine final overall_status

**Final status meanings:**

- DISCOVERED: Metadata extracted, not yet copied
- COPIED: Data copied to S3, table not yet created
- TABLE_CREATED: Hive table created/repaired, not yet validated
- VALIDATED: All validations passed - MIGRATION SUCCESS
- VALIDATION_FAILED: One or more validations failed
- FAILED: DistCp or table creation failed

---

#### Step 12 - `generate_html_report`

**Type:** PySpark
**Purpose:** Generate comprehensive HTML migration report and prepare for email delivery

- Queries tracking tables for run info and table status
- **Generates HTML report with comprehensive sections:**
  1. **Migration Summary** - Total/successful/failed tables, data volume, file counts, incremental runs
  2. **Validation Summary** - Tables validated, passed/failed counts, mismatch breakdowns
  3. **Table Migration Details** - Per-table status, active partition filter (with matched partition count), durations for discovery/DistCp/creation/validation
  4. **Metadata Validation Results** - Row count comparison, partition comparison, schema comparison
  5. **Data Validation Results** - File size comparison (MapR vs S3), file count comparison
  6. **Performance Metrics** - Data volume, DistCp speed (MB/s), rows/second, end-to-end duration
- Writes HTML report to S3 at `{report_location}/{run_id}_report.html`
- **Returns both:**
  - `report_path` - S3 location for audit/archival
  - `html_content` - Full HTML string for direct email delivery

---

#### Step 13 - `send_migration_report_email`

**Type:** PySpark  
**Purpose:** Send HTML migration report via email using SMTP

- Receives HTML content directly from `generate_html_report` task
- Extracts email configuration:
  - SMTP connection ID from Airflow variable
  - Recipients list (comma-separated) from Airflow variable
- Sends email with:
  - Subject: `Migration Report - {run_id}`
  - Body: Full HTML report (no S3 read required)
- **Skips email if:**
  - No recipients configured (`migration_email_recipients` variable empty)
  - Returns `{'sent': False, 'reason': 'no_recipients'}`
- Logs delivery status and recipient list
- Returns result with `sent` status, `recipients`, and `report_path`

---

#### Step 14 - `finalize_run`

**Type:** PySpark  
**Purpose:** Aggregate statistics and mark migration run as complete

- Queries `migration_table_status` to calculate:
  - Total tables processed
  - Successful tables (not in FAILED/PENDING states)
  - Failed tables
- Updates `migration_runs` table with:
  - `status` = COMPLETED
  - `completed_at` = Current timestamp
  - Final counts
- Provides summary metrics for the entire migration run

---

#### Step 15 - `cleanup_edge`

**Type:** SSH  
**Purpose:** Clean up temporary files on MapR edge node

- Removes temporary directory created in `cluster_login_setup`
- Cleans up DistCp log files
- Ensures edge node disk space is freed
- Failures are ignored

---

### Status Progression

```
DISCOVERED
    ↓
COPIED (DistCp successful)
    ↓
TABLE_CREATED (Hive table created/repaired)
    ↓
VALIDATED (All validations passed)

(Any stage can fail → FAILED)
```

---

## DAG 2: Iceberg Migration

### Purpose

Converts existing Hive tables in S3 to Apache Iceberg format using Spark procedures, with comprehensive validation and parent run tracking.

---

### Key Features

- **Two Migration Strategies:**
  - **In-place**: Convert existing Hive table to Iceberg (overwrites metadata)
  - **Snapshot**: Create separate Iceberg table alongside Hive table
- **Parent Run Tracking** - Links back to original MapR-to-S3 migration
- **Comprehensive Validation** - Row counts, partition counts, schema comparison
- **HTML Reporting** - Detailed migration and validation reports

---

### Duration Tracking

Tasks decorated with `@track_duration` automatically capture execution time:

- **Mechanism**: Decorator wraps task function and measures start/end time
- **Storage**: Adds `_task_duration` field to task result dictionary
- **XCom**: Duration flows through task dependencies via XCom
- **Tracking**: Saved to tracking tables in `*_duration_seconds` columns

**Tracked tasks:**

- `migrate_tables_to_iceberg` → `migration_duration_seconds`
- `validate_iceberg_tables` → `validation_duration_seconds`

---

### Migration Strategies

### Inplace Migration

#### What it does

- Converts existing Hive table to Iceberg format
- Uses Spark procedure: CALL spark_catalog.system.migrate('{table}')
- Overwrites table metadata - table becomes Iceberg table
- Original Hive table is lost (irreversible)

---

#### Characteristics

- Database name: Same as source
- Table name: Same as source
- Location: Same as source (metadata changes only)
- Storage: No data duplication
- Queries: Must use Iceberg-compatible engine

---

### Snapshot Migration

#### What it does

- Creates new Iceberg table alongside existing Hive table
- Uses Spark procedure: CALL spark_catalog.system.snapshot('{source}', '{dest}')
- Preserves original Hive table - both tables exist
- Creates separate Iceberg table with snapshot of data

---

### Characteristics

- Database name: Configurable (defaults to {source}\_iceberg)
- Table name: Same as source
- Location: Same as source (metadata layer only)
- Storage: Minimal duplication (metadata only)
- Queries: Can query both Hive and Iceberg versions

---

### Excel Configuration Format

**Required Columns:**

| Column                         | Required | Description                                       | Example                 |
| ------------------------------ | -------- | ------------------------------------------------- | ----------------------- |
| `database`                     | **Yes**  | Source database name                              | `sales_data_s3`         |
| `table`                        | No       | Table pattern (supports `*` wildcards)            | `transactions_*` or `*` |
| `inplace_migration`            | No       | `T`/`True` for in-place, `F`/`False` for snapshot | `F`                     |
| `destination_iceberg_database` | No       | Destination database (defaults based on strategy) | `sales_data_iceberg`    |

---

**Default Behavior:**

- If `inplace_migration = True`: Database remains the same and metadata migrates to Iceberg
- If `inplace_migration = False`: Database defaults to `{source_database}_iceberg` and creates seperate Iceberg metadata table

---

### Task Flow

```
init_iceberg_tracking_tables
    ↓
create_iceberg_migration_run
    ↓
parse_iceberg_excel
    ↓
lookup_parent_migration_run (Links to DAG 1)
    ↓
update_parent_run_id
    ↓
┌───────────────────────────────────────────────┐
│  Dynamic Task Mapping (per database config)   │
│                                               │
│  discover_hive_tables (PySpark)               │
│    ↓                                          │
│  migrate_tables_to_iceberg (PySpark)          │
│    ↓                                          │
│  update_migration_durations                   │
│    ↓                                          │
│  validate_iceberg_tables (PySpark)            │
│    ↓                                          │
│  update_iceberg_validation_status             │
└───────────────────────────────────────────────┘
    ↓
generate_iceberg_html_report
    ↓
send_iceberg_report_email (PySpark: Email report)
    ↓
finalize_iceberg_run
```

---

### Task Summaries

#### Step 0 - `init_iceberg_tracking_tables`

**Type:** PySpark  
**Purpose:** Initialize Iceberg migration tracking infrastructure

- Creates the `migration_tracking` database if it doesn't exist
- Creates two Iceberg tables for tracking:
  - `iceberg_migration_runs` - Run-level metadata (run ID, status, counts, timestamps)
  - `iceberg_migration_table_status` - Table-level tracking
- Ensures tracking tables persist across all iceberg migration runs

---

#### Step 1 - `create_iceberg_migration_run`

**Type:** PySpark  
**Purpose:** Generate unique run identifier and initialize run record

- Creates a unique run ID with timestamp and UUID
- Inserts initial record into `iceberg_migration_runs` table with status `RUNNING`
- Stores DAG configuration snapshot for audit trail
- Returns run ID for use in downstream tasks

---

#### Step 2 - `parse_iceberg_excel`

**Type:** PySpark  
**Purpose:** Read and parse Excel configuration file from S3 for Iceberg migration

- Reads Excel file from S3 using `pyspark.pandas.read_excel`
- Normalizes column names (lowercase, strip whitespace, replace spaces with underscores)
- Validates and defaults configuration values:
  - `destination_iceberg_database` defaults to <database_iceberg>
  - `inplace_migration` defaults to `False`
- Expands to list of database configurations for dynamic task mapping
- Filters out rows with empty database names

---

#### Step 3 - `lookup_parent_migration_run`

**Type:** PySpark
**Purpose:** Find parent MapR-to-S3 migration run ID by querying DAG 1 tracking tables

- Expands table patterns to get concrete table names
- For each table, queries DAG 1 tracking
  1. Finds most recent successful MapR-to-S3 migration for this table
  2. Only considers migrations that reached TABLE_CREATED or COPIED status
  3. Creates mapping: {database.table: parent_run_id}
- Determines most common parent run ID
- Returns lookup result

---

#### Step 4 - `update_parent_run_id`

**Type:** PySpark
**Purpose:** Update iceberg_migration_runs table with parent run link

- Extracts parent run ID from lookup result
- Updates run record if parent found

---

#### Step 5 - `discover_hive_tables`

**Type:** PySpark (mapped per database)  
**Purpose:** Discover Hive tables matching pattern in the source database

- Lists all tables in source database
- Filters tables by pattern
- For each matched table, gets location
- Returns discovery result

---

#### Step 6 - `migrate_tables_to_iceberg`

**Type:** PySpark (mapped per database)
**Purpose:** Migrate Hive tables to Iceberg format using Spark procedures

- Creates destination database if needed (snapshot mode only)
- For each discovered table
  - Gets Hive table row count and partition count (baseline)
  - Executes appropriate Spark procedure:
    A. **Inplace Migration:**
    - Converts Hive table to Iceberg in-place
    - Same database and table name
    - Overwrites table metadata (irreversible)
    - Table type changes from Hive external to Iceberg
      B. **Snapshot Migration:**
    - Creates new Iceberg table
    - Different database, same table name
    - Preserves original Hive table
    - Both tables point to same data location
- Gets Iceberg table row count and partition count (validation)
- Validate count and return migration results

---

#### Step 7 - `update_migration_durations`

**Type:** PySpark (mapped per database)  
**Purpose:** Update tracking table with migration durations extracted from XCom

- Extracts migration duration from @track_duration decorator
- Updates all records for this run:

---

#### Step 8 - `validate_iceberg_tables`

**Type:** PySpark (mapped per database)  
**Purpose:** Validate Iceberg tables: comprehensive Hive vs Iceberg comparison

- For each table:
  - Check if migration succeeded
  - Get destination row count
  - Get destination partition count
  - Perform schema comparison
  - Perform validation checks

---

#### Step 9 - `update_iceberg_validation_status`

**Type:** PySpark (mapped per database)  
**Purpose:** Update Iceberg tracking with validation results

- For each table in validation results:
  - Skip if validation not completed
  - Escape and truncates error/schema differences
- Update tracking table
- Determine final overall_status

**Final status meanings:**

- COMPLETED: Iceberg migration procedure executed successfully
- VALIDATED: All validations passed (row counts, partition counts, schema) - MIGRATION SUCCESS
- VALIDATION_FAILED: One or more validations failed
- FAILED: Iceberg migration procedure failed

---

#### Step 10 - `generate_iceberg_html_report`

**Type:** PySpark
**Purpose:** Generate comprehensive HTML migration report

- Queries tracking tables
- **Generates HTML report with comprehensive sections:**
  1. **Migration Summary** - Total/successful/failed tables, row counts, incremental runs
  2. **Table Migration Details** - Per-table status, durations for migration/validation
  3. **Validation Results (Hive vs Iceberg)** - Row count comparison, partition comparison, schema comparison
  4. **Performance Metrics** - Rows migrated, Migration speed (MB/s), rows/second, end-to-end duration
- Writes HTML report to S3 at `{report_location}/{run_id}_iceberg_report.html`
- **Returns both:**
  - `report_path` - S3 location for audit/archival
  - `html_content` - Full HTML string for direct email delivery

---

#### Step 11 - `send_iceberg_report_email`

**Type:** PySpark  
**Purpose:** Send HTML Iceberg migration report via email using SMTP

- Receives HTML content directly from `generate_iceberg_html_report` task
- Extracts email configuration:
  - SMTP connection ID from Airflow variable
  - Recipients list (comma-separated) from Airflow variable
- Sends email with:
  - Subject: `Iceberg Migration Report - {run_id}`
  - Body: Full HTML report (no S3 read required)
- **Skips email if:**
  - No recipients configured (`migration_email_recipients` variable empty)
  - Returns `{'sent': False, 'reason': 'no_recipients'}`
- Logs delivery status and recipient list
- Returns result with `sent` status, `recipients`, and `report_path`

---

#### Step 12 - `finalize_iceberg_run`

**Type:** PySpark  
**Purpose:** Aggregate statistics and mark migration run as complete

- Queries `iceberg_migration_table_status` to calculate:
  - Total tables processed
  - Successful tables (not in FAILED/PENDING states)
  - Failed tables
- Updates `iceberg_migration_runs` table with:
  - `status` = COMPLETED
  - `completed_at` = Current timestamp
  - Final counts
- Provides summary metrics for the entire migration run

---

### Status Progression

```
PENDING
    ↓
    │ [Iceberg migration procedure executed]
    ↓
COMPLETED (Iceberg migration successful)
    ↓
    │ [Validation: Hive vs Iceberg comparison]
    │ [All validations pass]
    ↓
VALIDATED
    │
    │ (OR, if any validation fails)
    ↓
VALIDATION_FAILED
```

---

## DAG 3: Folder-Only Data Copy

### Purpose

Copies raw folders from MapR-FS/HDFS to S3 using Hadoop DistCp via SSH, with no Hive metadata operations. Supports incremental re-runs and produces per-folder validation and an HTML report.

---

### Key Features

- **No Hive dependency** — pure filesystem copy, works for any data format
- **Files and folders** — works for both individual files and directories; `hadoop distcp` accepts any path. For a single file with no `dest_folder` specified, the destination key defaults to `basename(filename)`, so set `dest_folder` explicitly in the Excel if you need a precise S3 key.
- **Incremental support** — DistCp `-update` flag ensures only new/changed files are copied on re-runs
- **Per-folder tracking** — Iceberg tables record file counts, sizes, and match status for each folder
- **S3 validation** — re-verifies destination file count and size after copy
- **HTML report** — per-folder copy details with match indicators written to S3
- **Email delivery** — optional report email via SMTP

---

### Excel Configuration Format

**Required Columns:**

| Column          | Required | Description                                                                                                                                | Example                           |
| --------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------- |
| `source_path`   | **Yes**  | Full MapR/HDFS source path                                                                                                                 | `/mapr/cluster1/data/raw/sales`   |
| `target_bucket` | **Yes**  | S3 bucket — normalised to `s3a://`                                                                                                         | `s3a://data-lake`                 |
| `dest_folder`   | No       | Destination folder inside the bucket; defaults to the basename of `source_path` if not specified                                           | `sales`                           |
| `endpoint`      | No       | S3 endpoint URL for a non-default tenant; credentials resolved via `<hostname>_access_key/secret_key` Variables (see Multi-Tenant section) | `https://s3.tenant-a.example.com` |

**Default Behaviour:**

- If `dest_folder` is empty, the folder name defaults to the basename of `source_path`.
  - Example: `source_path = /mapr/cluster1/data/raw/sales` → `dest_folder = sales`
- `target_bucket` is normalised: `s3://` and `s3n://` are rewritten to `s3a://`.
- Rows with a missing `source_path` or `target_bucket` are skipped with a warning.

**Excel Sample:**

```
| source_path                                        | target_bucket          | dest_folder         |
|----------------------------------------------------|------------------------|---------------------|
| /mapr/cluster1/data/raw/sales                      | s3a://data-lake        | raw/sales           |
| /mapr/cluster1/data/raw/marketing                  | s3a://data-lake        | raw/marketing       |
| /mapr/cluster1/data/processed/finance              | s3a://data-lake        |                     |
| hdfs://namenode:8020/warehouse/logs/app_events     | s3://archive-bucket    | logs/app_events     |
| hdfs://namenode:8020/user/hive/warehouse/features  | s3a://ml-data-lake     |                     |
```

> Row 3: `dest_folder` is empty → defaults to `finance` (basename of source path)
> Row 4: HDFS path uses `hdfs://namenode:8020/` prefix; `s3://` bucket is automatically normalised to `s3a://`
> Row 5: `dest_folder` is empty → defaults to `features`

---

### Task Flow

```
validate_prerequisites_folder_copy  (SSH: check hadoop distcp, hadoop fs)
    ↓
init_folder_copy_tracking_tables
    ↓
create_data_copy_run
    ↓
parse_folder_copy_excel
    ↓
cluster_login_setup  (edge node auth)
    ↓
┌──────────────────────────────────────────────────────────────┐
│  Dynamic Task Mapping (one instance per Excel row)           │
│                                                              │
│  run_folder_distcp_ssh (SSH: DistCp -update, 24h timeout)   │
│      ↓                                                       │
│  record_data_copy_status                                     │
│      ↓                                                       │
│  validate_data_copy (SSH)                                    │
│      ↓                                                       │
│  update_data_copy_validation                                 │
└──────────────────────────────────────────────────────────────┘
    ↓ (all mapped tasks done)
finalize_data_copy_run
    ↓
generate_data_copy_html_report
    ↓
send_data_copy_report_email
```

---

### Task Summaries

#### Step 1 - `validate_prerequisites_folder_copy`

**Type:** SSH
**Purpose:** Validate SSH connectivity and Hadoop tooling before starting the folder copy

- Connects to the cluster edge node via SSH
- Runs three sequential checks:
  1. **SSH Connectivity** — verifies SSH connection with a simple echo command
  2. **Hadoop DistCp** — checks `hadoop distcp` is available on the edge node
  3. **Hadoop FS** — verifies `hadoop fs -ls /` executes successfully
- Sources environment via the login shell before each check to ensure environment variables are loaded
- If **all checks pass**, returns a `checks` dict and proceeds
- If **any check fails**, raises an exception with a detailed summary, halting the DAG before any tracking tables or run records are created

---

#### Step 2 - `init_folder_copy_tracking_tables`

**Type:** PySpark
**Purpose:** Create `data_copy_runs` and `data_copy_status` Iceberg tables if they do not exist

---

#### Step 3 - `create_data_copy_run`

**Type:** PySpark
**Purpose:** Insert a `RUNNING` record into `data_copy_runs` and return the `run_id`

- Run ID format: `folder_run_{YYYYMMDD_HHMMSS}_{uuid8}`

---

#### Step 4 - `parse_folder_copy_excel`

**Type:** PySpark
**Purpose:** Read and parse the Excel config file from S3

- Reads `source_path`, `target_bucket`, `dest_folder` columns
- Normalises `target_bucket` to `s3a://`
- Defaults `dest_folder` to `basename(source_path)` if not specified
- Returns a list of folder config dicts for dynamic task mapping
- Raises if no valid rows are found

---

#### Step 5 - `cluster_login_setup`

**Type:** SSH
**Purpose:** Authenticate with the cluster edge node and set up the session environment

- Receives the tracking `run_id` (same pattern as DAG 1 and DAG 2)
- Performs cluster authentication using the configured `auth_method` (`mapr`, `kinit`, or `none`)
- Returns a `cluster_setup` dict consumed by downstream SSH tasks

---

#### Step 6 - `run_folder_distcp_ssh`

- Receives the tracking `run_id` (same pattern as DAG 1 and DAG 2)
- Performs cluster authentication using the configured `auth_method` (`mapr`, `kinit`, or `none`)
- Returns a `cluster_setup` dict consumed by downstream SSH tasks

---

#### Step 6 - `run_folder_distcp_ssh`

**Type:** SSH (mapped per folder)
**Purpose:** Copy a single source folder to S3 via Hadoop DistCp

- Always uses `-update` flag — safe for both full and incremental runs
- Captures source file count and size before copy
- Captures S3 file count and size before and after copy
- Computes `files_copied` and `bytes_copied` as before/after deltas
- Sets `file_count_match` (exact) and `size_match` (within 1% tolerance)
- On failure returns a FAILED result dict — does not raise — so tracking can record it
- **Timeout:** 24 hours (`SSH_COMMAND_TIMEOUT`)

---

#### Step 7 - `record_data_copy_status`

**Type:** PySpark (mapped per folder)
**Purpose:** Insert one row into `data_copy_status` with DistCp metrics

---

#### Step 8 - `validate_data_copy`

**Type:** SSH (mapped per folder)
**Purpose:** Re-verify the S3 destination after copy

- Skips (marks `VALIDATION_SKIPPED`) if the copy step already failed
- Re-runs `hadoop fs -ls -R` and `hadoop fs -du -s` on the S3 destination
- Sets `VALIDATED` only if destination exists, file count matches, and size is within 1%
- Otherwise sets `VALIDATION_FAILED` with a descriptive error

---

#### Step 9 - `update_data_copy_validation`

**Type:** PySpark (mapped per folder)
**Purpose:** Update `data_copy_status` with final validation metrics and status

---

#### Step 10 - `finalize_data_copy_run`

**Type:** PySpark
**Purpose:** Aggregate folder counts and mark the run as complete

- Queries `data_copy_status` for authoritative counts
- Sets run status to `COMPLETED` (zero failures) or `COMPLETED_WITH_ERRORS`
- Updates `data_copy_runs` with totals and `completed_at`

---

#### Step 11 - `generate_data_copy_html_report`

**Type:** PySpark
**Purpose:** Generate an HTML report and write it to S3

- Summary cards: run status, total/validated/failed folders, incremental count, total GB, files, bytes copied
- Per-folder details table with source path, destination, copy status badge, file/size match indicators, and error snippets
- Writes to `{report_location}/{run_id}_data_copy_report.html`
- Returns `report_path` (S3 key); email task reads the report directly from S3

---

#### Step 12 - `send_data_copy_report_email`

**Type:** PySpark
**Purpose:** Email the HTML report via SMTP

- Subject: `Folder Data Copy Report - {run_id}`
- Skips silently if `migration_email_recipients` variable is empty
- Uses same SMTP connection (`migration_smtp_conn_id`) as DAG 1 and DAG 2

---

### Status Progression

```
RUNNING  (data_copy_runs while DAG is executing)
    ↓
    │ [Per folder: DistCp completes]
    ↓
COMPLETED / VALIDATED  (all folders copied and validated)
    │
    │ (OR, if any folder failed)
    ↓
COMPLETED_WITH_ERRORS
```

**Per-folder statuses (data_copy_status):**

| Status               | Meaning                                            |
| -------------------- | -------------------------------------------------- |
| `COMPLETED`          | DistCp succeeded (before validation)               |
| `VALIDATED`          | Destination verified — file count and size match   |
| `VALIDATION_FAILED`  | Destination exists but file count or size mismatch |
| `VALIDATION_SKIPPED` | Copy step failed — validation not attempted        |
| `FAILED`             | DistCp failed                                      |

---

## DAG 4: Parquet HMS Registration (`parquet_hms_registration`)

### Purpose

Registers plain parquet files already sitting on S3 (e.g. copied by DAG 3 or an
external migration) as **external Hive Metastore tables**, so they become
queryable without moving or rewriting any data.

### Key Features

- **Schema inference** - column names and types are inferred directly from the
  parquet files via Spark with `mergeSchema=true`, so the footers of **all**
  files are unioned — columns added or removed by schema evolution are not
  silently dropped; no schema input required
- **Automatic partition discovery** - Hive-style `key=value` directory layouts
  are detected, the table is created with the matching `PARTITIONED BY` clause,
  and partitions are registered with `MSCK REPAIR TABLE`. Partition columns are
  always registered as **STRING** (`partitionColumnTypeInference` is disabled):
  values keep their exact directory form (`month=01` stays `01`), and string
  partition keys are what Athena/Glue recommend for metastore interop
- **Safe by default** - tables that already exist in HMS at a _different_
  location are recorded as `SKIPPED`, never dropped or replaced; an existing
  table at the _same_ location (an Airflow retry) is MSCK-repaired and
  recorded `REGISTERED`
- **Validation** - row counts (HMS table vs direct parquet read) and partition
  counts (`SHOW PARTITIONS` vs leaf `key=value` directories on S3) are
  compared after registration
- **Tracking & reporting** - run-level and per-table status in Iceberg tracking
  tables, HTML report written to S3 and emailed

### Excel Configuration

One row per table to register:

| database   | table        | s3_location                             |
| ---------- | ------------ | --------------------------------------- |
| sales_data | transactions | s3a://data-lake/sales_data/transactions |
| sales_data | orders       | s3://data-lake/sales_data/orders        |

- All three columns are required; rows with missing cells are skipped with a warning
- `s3_location` accepts `s3://`, `s3n://`, or `s3a://` (normalized to `s3a://`)
- The target database is created with `CREATE DATABASE IF NOT EXISTS` when missing
- Duplicate `(database, table)` rows: the first row wins
- Partition columns are **not** configured — they are discovered from the
  directory layout. Locations without `key=value` directories are registered as
  unpartitioned tables.

### Task Flow

```
init_hms_tracking_tables
        |
create_hms_registration_run
        |
parse_parquet_hms_excel
        |
register_parquet_tables          (mapped: one task per Excel row)
        |
validate_registered_tables       (mapped, max 3 concurrent)
        |
update_hms_validation_status     (mapped)
        |
generate_hms_html_report
        |
send_hms_report_email
        |
finalize_hms_run
```

### Task Summaries

| Task                           | Description                                                                                                                                                                                                                                          |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `init_hms_tracking_tables`     | Creates `hms_registration_runs` and `hms_registration_status` Iceberg tracking tables if missing                                                                                                                                                     |
| `create_hms_registration_run`  | Inserts a `RUNNING` run record; generates the `hms_reg_...` run ID                                                                                                                                                                                   |
| `parse_parquet_hms_excel`      | Reads the Excel file from S3; emits one config per valid row                                                                                                                                                                                         |
| `register_parquet_tables`      | Infers schema (merged across all footers), detects partition columns from `key=value` dirs (typed STRING), `CREATE EXTERNAL TABLE` + `MSCK REPAIR TABLE`; existing table at same location → repair + `REGISTERED`, at different location → `SKIPPED` |
| `validate_registered_tables`   | Compares HMS row count vs direct parquet count, and `SHOW PARTITIONS` count vs leaf `key=value` directories on S3                                                                                                                                    |
| `update_hms_validation_status` | Writes validation results back to the tracking table (`VALIDATED` / `VALIDATION_FAILED`)                                                                                                                                                             |
| `generate_hms_html_report`     | Builds the HTML report and writes it to `migration_report_location`                                                                                                                                                                                  |
| `send_hms_report_email`        | Emails the report via SMTP (skips gracefully when no recipients configured)                                                                                                                                                                          |
| `finalize_hms_run`             | Aggregates per-table statuses into the run record (`COMPLETED` / `COMPLETED_WITH_FAILURES` / `FAILED`)                                                                                                                                               |

### Status Progression

Per table: `REGISTERED` → `VALIDATED` (or `VALIDATION_FAILED`), with terminal
`SKIPPED` (table already in HMS at a different location) and `FAILED`
(registration error) states.
Per run: `RUNNING` → `COMPLETED` / `COMPLETED_WITH_FAILURES` / `FAILED`.

### Notes & Limitations

- **Partition columns are always STRING.** Spark's partition type inference is
  explicitly disabled (`spark.sql.sources.partitionColumnTypeInference.enabled=false`),
  so directory values are never coerced (`month=01` and `month=1` stay distinct,
  leading zeros survive, `dt=2024-01-01` is not turned into a DATE). Query with
  string predicates (`WHERE year='2024'`) or cast in the query. See the
  [Athena guidance on string partition keys](https://docs.aws.amazon.com/athena/latest/ug/performance-tuning-data-optimization-techniques.html)
  and the [Spark partition discovery docs](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- **Schema evolution is handled at registration** via `mergeSchema=true`: the
  registered schema is the union of all file footers; files missing
  later-added columns read those columns as NULL (standard parquet behavior).
  Genuinely incompatible type changes across files fail the registration loudly
- Empty partition directories are registered by MSCK and return zero rows when
  queried; validation counts directories on both sides, so empty partitions do
  not flag a mismatch
- Non-Hive-style layouts (no `key=value` directories) are registered as
  unpartitioned tables
- No drop/replace support — re-running against a table registered elsewhere
  records `SKIPPED`

---

## Remediation: fixing already-migrated tables

### Recreating DAG 1 destination tables (`migration_recreate_tables`)

DAG 1 normally repairs an existing destination table (`MSCK REPAIR`) instead of
rewriting it. If a table was created by an older DAG version with wrong
partition-column types (e.g. a `date` partition key stored as `STRING`), set the
`migration_recreate_tables` Airflow Variable (or `MIGRATION_RECREATE_TABLES` env
var) to `true` and re-run DAG 1. On that run, `create_hive_tables` **DROPs** each
existing destination table and recreates it from scratch with the correct schema.

- Destination tables are **EXTERNAL**, so `DROP TABLE` removes only the
  Hive/Iceberg metadata — **the S3 data is preserved**.
- Default is `false`. Flip it on for the remediation run, then set it back to
  `false` for normal incremental runs (it applies to every table in the run).

### ⚠️ If you also ran DAG 2 with **in-place** migration

DAG 2 **in-place** migration (`inplace_migration = True`) converts the DAG 1
destination table to an **Iceberg** table _under the same name and location_.
DAG 1 with `migration_recreate_tables=true` **will refuse to drop an Iceberg
table** (the table is marked `FAILED` with a message pointing here) — dropping it
would destroy the Iceberg table's metadata and snapshots.

To remediate such a table you must first strip the Iceberg layer with the
standalone **`iceberg_cleanup.py`** script, then recreate and re-migrate:

```
iceberg_cleanup.py --apply   →   DAG 1 (migration_recreate_tables=true)   →   DAG 2 (in-place)
```

> **DAG 2 snapshot migrations do NOT need this.** A snapshot writes the Iceberg
> table to a separate `{database}_iceberg` database, so the DAG 1 Hive table keeps
> its name and DAG 1 recreate is already safe — no cleanup required.

#### `iceberg_cleanup.py`

A self-contained remediation script (not an Airflow DAG). For each named table it:

1. verifies the table **is** an Iceberg table (Hive tables are skipped — nothing
   to clean);
2. runs `DROP TABLE` **without PURGE** (metadata-only — **data files are kept**);
3. deletes the table's `metadata/` folder, returning the S3 location to a plain
   Parquet/ORC directory that DAG 1 can recreate as a Hive external table.

**Only table definitions and Iceberg metadata are removed; the data referenced by
the table remains intact on S3.**

Run it with the same Spark configuration the DAGs use (Hive metastore + S3A
credentials), e.g. via `spark-submit` on the cluster. It **defaults to a dry
run** — pass `--apply` to make changes:

```bash
# dry run — reports what would be cleaned, makes no changes
spark-submit iceberg_cleanup.py --tables db1.tbl_a,db1.tbl_b

# execute
spark-submit iceberg_cleanup.py --tables db1.tbl_a,db1.tbl_b --apply

# convenience: qualify a bare table list with a single database
spark-submit iceberg_cleanup.py --database db1 --tables tbl_a,tbl_b --apply
```

| Per-table status      | Meaning                                           |
| --------------------- | ------------------------------------------------- |
| `DRY_RUN`             | Would be cleaned (no `--apply`) — no changes made |
| `CLEANED`             | Dropped (no purge) and `metadata/` folder removed |
| `SKIPPED_NOT_ICEBERG` | Table is Hive, not Iceberg — left untouched       |
| `SKIPPED_NOT_FOUND`   | Table does not exist                              |
| `FAILED`              | An error occurred (data untouched)                |

### Propagating the fix to Iceberg tables

If DAG 2 was already run, the Iceberg table inherited the **wrong** partition
type too (both `system.migrate` and `system.snapshot`/CTAS derive their schema
and partition spec from the **source Hive table**). The fix propagates
automatically: once DAG 1 has recreated the Hive table with the correct partition
types, **re-running DAG 2 regenerates the Iceberg table with the correct types**.
You only need to clean up the stale Iceberg table and run the steps in order.

**If DAG 2 was run with _snapshot_** (`{db}.{tbl}` is Hive, `{db}_iceberg.{tbl}` is
the Iceberg copy):

```
1. DAG 1 (migration_recreate_tables=true)          # recreate Hive {db}.{tbl} with correct types (safe — it's Hive)
2. iceberg_cleanup.py --apply --tables {db}_iceberg.{tbl}   # drop stale snapshot + remove its metadata/ folder
3. DAG 2 (snapshot)                                # re-snapshot from the fixed Hive table
```

> DAG 2 self-drops an existing snapshot destination before re-snapshotting, but
> running `iceberg_cleanup` first also clears any leftover `metadata/` folder so
> the re-snapshot starts clean.

**If DAG 2 was run with _in-place_** (`{db}.{tbl}` is now an Iceberg table under the
same name — DAG 1 recreate refuses to drop it):

```
1. iceberg_cleanup.py --apply --tables {db}.{tbl}  # strip Iceberg layer → back to plain parquet (data kept)
2. DAG 1 (migration_recreate_tables=true)          # recreate Hive {db}.{tbl} with correct types
3. DAG 2 (in-place)                                # system.migrate on the corrected Hive table
```

**Why it works:** in-place `system.migrate` builds the Iceberg schema + identity
partition spec from the Hive metastore definition; snapshot anchors to the source
Hive schema (the CTAS fallback does `SELECT * ... PARTITIONED BY (cols)`). Either
path inherits the corrected partition type once the Hive table is right, and DAG 1
re-registers partitions with the correct types so DAG 2 has correct partition
metadata to work from.

> **Note:** regenerating the Iceberg table resets its snapshot/time-travel
> history (you are rebuilding the metadata, not altering it in place). The
> underlying data files are untouched — a partition-_type_ fix requires rebuilding
> the Iceberg metadata.

---

## Tracking Tables

### MapR-to-S3 Migration Tracking

1. **migration_tracking.migration_runs**: Run-level metadata for MapR-to-S3 migrations.
2. **migration_tracking.migration_table_status**: Table-level tracking for MapR-to-S3 migrations.

---

### Iceberg Migration Tracking

1. **migration_tracking.iceberg_migration_runs**: Run-level metadata for Iceberg migrations.
2. **migration_tracking.iceberg_migration_table_status**: Table-level tracking for Iceberg migrations.

### Folder Data Copy Tracking

1. **migration_tracking.data_copy_runs**: Run-level metadata for folder-only data copy runs.
2. **migration_tracking.data_copy_status**: Folder-level tracking — one row per source/destination pair per run.

---

End of Document
