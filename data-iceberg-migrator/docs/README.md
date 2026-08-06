# Source (MapR-FS/HDP-HDFS) to S3 Migration DAG

An automated **Airflow TaskFlow-based migration pipeline** consisting of four independent DAGs for orchestrating large-scale Hive table migrations from source (MapR-FS or HDP-HDFS) to S3 and converting existing tables to Iceberg format.

---

## Overview

This implementation provides four independent but complementary migration DAGs:

1. **`source_to_s3_migration`** - Migrates Hive tables from source (MapR-FS/HDP-HDFS) to S3
2. **`iceberg_migration`** - Converts existing Hive tables in S3 to Apache Iceberg format
3. **`folder_only_data_copy`** - Copies raw folders from source cluster (MapR-FS/HDP-HDFS) to S3 via DistCp — no Hive metadata
4. **`iceberg_catalog_migration`** - Migrates existing Iceberg tables from a Hadoop catalog to a Hive (HMS) catalog via the `rewrite_table_path` procedure — full snapshot history preserved

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

| Variable                   | Description                                                                                                                                                                                                                  | Required For     | Applies To                                        |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- | ------------------------------------------------- |
| `auth_method`              | Authentication method: `mapr`, `kinit`, or `none`                                                                                                                                                                            | MapR/Kerberos    | `source_to_s3_migration`, `folder_only_data_copy` |
| `service_account_user_id`  | **Recommended.** Tenant service account the DAG runs as on the source cluster. Validates the MapR ticket and builds every user-scoped temp and log path. See [Service account configuration](#service-account-configuration) | All auth methods | `source_to_s3_migration`, `folder_only_data_copy` |
| `mapr_user`                | Deprecated alias for `service_account_user_id`, kept for backward compatibility. If both are set, `service_account_user_id` wins                                                                                             | MapR auth        | `source_to_s3_migration`, `folder_only_data_copy` |
| `mapr_ticketfile_location` | MapR ticket file path                                                                                                                                                                                                        | MapR auth        | `source_to_s3_migration`, `folder_only_data_copy` |
| `cluster_type`             | Display label for reports: `MapR`, `HDP`, etc.                                                                                                                                                                               | HTML reports     | `source_to_s3_migration`                          |

### Optional Variables

| Variable                           | Default          | Description                                                                                                                                                                                                      | Applies To                                        |
| ---------------------------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `cluster_edge_temp_path`           | `/tmp/migration` | Temp dir on the edge node's **local disk** for run-scoped files (discovery scripts, path lists). Supports `${USER}`, expanded to the MapR ticket user. Not used for DistCp logs — see `cluster_distcp_log_root`. | `source_to_s3_migration`, `folder_only_data_copy` |
| `cluster_distcp_log_root`          | `/tmp`           | Root on the **cluster filesystem** (MapR-FS/HDFS) under which per-user, per-run DistCp log directories are created. The user segment comes from `service_account_user_id`.                                       | `source_to_s3_migration`                          |
| `cluster_edge_discovery_temp_path` | `/tmp`           | Local temp dir on edge node for the Spark discovery scratch folder. Literal path only (no `${USER}`).                                                                                                            | `source_to_s3_migration`                          |
| `cluster_hive_scratch_dir`         | `/tmp/hive`      | Hive scratch dir for PySpark sessions started on the **source edge node**. Literal path only (no `${USER}`).                                                                                                     | `source_to_s3_migration`                          |
| `s3_endpoint`                      | _(empty)_        | Default S3 endpoint URL (all buckets)                                                                                                                                                                            | `source_to_s3_migration`, `folder_only_data_copy` |
| `s3_access_key`                    | _(empty)_        | Default S3 access key (all buckets)                                                                                                                                                                              | `source_to_s3_migration`, `folder_only_data_copy` |
| `s3_secret_key`                    | _(empty)_        | Default S3 secret key (all buckets)                                                                                                                                                                              | `source_to_s3_migration`, `folder_only_data_copy` |
| `migration_distcp_mappers`         | `50`             | Number of DistCp mappers                                                                                                                                                                                         | `source_to_s3_migration`, `folder_only_data_copy` |
| `migration_distcp_bandwidth`       | `100`            | Bandwidth limit per mapper (MB/s)                                                                                                                                                                                | `source_to_s3_migration`, `folder_only_data_copy` |
| `migration_distcp_preserve_delete` | `true`           | DistCp delete-preservation mode for partition-filtered copies (see [DistCp partition copy modes](#distcp-partition-copy-modes))                                                                                  | `source_to_s3_migration`                          |
| `migration_include_db_in_path`     | `true`           | When `true` (default), destination S3 path is `{bucket}/{database}/{table}`. When `false`, path is `{bucket}/{table}` (database folder omitted)                                                                  | `source_to_s3_migration`                          |
| `s3_listing_tool`                  | `hadoop`         | Tool for S3 listing: `hadoop` or `boto3`                                                                                                                                                                         | Currently unused                                  |
| `migration_smtp_conn_id`           | `smtp_default`   | Airflow SMTP connection ID for email reports                                                                                                                                                                     | All DAGs                                          |
| `migration_email_recipients`       | _(empty)_        | Comma-separated email addresses for reports                                                                                                                                                                      | All DAGs                                          |
| `hdfs_nameservice`                 | _(empty)_        | HDFS HA nameservice (e.g. `mycluster`); leave empty for MapR                                                                                                                                                     | `source_to_s3_migration`, `folder_only_data_copy` |

#### Service account configuration

**Set `service_account_user_id` explicitly for every tenant.** It is the single
value that identifies which account the DAG runs as on the source cluster, and it
drives all of the following:

- the MapR ticket validation check in `validate_prerequisites` and `cluster_login_setup`
- the MapR ticket file path, when `mapr_ticketfile_location` contains `${USER}`
- the DistCp log directory on the cluster filesystem
- the edge-node temp directory, when `cluster_edge_temp_path` contains `${USER}`

**Resolution order**

| Order | Source                                 | When used                                 |
| ----- | -------------------------------------- | ----------------------------------------- |
| 1     | `service_account_user_id` config       | Whenever it is set — always wins          |
| 2     | `mapr_user` config                     | Legacy deployments that have not migrated |
| 3     | Active MapR ticket (`maprlogin print`) | Neither is set, and a ticket exists       |
| 4     | Login shell user (`id -un`)            | Nothing else available                    |

**When it is configured (recommended)**

Paths are deterministic and reviewable before the run. The value appears in the
`cluster_login_setup` task log as:

    MAPR_EFFECTIVE_USER=`USER_NAME`
    SERVICE_ACCOUNT_SOURCE=config:service_account_user_id

It is also used to validate the MapR ticket, so if the configured account and the
ticket on the edge node disagree, the run fails immediately at login with a clear
message rather than part-way through the copy against the wrong identity.

**When it is not configured (not recommended)**

The DAG falls back to whichever identity the edge node's `.profile` happens to
provide. This works, but it means the paths a run uses depend on node-local shell
state rather than on anything visible in Airflow — and if `.profile` is later
pointed at a different tenant's ticket, the change is silent. The task log emits:

    WARNING: service_account_user_id is not configured. Falling back to ...
    SERVICE_ACCOUNT_SOURCE=login shell (.profile)

**Where to set it**

Airflow Variable `service_account_user_id`, or environment variable
`SERVICE_ACCOUNT_USER_ID`. Like all variables here it supports a run-scoped
override (`service_account_user_id__<run_id>`), so multiple tenants can run
concurrently from the same DAG without editing it.

#### Edge-node temp paths for service accounts

`cluster_edge_temp_path` supports `${USER}`, `$USER`, `{user}`, and a leading `~`. These expand on the edge node to the **MapR ticket user** (parsed from `maprlogin print`), not the SSH/Linux login user.

Example for a tenant service account:

    cluster_edge_temp_path = /user/${USER}/tmp/migration

The resolved path is echoed as `TEMP_DIR=` and the identity as `MAPR_EFFECTIVE_USER=`
in the `cluster_login_setup` task log.

`cluster_edge_discovery_temp_path` does **not** support placeholders — the discovery
directory is created over a plain SSH command and then written to over SFTP, which does
not expand shell variables. Use a literal path.

#### DistCp log directory on the cluster filesystem

`hadoop distcp -log` writes its logs to the **cluster filesystem** (MapR-FS/HDFS),
not to the edge node's local disk. This is a common source of confusion, because
the two look identical at the shell — `/tmp` on the edge node and `/tmp` on
MapR-FS are different directories with different ownership. `cluster_edge_temp_path`
controls the local one; `cluster_distcp_log_root` controls the cluster one.

**Resolved path**

    <cluster_distcp_log_root>/<cluster_user>/distcp_logs/<run_id>/distcp_<table>.log

With the defaults and a tenant service account of `sa_user`, a run resolves to:

    /tmp/sa_user/distcp_logs/run_20260806_143022_a1b2c3d4/distcp_transactions.log

Each segment serves a purpose:

| Segment                   | Source                                     | Why                                                                          |
| ------------------------- | ------------------------------------------ | ---------------------------------------------------------------------------- |
| `cluster_distcp_log_root` | Airflow Variable / env var, default `/tmp` | Lets you relocate logs if `/tmp` is restricted on your cluster               |
| `<cluster_user>`          | Derived at runtime (see below)             | Isolates each tenant so accounts never write into another's directory        |
| `distcp_logs`             | Fixed                                      | Keeps DistCp logs separate from other artifacts under the user directory     |
| `<run_id>`                | Generated per DAG run                      | Prevents runs from overwriting each other; logs are read back for validation |

**How `<cluster_user>` is determined**

It is the resolved service account — `service_account_user_id` when set, otherwise
the `.profile` fallback. The resolved value is echoed in the
`cluster_login_setup` task log as:

    MAPR_EFFECTIVE_USER=<user>
    SERVICE_ACCOUNT_SOURCE=<where it came from>
    DISTCP_LOG_DIR=<full path>

**When `service_account_user_id` is configured**

Each tenant resolves to its own directory — `TENANT_1` writes under
`/tmp/TENANT_1/`, `TENANT_2` under `/tmp/TENANT_2/`. This holds whether
tenants run on the same edge node or different ones, and the path is known before
the run starts.

The configured value is used verbatim, including casing, so it must match the
account name as the cluster reports it. To confirm what a given account is called,
run this on the edge node as that account:

    maprlogin print 2>/dev/null | sed -n 's/.*user = \([^,]*\),.*/\1/p' | head -1

**When it is not configured**

The path falls back to the `.profile` identity. Multiple tenants sharing one edge
node under the same login user will then share a log directory — still correct,
since ownership matches, but per-tenant isolation is lost and the path is not
predictable from Airflow alone.

**Permissions and retention**

The directory is created with `hadoop fs -mkdir -p` and set to `700`, so only the
owning service account can read its own DistCp logs. If you need the migration team
to inspect another tenant's logs, either relax the mode after the run or point
`cluster_distcp_log_root` at a shared location with group access.

Directory creation runs under `set -e`, so if `cluster_distcp_log_root` is not
writable by the service account, `cluster_login_setup` fails immediately rather
than the run failing later at the DistCp step. Logs are **not** deleted at the end
of a run — `cleanup_edge` leaves them in place because the validation steps read
them back. Include this path in your normal cluster retention policy.

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

The default `/tmp/hive` is world-writable on both MapR-FS and the local filesystem
and works for every service account. Only change it if `/tmp/hive` is restricted on
your cluster.

Placeholders are **not** supported. The value is baked into a Python script that
is written over SFTP, which does not expand shell variables, so a `${USER}` here
would be created as a literal directory of that name. Use a literal path.

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

| DAG   | Parameter         | Required | Description                                 | Example                                              |
| ----- | ----------------- | -------- | ------------------------------------------- | ---------------------------------------------------- |
| DAG 1 | `excel_file_path` | Yes      | S3 path to Excel config                     | `s3a://config-bucket/migration.xlsx`                 |
| DAG 2 | `excel_file_path` | Yes      | S3 path to Iceberg config                   | `s3a://config-bucket/iceberg_migration.xlsx`         |
| DAG 3 | `excel_file_path` | Yes      | S3 path to folder copy config               | `s3a://config-bucket/folder_copy.xlsx`               |
| DAG 4 | `excel_file_path` | Yes      | S3 path to Iceberg catalog migration config | `s3a://config-bucket/iceberg_rewrite_migration.xlsx` |

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
- Resolves the effective cluster identity from `maprlogin print` (falling back to `id -un`) and logs it as `MAPR_EFFECTIVE_USER=`
- Creates the local working directory on the edge node (`{cluster_edge_temp_path}/{run_id}`, default `/tmp/migration/{run_id}`)
- Creates the DistCp log directory on the cluster filesystem (`{cluster_distcp_log_root}/{cluster_user}/distcp_logs/{run_id}`, default root `/tmp`) and logs it as `DISTCP_LOG_DIR=`
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
- DistCp logs written to the cluster filesystem at `{cluster_distcp_log_root}/{cluster_user}/distcp_logs/{run_id}/distcp_{table}.log` (partition-filtered copies add a `_part{n}` suffix, one log per partition)
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

## DAG 4: Iceberg Catalog Migration (`iceberg_catalog_migration`)

> Full reference: [README_migration_iceberg_catalog.md](README_migration_iceberg_catalog.md)

### Purpose

Migrates **existing Iceberg tables** from a **Hadoop catalog** to a **Hive (HMS)
catalog**. The Iceberg data and metadata files must already be copied to the
destination S3 bucket; this DAG rewrites the path references embedded in the
metadata (via the `rewrite_table_path` stored procedure) and registers the table
in HMS with `register_table`.

Unlike DAG 2 (which converts _Hive_ tables to Iceberg), DAG 4 is
Iceberg-to-Iceberg: no data is rewritten and full snapshot history, partition
transforms, exact schema types, and table properties are preserved.

---

### Prerequisites

- Both **data files** and **metadata files** for every table are already present
  under `dest_s3_prefix` on S3
- The destination Spark/Iceberg runtime is **Iceberg 1.4+** (required for
  `rewrite_table_path`; register-only rows do not invoke the procedure)
- Both source and destination buckets are reachable from the Spark workers —
  `rewrite_table_path` reads the source metadata/manifests to know what to
  rewrite. This DAG does **not** configure S3 credentials itself; it relies on
  the endpoints/credentials registered through the nx1 portal's object store
  configuration

---

### Key Features

- **Two row categories** — a row is **rewrite** when `source_s3_prefix` differs
  from `dest_s3_prefix`, and **register-only** when `source_s3_prefix` is blank
  or equal to `dest_s3_prefix` (metadata already points at the destination, so
  the rewrite is skipped and the table is registered straight from the dest
  metadata file)
- **History preserved** — every snapshot's paths are rewritten, so time travel,
  partition transforms (`year`, `month`, `bucket`, `truncate`, …), exact schema
  types, and table properties survive the move
- **Incremental** — re-copy new/changed data and metadata files to the
  destination, then re-run; the DAG rewrites whatever is present at the
  destination. Change detection and syncing are **not** handled by the DAG
- **Five-way validation** — row count, partition count, schema, path-rewrite
  verification (no snapshot still references `source_s3_prefix`), and snapshot
  count
- **Tracking & reporting** — run-level and per-table status in Iceberg tracking
  tables, HTML report written to S3 and emailed

---

### Excel Configuration Format

| Column             | Required    | Description                                                                                                                                   | Example                         |
| ------------------ | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| `database`         | **Yes**     | HMS database name (same for source and destination)                                                                                           | `analytics`                     |
| `table`            | No          | Table name(s) — single, comma-separated, or wildcard; defaults to `*`                                                                         | `orders` or `trans*` or `*`     |
| `source_s3_prefix` | Conditional | Original source prefix embedded in the pre-copied metadata. **Leave blank** or set equal to `dest_s3_prefix` to mark the row as register-only | `s3a://source-bucket/warehouse` |
| `dest_s3_prefix`   | **Yes**     | Destination prefix the metadata paths are rewritten to (or where the table already lives, for register-only rows)                             | `s3a://dest-bucket/warehouse`   |

Rows are grouped by `(database, source_s3_prefix, dest_s3_prefix)`; multiple rows
for the same group accumulate their table tokens. Blank `source_s3_prefix` cells
are mirrored to `dest_s3_prefix` before grouping.

**Excel Sample:**

```
| database  | table      | source_s3_prefix              | dest_s3_prefix              |
|-----------|------------|-------------------------------|-----------------------------|
| analytics | orders     | s3a://source-bucket/warehouse | s3a://dest-bucket/warehouse |  ← rewrite
| analytics | customers  | s3a://source-bucket/warehouse | s3a://dest-bucket/warehouse |  ← rewrite
| reporting | *          | s3a://source-bucket/warehouse | s3a://dest-bucket/warehouse |  ← rewrite
| ops       | inventory  |                               | s3a://dest-bucket/ops       |  ← register-only (blank source)
| ops       | snapshots  | s3a://dest-bucket/ops         | s3a://dest-bucket/ops       |  ← register-only (source == dest)
```

---

### Task Flow

```
init_tracking_tables
    ↓
create_migration_run
    ↓
parse_excel
    ↓
┌───────────────────────────────────────────────────────────────────┐
│  Dynamic Task Mapping (one set of tasks per database config)      │
│  (every mapped task uses trigger_rule = all_done)                 │
│                                                                   │
│  validate_data_presence                                           │
│    ↓                                                              │
│  update_data_presence_in_tracking                                 │
│    ↓                                                              │
│  discover_tables (PySpark)                                        │
│    ↓                                                              │
│  update_discovered_tables_in_tracking                             │
│    ↓                                                              │
│  rewrite_and_register_tables                                      │
│    ↓                                                              │
│  update_rewrite_and_register_in_tracking                          │
│    ↓                                                              │
│  validate_dest_tables  (max 3 concurrent)                         │
│    ↓                                                              │
│  update_validation_in_tracking                                    │
└───────────────────────────────────────────────────────────────────┘
    ↓
generate_html_report
    ↓
send_report_email
    ↓
finalize_run
```

---

### Task Summaries

| Task                                      | Description                                                                                                                                                                                                                                                         |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `init_tracking_tables`                    | Creates `rewrite_migration_runs` and `rewrite_migration_table_status` Iceberg tracking tables if missing                                                                                                                                                            |
| `create_migration_run`                    | Inserts a `RUNNING` run record; generates the `rewrite_run_{YYYYMMDD_HHMMSS}_{uuid8}` run ID                                                                                                                                                                        |
| `parse_excel`                             | Reads the Excel file from S3 and groups rows by `(database, source_s3_prefix, dest_s3_prefix)` for dynamic task mapping; raises if no valid rows                                                                                                                    |
| `validate_data_presence`                  | Lists table directories under `dest_s3_prefix`, filters by the row's table tokens, and verifies each path exists, has a `metadata/` subdirectory, and contains files → `CONFIRMED` / `MISSING` / `FAILED`                                                           |
| `update_data_presence_in_tracking`        | Inserts (or, on rerun, updates) the initial per-table tracking rows with presence status, file count, size, and `overall_status`                                                                                                                                    |
| `discover_tables`                         | Reads schema, partition spec, row count, and snapshot count from the destination `metadata.json` for each `CONFIRMED` table (not from HMS, which may not have it registered yet)                                                                                    |
| `update_discovered_tables_in_tracking`    | Writes discovery results — schema/partition JSON, source counts, `DISCOVERED` status — back to the tracking table                                                                                                                                                   |
| `rewrite_and_register_tables`             | Drops any stale HMS registration (no `PURGE`), then: **register-only** → resolve latest dest `metadata.json` and `register_table`; **rewrite** → temporarily register from the source metadata, `CALL rewrite_table_path`, drop the temp, register the new metadata |
| `update_rewrite_and_register_in_tracking` | Records create status/duration and overwrites `source_row_count` / `source_partition_count` with post-register counts; `overall_status` is sticky (`FAILED` / `DATA_MISSING` are not overwritten)                                                                   |
| `validate_dest_tables`                    | Compares row count, partition count, schema, snapshot count, and verifies no snapshot's `manifest_list` still references `source_s3_prefix` (path check skipped for register-only tables)                                                                           |
| `update_validation_in_tracking`           | Writes validation results and the final `VALIDATED` / `VALIDATION_FAILED` status, with a race-safe catch-all for rows no validator reached                                                                                                                          |
| `generate_html_report`                    | Builds the HTML report and writes it to `{report_location}/{run_id}_rewrite_report.html`                                                                                                                                                                            |
| `send_report_email`                       | Emails the report (subject `Iceberg Catalog Migration Report — {run_id}`); skips silently when no recipients configured                                                                                                                                             |
| `finalize_run`                            | Aggregates per-table statuses into the run record (`COMPLETED` / `COMPLETED_WITH_MISSING` / `COMPLETED_WITH_FAILURES` / `FAILED`)                                                                                                                                   |

---

### Status Progression

```
DATA_CONFIRMED  (metadata and data files found at destination)
    ↓
DISCOVERED  (metadata read from destination, tracking record updated)
    ↓
TABLE_CREATED  (rewrite_table_path + register_table executed)
    ↓
VALIDATED  (row count, partition count, schema, path rewrite, snapshot count all match)

DATA_MISSING → skipped in all downstream steps, visible in report
(Any stage) → FAILED or VALIDATION_FAILED
```

| Per-table status    | Meaning                                                                      |
| ------------------- | ---------------------------------------------------------------------------- |
| `DATA_CONFIRMED`    | Data and metadata files present at destination S3 — tracking row inserted    |
| `DISCOVERED`        | Metadata read from destination, schema/partition info recorded               |
| `DATA_MISSING`      | No files or missing `metadata/` directory — skipped                          |
| `TABLE_CREATED`     | `rewrite_table_path` + `register_table` completed, validation pending        |
| `VALIDATED`         | All five validation checks passed — migration success                        |
| `VALIDATION_FAILED` | Row count, partition count, schema, path-rewrite, or snapshot-count mismatch |
| `FAILED`            | Error at data presence check, discovery, or table creation                   |

**Per-run statuses:** `COMPLETED` · `COMPLETED_WITH_MISSING` (no failures, some
tables had no data at the destination) · `COMPLETED_WITH_FAILURES` · `FAILED`
(run-level tracking error).

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

### Iceberg Catalog Migration Tracking

1. **migration_tracking.rewrite_migration_runs**: Run-level metadata for Hadoop-catalog-to-HMS Iceberg migrations.
2. **migration_tracking.rewrite_migration_table_status**: Table-level tracking — data presence, discovery, registration, and validation; partitioned by `source_database`.

---

End of Document
