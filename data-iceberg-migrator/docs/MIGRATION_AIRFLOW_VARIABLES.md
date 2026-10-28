# Migration DAG — Airflow Variables Reference

Reference for every Airflow Variable the `data-iceberg-migrator` DAGs read.
Set these at **Airflow UI → Admin → Variables** 


## Which DAG does what

| ID | DAG file                            | What it does                                                 |
|----|-------------------------------------|--------------------------------------------------------------|
| 1  | `migration_dag_mapr_to_s3.py`       | MapR / HDFS → S3 + Hive table create + validation. SSH-driven, uses DistCp, has discovery. |
| 2  | `migration_dag_iceberg.py`          | Hive → Iceberg conversion (in-place or snapshot). Pure Spark/Kyuubi — no edge SSH, no DistCp, no discovery. |
| 3  | `migration_dag_folder_copy.py`      | Folder-only data copy (no Hive table create, no validation). SSH-driven, uses DistCp. |

**"Applies to" column values** below use these IDs. `All` means 1, 2, and 3.

**Resolution precedence** (unless noted otherwise):
`Airflow Variable → env var → default`. A Variable cleared in the UI reads
back as present-but-empty; empty is treated as unset for most keys. Non-integer,
zero/negative, or unreadable Variables silently fall through to env/default so a
bad value can never drop the DAG from Airflow.

**Parse-time vs. run-time:** most Variables are read inside tasks at run time
(cheap). A small allowlist is read at DAG **parse time** — every scheduler parse
loop (~30s) hits the Airflow metadata DB for these. They're kept small and
explicit; see **Parse-time Variables** at the bottom. Marked `[parse]` in the
tables below where applicable.

---

## 1. SSH & Edge Access

| Variable                             | Env                                   | Default              | Values               | Applies to | Description |
|--------------------------------------|---------------------------------------|----------------------|----------------------|------------|-------------|
| `cluster_ssh_conn_id`                | `CLUSTER_SSH_CONN_ID`                 | `cluster_edge_ssh`   | Airflow conn id      | 1, 3       | SSH connection the DAG uses to reach the edge node. Must exist in Admin → Connections. DAG 2 goes through Spark/Kyuubi and ignores this. |
| `cluster_edge_temp_path`             | `CLUSTER_EDGE_TEMP_PATH`              | `/tmp/migration`     | absolute path        | 1, 3       | Scratch dir on the edge for login tickets, run markers, DistCp staging. Literal path only (no `$VAR` / `~`). |
| `cluster_edge_discovery_temp_path`   | `CLUSTER_EDGE_DISCOVERY_TEMP_PATH`    | `/tmp`               | absolute path        | 1          | Parent for per-run discovery working dirs. Literal path only. |
| `cluster_hive_scratch_dir`           | `CLUSTER_HIVE_SCRATCH_DIR`            | `/tmp/hive`          | absolute path        | 1          | Passed to the edge-side discovery pyspark as `hive.exec.scratchdir`. |
| `cluster_distcp_log_root`            | `CLUSTER_DISTCP_LOG_ROOT`             | `/tmp`               | absolute path        | 1, 3       | Base dir for per-run DistCp log files on the edge. |

---

## 2. Source Cluster Authentication

| Variable                       | Env                          | Default                        | Values                       | Applies to | Description |
|--------------------------------|------------------------------|--------------------------------|------------------------------|------------|-------------|
| `auth_method`                  | `AUTH_METHOD`                | `mapr`                         | `mapr` / `kinit` / `none`    | 1, 3       | How the DAG proves identity to the source cluster. `mapr` → maprlogin ticket. `kinit` → Kerberos TGT. `none` → skip the auth probe (local Docker testing). |
| `service_account_user_id`      | `SERVICE_ACCOUNT_USER_ID`    | *(empty)*                      | Unix username                | 1, 3       | Service account whose TGT is checked under `auth_method=kinit`. Also used by `validate_prerequisites` logs. |
| `mapr_ticketfile_location`     | `MAPR_TICKETFILE_LOCATION`   | `/tmp/maprticket_${USER}`      | path (may use `${USER}`)     | 1, 3       | Path to the maprlogin ticket file on the edge. Only consulted when `auth_method=mapr`. |
| `cluster_type`                 | `CLUSTER_TYPE`               | `MapR`                         | `MapR` / `HDP` / `CDP`       | 1, 3       | Report label + chooses S3A committer. HDP/CDP paths skip MapR-specific behaviors. |
| `hdfs_nameservice`             | `HDFS_NAMESERVICE`           | *(empty)*                      | HDFS HA nameservice name     | 1, 3       | Required for HDFS HA clusters; leave empty for MapR. |

---

## 3. S3 Configuration

| Variable                         | Env                           | Default              | Values            | Applies to | Description |
|----------------------------------|-------------------------------|----------------------|-------------------|------------|-------------|
| `migration_default_s3_bucket`    | `MIGRATION_DEFAULT_S3_BUCKET` | `s3a://data-lake`    | `s3a://` URL      | 1          | Fallback bucket when an Excel row leaves the bucket column empty. |
| `s3_endpoint`                    | `S3_ENDPOINT`                 | *(empty)*            | HTTPS URL         | All        | S3 endpoint. Empty = AWS default. Required for non-AWS S3. Used by `build_s3_opts` for every S3 interaction (DistCp, tracking table writes, checkpoints). |
| `s3_access_key`                  | `S3_ACCESS_KEY`               | *(empty)*            | string            | All        | S3 access key id. Empty = fall through to IAM role / `~/.aws/credentials`. |
| `s3_secret_key`                  | `S3_SECRET_KEY`               | *(empty)*            | string (secret)   | All        | S3 secret access key. Same fallback as above. |
| `s3_listing_tool`                | `S3_LISTING_TOOL`             | `hadoop`             | `hadoop` / `aws`  | 1          | Which CLI the report uses when listing S3. `hadoop` uses `hadoop fs -ls`; `aws` uses the AWS CLI. |

---

## 4. Tracking & Reporting

| Variable                        | Env                           | Default                                       | Values         | Applies to | Description |
|---------------------------------|-------------------------------|-----------------------------------------------|----------------|------------|-------------|
| `migration_tracking_database`   | `MIGRATION_TRACKING_DATABASE` | `migration_tracking`                          | Hive database  | All        | Database holding `migration_runs` + `migration_table_status` Iceberg tracking tables. |
| `migration_tracking_location`   | `MIGRATION_TRACKING_LOCATION` | `s3a://data-lake/migration_tracking`          | `s3a://` URL   | All        | Location of the tracking Iceberg tables. Also the default parent for DistCp and discovery checkpoints. |
| `migration_report_location`     | `MIGRATION_REPORT_LOCATION`   | `s3a://data-lake/migration_reports`           | `s3a://` URL   | All        | Where per-run HTML reports are written. |
| `migration_spark_conn_id`       | `MIGRATION_SPARK_CONN_ID`     | `spark_default`                               | Airflow conn id | All       | Spark connection used by `@task.pyspark` tasks (Kyuubi / Thrift). |

---

## 5. Discovery Behavior (DAG 1)

All variables in this section apply to **DAG 1 only** (DAG 2 has its own Iceberg
discovery that doesn't share these knobs; DAG 3 is folder-copy with no discovery).

| Variable                                      | Env                                              | Default    | Values            | Applies to | Description |
|-----------------------------------------------|--------------------------------------------------|------------|-------------------|------------|-------------|
| `migration_discovery_max_concurrent` `[parse]` | `MIGRATION_DISCOVERY_MAX_CONCURRENT`             | `3`        | positive int      | 1          | Across-database parallelism. How many mapped `discover_tables_via_spark_ssh` instances run in parallel. Each is a `local[*]` pyspark driver on the edge — edge-RAM-bounded. |
| `migration_discovery_per_table_parallelism`   | `MIGRATION_DISCOVERY_PER_TABLE_PARALLELISM`      | `8`        | int, clamped `[1,64]` | 1      | Within-one-task threadpool. How many tables walk through DESCRIBE / SHOW PARTITIONS / COUNT concurrently on the same SparkSession. Biggest lever for a single-giant-DB case. |
| `migration_discovery_spark_master`            | `MIGRATION_DISCOVERY_SPARK_MASTER`               | `local[*]` | Spark master URL  | 1          | Where the discovery pyspark driver runs. Switch to `yarn` to offload scans to the cluster and remove the edge-RAM constraint. |
| `migration_discovery_driver_memory`           | `MIGRATION_DISCOVERY_DRIVER_MEMORY`              | *(empty)*  | e.g. `4g`         | 1          | `--driver-memory` for the discovery pyspark call. Empty = Spark default. |
| `migration_discovery_checkpoint_location`     | `MIGRATION_DISCOVERY_CHECKPOINT_LOCATION`        | *(empty → `{tracking_location}/_discovery_checkpoints`)* | `s3a://` URL or empty | 1 | Where per-table discovery checkpoints are written so retries skip tables already walked. Empty = default under tracking_location. Deleted at the end of each run. |
| `migration_metastore_retry_attempts`          | `MIGRATION_METASTORE_RETRY_ATTEMPTS`             | `4`        | int               | 1          | DESCRIBE/SHOW PARTITIONS retries per table on transient HMS failures. |
| `migration_metastore_retry_backoff_seconds`   | `MIGRATION_METASTORE_RETRY_BACKOFF_SECONDS`      | `15`       | int (seconds)     | 1          | Backoff between HMS retries. Multiplied by attempt number. |
| `migration_metastore_socket_timeout`          | `MIGRATION_METASTORE_SOCKET_TIMEOUT`             | `600s`     | Hadoop duration   | 1          | `hive.metastore.client.socket.timeout`. Format: `<n>[ms\|s\|m\|h]`. |
| `migration_metastore_failure_retries`         | `MIGRATION_METASTORE_FAILURE_RETRIES`            | `5`        | int               | 1          | `hive.metastore.failure.retries` passed to Spark. |

---

## 6. DistCp Copy Behavior

### 6a. Concurrency & batching (DAG 1)

| Variable                                         | Env                                             | Default | Values                           | Applies to | Description |
|--------------------------------------------------|-------------------------------------------------|---------|----------------------------------|------------|-------------|
| `migration_distcp_copy_max_concurrent` `[parse]` | `MIGRATION_DISTCP_COPY_MAX_CONCURRENT`          | `3`     | positive int                     | 1          | How many `run_distcp_ssh` batches run in parallel. Shared YARN queue cap. Also used by `flatten_and_batch` for lane-aware batch sizing. |
| `migration_distcp_max_tables_per_batch`          | `MIGRATION_DISTCP_MAX_TABLES_PER_BATCH`         | `25`    | int                              | 1          | Cap on tables packed into one DistCp batch. |
| `migration_distcp_call_timeout_max_seconds`      | `MIGRATION_DISTCP_CALL_TIMEOUT_MAX_SECONDS`     | `21600` | int (seconds)                    | 1          | Hard ceiling on a single `hadoop distcp` invocation (6h default). |

### 6b. Auto-sizing (DAG 1, opt-in)

| Variable                                         | Env                                             | Default            | Values            | Applies to | Description |
|--------------------------------------------------|-------------------------------------------------|--------------------|-------------------|------------|-------------|
| `migration_distcp_enable_auto_sizing`            | `MIGRATION_DISTCP_ENABLE_AUTO_SIZING`           | `false`            | bool              | 1          | Master switch for per-table mapper/bandwidth sizing. When off, every table uses `distcp_default_mappers` + `distcp_default_bandwidth` (pre-auto-sizing behavior). |
| `migration_distcp_mappers`                       | `MIGRATION_DISTCP_MAPPERS`                      | *(empty = auto)*   | int or empty      | 1          | Force a fixed mapper count (only consulted when auto-sizing is on). Must be set together with `distcp_bandwidth`. |
| `migration_distcp_bandwidth`                     | `MIGRATION_DISTCP_BANDWIDTH`                    | *(empty = auto)*   | int or empty      | 1          | Force a fixed per-mapper MB/s (only consulted when auto-sizing is on). |
| `migration_distcp_default_mappers`               | `MIGRATION_DISTCP_DEFAULT_MAPPERS`              | `50`               | int               | 1          | Used when the size probe returns nothing usable, and when auto-sizing is off. |
| `migration_distcp_default_bandwidth`             | `MIGRATION_DISTCP_DEFAULT_BANDWIDTH`            | `100`              | int (MB/s)        | 1          | Per-mapper bandwidth fallback. |
| `migration_distcp_target_bytes_per_mapper`       | `MIGRATION_DISTCP_TARGET_BYTES_PER_MAPPER`      | `2147483648` (2 GiB) | int (bytes)     | 1          | Auto-sizing input. Mapper count ≈ `source_bytes / target`. |
| `migration_distcp_min_mappers`                   | `MIGRATION_DISTCP_MIN_MAPPERS`                  | `1`                | int               | 1          | Floor for auto-sized mapper count. |
| `migration_distcp_max_mappers`                   | `MIGRATION_DISTCP_MAX_MAPPERS`                  | `100`              | int               | 1          | Ceiling for auto-sized mapper count. |
| `migration_distcp_target_aggregate_mbps`         | `MIGRATION_DISTCP_TARGET_AGGREGATE_MBPS`        | `2000`             | int (MB/s)        | 1          | Aggregate throughput target the auto-sizer tries to match across all mappers. |
| `migration_distcp_max_mapper_bandwidth`          | `MIGRATION_DISTCP_MAX_MAPPER_BANDWIDTH`         | `500`              | int (MB/s)        | 1          | Per-mapper ceiling. Prevents a table with few mappers from being asked to sustain multi-GB/s per stream. |

### 6c. Low-level knobs (DAG 1 and DAG 3)

| Variable                                         | Env                                             | Default    | Values                     | Applies to | Description |
|--------------------------------------------------|-------------------------------------------------|------------|----------------------------|------------|-------------|
| `migration_distcp_strategy`                      | `MIGRATION_DISTCP_STRATEGY`                     | `dynamic`  | `dynamic` / `uniformsize`  | 1, 3       | DistCp `-strategy` flag. Empty value falls back to default (DistCp rejects a literal empty). |
| `migration_distcp_map_memory_mb`                 | `MIGRATION_DISTCP_MAP_MEMORY_MB`                | *(empty)*  | int or empty               | 1          | MapReduce task heap. Empty → not emitted. |
| `migration_distcp_map_java_opts`                 | `MIGRATION_DISTCP_MAP_JAVA_OPTS`                | *(empty)*  | JVM opts string            | 1          | Extra JVM opts for DistCp map tasks. |
| `migration_distcp_client_java_opts`              | `MIGRATION_DISTCP_CLIENT_JAVA_OPTS`             | *(empty)*  | JVM opts string            | 1, 3       | Extra JVM opts for the DistCp client (the submitter). |
| `migration_distcp_extra_hadoop_opts`             | `MIGRATION_DISTCP_EXTRA_HADOOP_OPTS`            | *(empty)*  | `-D key=val` string        | 1          | Pass-through extra `-D` flags. |
| `migration_distcp_preserve_delete`               | `MIGRATION_DISTCP_PRESERVE_DELETE`              | `true`     | bool                       | 1, 3       | Pass DistCp `-delete` on `-update` runs. Set `false` for append-only semantics. |
| `migration_distcp_checkpoint_location`           | `MIGRATION_DISTCP_CHECKPOINT_LOCATION`          | *(empty → `{tracking_location}/_distcp_checkpoints`)* | `s3a://` URL or empty | 1 | Where per-DistCp-item checkpoints land. Deleted at the end of each run. |

---

## 7. Validation (DAG 1)

| Variable                             | Env                                   | Default | Values       | Applies to | Description |
|--------------------------------------|---------------------------------------|---------|--------------|------------|-------------|
| `MIGRATION_VALIDATION_MAX_CONCURRENT` *(env-only)* `[parse]` | `MIGRATION_VALIDATION_MAX_CONCURRENT` | `3`     | positive int | 1          | How many `validate_destination_tables` task instances run in parallel. **Not Variable-tunable** — set via env in `env.shared` and redeploy. |

---

## 8. Hive Table Creation (DAG 1)

| Variable                       | Env                           | Default | Values  | Applies to | Description |
|--------------------------------|-------------------------------|---------|---------|------------|-------------|
| `migration_recreate_tables`    | `MIGRATION_RECREATE_TABLES`   | `false` | bool    | 1          | When true, `create_hive_tables` DROPs the existing destination table (metadata only — EXTERNAL tables keep their S3 data) and recreates it. Used to fix tables with wrongly-typed partition columns. Set back to false after one run. Refuses to drop Iceberg tables. |

---

## 9. Path Structure (DAG 1)

| Variable                                   | Env                                           | Default | Values  | Applies to | Description |
|--------------------------------------------|-----------------------------------------------|---------|---------|------------|-------------|
| `migration_include_db_in_path`             | `MIGRATION_INCLUDE_DB_IN_PATH`                | `true`  | bool    | 1          | When true, dest path is `{bucket}/{database}/{table}`. When false, `{bucket}/{table}` (database folder omitted). |
| `migration_use_source_relative_path`       | `MIGRATION_USE_SOURCE_RELATIVE_PATH`          | `false` | bool    | 1          | When true, dest path mirrors the source path under the bucket. Overrides `include_db_in_path`. |
| `migration_source_path_strip_prefixes`     | `MIGRATION_SOURCE_PATH_STRIP_PREFIXES`        | *(empty)* | comma-separated list of path prefixes | 1 | Prefixes stripped from the source path before using it as the dest path. Only consulted when `use_source_relative_path=true`. |

---

## 10. Iceberg Conversion (DAG 2)

| Variable                              | Env                                       | Default | Values  | Applies to | Description |
|---------------------------------------|-------------------------------------------|---------|---------|------------|-------------|
| `migration_iceberg_drop_backup`       | `MIGRATION_ICEBERG_DROP_BACKUP`           | `false` | bool    | 2          | Drop the `_backup_` table after a successful in-place Iceberg migration. Also overridable per-run via DAG config. |
| `migration_iceberg_inplace_text_ctas` | `MIGRATION_ICEBERG_INPLACE_TEXT_CTAS`     | `true`  | bool    | 2          | For TEXTFILE source tables, do CTAS-swap instead of in-place `migrate` procedure (Iceberg's migrate doesn't support TEXTFILE on all releases). Also overridable per-run via DAG config. |

---

## 11. Folder Copy (DAG 3)

| Variable                              | Env                                       | Default | Values  | Applies to | Description |
|---------------------------------------|-------------------------------------------|---------|---------|------------|-------------|
| `migration_folder_copy_allow_delete`  | `MIGRATION_FOLDER_COPY_ALLOW_DELETE`      | `false` | bool    | 3          | When true, DAG 3's folder copy may remove destination files that no longer exist at the source (DistCp `-delete`). Default protects against accidental data loss on first-time cutovers. |

---

## 12. Email & Reporting

| Variable                       | Env                           | Default        | Values                     | Applies to | Description |
|--------------------------------|-------------------------------|----------------|----------------------------|------------|-------------|
| `migration_smtp_conn_id`       | `MIGRATION_SMTP_CONN_ID`      | `smtp_default` | Airflow conn id            | All        | SMTP connection used to send the per-run report email. |
| `migration_email_recipients`   | `MIGRATION_EMAIL_RECIPIENTS`  | *(empty)*      | comma-separated addresses  | All        | Who receives the per-run report. Empty = no email. |

---

## 13. DAG Owner

| Variable                 | Env                     | Default           | Values              | Applies to | Description |
|--------------------------|-------------------------|-------------------|---------------------|------------|-------------|
| `migration_dag_owner` `[parse]` | `MIGRATION_DAG_OWNER`   | `data-migration`  | single Unix username (no commas) | All | Owner shown in Airflow AND the task pod's `HADOOP_USER_NAME` / `SPARK_USER` via the `pod_mutation_hook`. This is the principal Ranger authorizes for direct S3/HDFS access. Blank or comma-bearing values fall through to the literal injected by `deploy.py --owner`. **Not run-scoped**: one value per DAG, refreshed each parse. |

---

## Parse-time Variables (allowlist)

These are read at DAG parse time, so each costs a metadata-DB round trip per
scheduler parse loop (~30s). Kept small and explicit — adding a new one is a
deliberate decision (guarded by `test_parse_path_variable_reads_are_in_the_allowlist`).

- `migration_dag_owner` — All DAGs
- `migration_discovery_max_concurrent` — DAG 1
- `migration_distcp_copy_max_concurrent` — DAG 1

A failure reading any of these (bad int, DB blip, non-positive) silently falls
through to env/default — a bad Variable can never drop the DAG from Airflow.

---

## Tuning quick-reference (DAG 1)

**Slow discovery against one giant database?**
→ raise `migration_discovery_per_table_parallelism` (try `16` on a healthy
Hive metastore). Default `8` is safe for MapR Hive 1.2.

**Slow discovery across many Excel databases?**
→ raise `migration_discovery_max_concurrent`. Each extra concurrent discovery
is one extra `local[*]` pyspark on the edge, so verify `free -g` headroom.

**Metastore throwing `TTransportException`?**
→ lower `migration_discovery_per_table_parallelism` to `4` (fewer concurrent
HMS clients) and/or raise `migration_metastore_retry_attempts`.

**DistCp jobs queueing in YARN?**
→ lower `migration_distcp_copy_max_concurrent`, or widen the YARN queue.

**Want to migrate append-only (preserve dest files on source deletes)?**
→ `migration_distcp_preserve_delete=false`.

**Fixing wrongly-typed partition columns from an older DAG version?**
→ one run with `migration_recreate_tables=true`, then set back to `false`.

See also `README.md` → "Tuning DAG 1 performance" for the operational walk-through.
