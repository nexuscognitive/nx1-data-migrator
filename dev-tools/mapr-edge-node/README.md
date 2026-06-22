# MapR Edge Node — Dev Setup

Docker-based environment that simulates a MapR Hadoop 2.7.0-mapr-1808 edge node
for end-to-end local testing of the `source_to_s3_migration` DAG.

MapR's distribution is not publicly available, so this image uses the Apache
upstream versions that MapR 2.7.0-mapr-1808 (MEP 5.0) is built on. The Hadoop
APIs used by the DAG — Hive metastore queries, `hadoop distcp`, and HDFS FS
commands — are identical between the two, making this image functionally
equivalent for testing discover and distcp tasks.

---

## Stack

| Component | This Image                            | Client (MapR)         |
| --------- | ------------------------------------- | --------------------- |
| OS        | Ubuntu 18.04                          | —                     |
| Java      | OpenJDK 8                             | —                     |
| Hadoop    | 2.7.7                                 | 2.7.0-mapr-1808       |
| Hive      | 1.2.2                                 | MEP 5.0 (Hive 1.2.x)  |
| Spark     | 2.3.4                                 | MEP 5.0 (Spark 2.3.x) |
| Python    | 2.7 (default) / 3 (setup script only) | —                     |
| SSH port  | 2223 (user: `root`, password: `root`) | —                     |

---

## Prerequisites

- **Windows + WSL 2** (Ubuntu) — all commands run in a WSL terminal
- **Docker Desktop** with WSL 2 integration enabled
- At least **8 GB RAM** and **20 GB free disk** allocated to Docker

---

## Directory Structure

```
dev-tools/mapr-edge-node/
├── Dockerfile
├── docker-compose.yml
├── README.md
├── conf/
│   ├── core-site.xml        # HDFS + S3A filesystem registration
│   ├── hdfs-site.xml
│   ├── mapred-site.xml      # local framework (YARN blocked in Docker, see Troubleshooting)
│   ├── yarn-site.xml
│   └── hive-site.xml
└── scripts/
    ├── entrypoint.sh        # container startup — starts SSH, HDFS, YARN, Hive metastore thrift, HiveServer2
    ├── bootstrap-hdfs.sh    # first-run only — formats namenode, inits Hive metastore schema
    ├── profile-additions.sh # sets PATH/PYTHONPATH/HADOOP_CLASSPATH for all SSH sessions
    ├── setup-test-data.sh   # populates HDFS + Hive with test databases and tables
    ├── test-discover.sh     # runs the exact PySpark discover script from the DAG
    └── test-distcp.sh       # runs the exact distcp shell command from the DAG
```

---

## Quick Start

All commands run from a terminal in the `dev-tools/mapr-edge-node/` directory.
On Windows use PowerShell; on Linux/Mac use bash.

### Step 1 — Build the image

> First build downloads ~1.2 GB and takes 20–40 minutes.
> Subsequent builds use Docker layer cache and finish in under 30 seconds.

```powershell
docker compose build
```

If the build fails with a network or extraction error, clear the build cache first:

```powershell
docker builder prune -f
docker compose build --no-cache
```

### Step 2 — Start the container

```powershell
docker compose up -d
```

Wait ~90 seconds for HiveServer2 to be ready, then check:

```powershell
docker logs mapr-edge-node --tail 10
```

Expected output:

```
===============================
MapR-equivalent edge node ready!
Hadoop: 2.7.7 (simulates MapR 2.7.0-mapr-1808)
Hive:   1.2.2
Spark:  2.3.4
SSH:  ssh root@localhost -p 2223  (password: root)
HDFS: http://localhost:9871
YARN: http://localhost:8089
===============================
```

If the banner never appears after 2 minutes, see [Troubleshooting](#troubleshooting).

### Step 3 — Verify services

```powershell
docker exec -u root mapr-edge-node hdfs dfsadmin -report
# Expected: Live datanodes (1)

docker exec -u root mapr-edge-node netstat -tlnp 2>/dev/null | grep -e 9083 -e 10000
# Expected: both ports listening (9083 = metastore thrift, 10000 = HiveServer2)

docker exec -u root mapr-edge-node /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true -e "SHOW DATABASES;"
# Expected: default
```

### Step 4 — Populate test data

Run once after starting the container (or after any full reset):

```powershell
docker exec -u root mapr-edge-node bash /setup-test-data.sh
```

This creates 17 databases and 57 tables covering all migration DAG test scenarios,
`hive_type_to_spark_ddl` struct/complex-type cases, and the error-based-retry
regression/transient/permanent test suite. See [Test Data Reference](#test-data-reference)
below for the full table inventory.

### Step 5 — SSH into the container (optional)

```powershell
# Clear stale host key if you rebuilt the image
ssh-keygen -f "$env:USERPROFILE\.ssh\known_hosts" -R '[localhost]:2223'

ssh root@localhost -p 2223
# password: root
```

---

## Connecting to Airflow for End-to-End Testing

The nx1 tenant Airflow runs in Kubernetes and cannot reach `localhost:2223`
directly. Use a TCP tunnel to expose the container's SSH port publicly.

### Step 1 — Install bore (one time, in WSL)

```bash
curl -sSL https://github.com/ekzhang/bore/releases/download/v0.5.0/bore-v0.5.0-x86_64-unknown-linux-musl.tar.gz \
  | sudo tar -xz -C /usr/local/bin
```

### Step 2 — Start the tunnel

Open a dedicated terminal and keep it running throughout your test session:

```bash
bore local 2223 --to bore.pub
# Output: listening at bore.pub:XXXXX  ← note this port number
```

The port number changes every time you restart bore. Update the Airflow connection each time.

### Step 3 — Configure the Airflow SSH connection

In **Airflow UI → Admin → Connections**, find or create `cluster_edge_ssh`:

| Field     | Value                                                                             |
| --------- | --------------------------------------------------------------------------------- |
| Conn Type | SSH                                                                               |
| Host      | `bore.pub`                                                                        |
| Port      | `XXXXX` (the number from bore output)                                             |
| Username  | `root`                                                                            |
| Password  | `root`                                                                            |
| Extra     | `{"timeout": "30", "no_host_key_check": "true", "allow_host_key_change": "true"}` |

### Step 4 — Set Airflow Variables

In **Airflow UI → Admin → Variables**, set these before triggering a DAG run:

| Key                        | Value                      |
| -------------------------- | -------------------------- |
| `auth_method`              | `mapr`                     |
| `mapr_user`                | `root`                     |
| `mapr_ticketfile_location` | `/tmp/maprticket`          |
| `s3_access_key`            | Your AWS Access Key ID     |
| `s3_secret_key`            | Your AWS Secret Access Key |
| `migration_distcp_mappers` | `1`                        |

> `maprlogin` and `/tmp/maprticket` are baked into the image. No manual
> ticket creation steps are needed.

### Step 5 — Trigger the DAG

Trigger `source_to_s3_migration` with your Excel config pointing at the test
data in this container. The DAG will SSH into the container via bore, run the
discover PySpark script, and execute distcp.

> Update the Airflow connection port every time you restart the bore tunnel.

---

## Testing Discover and DistCp Directly

The test scripts bypass Airflow entirely and run the **exact same code** the
DAG executes over SSH — no wrappers, no reimplementations. This lets you
validate any change made to the DAG's `discover_tables_via_spark_ssh` or
`run_distcp_ssh` task before triggering a full DAG run.

### How to update the scripts when you change the DAG

**For discover changes** — copy the updated `pyspark_script` heredoc from
`discover_tables_via_spark_ssh()` in `migration_dag_mapr_to_s3.py` into the
`PYSPARK_SCRIPT_EOF` block in `test-discover.sh`. The parameter mapping is
documented in the script header.

**For distcp changes** — copy the updated `cmd` string from `run_distcp_ssh()`
into the matching `CMD_EOF` block (Branch A for partition filter active, Branch
B for full table) in `test-distcp.sh`. The mapping of Python f-string keys to
shell variables is documented in the script header.

---

### test-discover.sh

Generates `discover_tables.py` with your parameters substituted in and runs it
with `pyspark < discover_tables.py` — the exact command the DAG sends over SSH.
Also writes `partition_utils.py` alongside it, exactly as the DAG does via SFTP.

**Signature:**

```powershell
docker exec -u root mapr-edge-node bash /test-discover.sh `
  <src_db> <tables> <dest_db> <dest_bucket> [partition_filter]
```

No special quoting needed — `tables` accepts comma-separated names or `*`.

**Examples:**

Single table:

```powershell
docker exec -u root mapr-edge-node bash /test-discover.sh sales_db orders sales_db s3a://my-bucket 2>$null
```

All tables in a database (wildcard):

```powershell
docker exec -u root mapr-edge-node bash /test-discover.sh hr_db "*" hr_db s3a://my-bucket 2>$null
```

Multiple specific tables:

```powershell
docker exec -u root mapr-edge-node bash /test-discover.sh sales_db orders,customers sales_db s3a://my-bucket 2>$null
```

Table with partition filter:

```powershell
docker exec -u root mapr-edge-node bash /test-discover.sh logs_db app_logs logs_db s3a://my-bucket dt>=2024-01-15 2>$null
```

3-level partition:

```powershell
docker exec -u root mapr-edge-node bash /test-discover.sh analytics_db events analytics_db s3a://my-bucket 2>$null
```

> The `2>$null` suffix suppresses Spark INFO/WARN log noise. Remove it if you
> need to see the full Spark output for debugging.

**What to look for in the output:**

The script prints a JSON block between `===JSON_START===` and `===JSON_END===`.
This is the exact metadata dict the DAG parses to drive distcp and table
creation. Check for:

- `source_location` — the HDFS path the DAG will distcp from
- `partitions` — the list after partition filter is applied
- `filtered_partitions` — should exclude partitions that don't match the filter
- `partition_filter_active` — `true` when the filter reduced the partition list
- `unregistered_partitions` — `true` for `analytics_db.sessions`
- `serde_properties` — `field.delim` and `null.format` for TEXTFILE tables
- `file_format`, `schema`, `row_count`, `source_total_size_bytes`

---

### test-distcp.sh

Runs the exact `cmd` string that `run_distcp_ssh()` sends over SSH, with your
parameters substituted in. Supports both the full-table path and the
per-partition path.

> **Note:** DistCp runs with `-Dmapreduce.framework.name=local` on this
> container, set via `conf/mapred-site.xml`. The DAG also includes this flag
> in its distcp commands when targeting this container. Real MapR clusters are
> unaffected because their YARN NodeManager does not run as root.

**Signature:**

```powershell
docker exec -u root mapr-edge-node bash /test-distcp.sh `
  <source_location> <s3_location> [mappers] [bandwidth] [partition_filter_active] [filtered_partitions_json]
```

**Examples:**

Full table copy, HDFS → HDFS (local testing, no AWS creds needed):

```powershell
docker exec -u root mapr-edge-node bash /test-distcp.sh `
  hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders `
  hdfs://localhost:9000/tmp/distcp-test/orders 2>$null
```

Run the same command again to test the incremental (`-update -delete`) path:

```powershell
docker exec -u root mapr-edge-node bash /test-distcp.sh `
  hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders `
  hdfs://localhost:9000/tmp/distcp-test/orders 2>$null
```

Partition filter active — write the JSON array via Python first to avoid
PowerShell quoting issues, then pass it:

```powershell
docker exec -u root mapr-edge-node python2.7 -c "import json; open('/tmp/parts.json','w').write(json.dumps(['dt=2024-01-15','dt=2024-02-01'],separators=(',',':')))"
docker exec -u root mapr-edge-node bash -c 'PARTS=$(cat /tmp/parts.json) && bash /test-distcp.sh hdfs://localhost:9000/user/hive/warehouse/logs_db.db/app_logs hdfs://localhost:9000/tmp/distcp-test/app_logs 1 100 true "$PARTS"'
```

Real S3 destination — pass AWS credentials via `-e`:

```powershell
docker exec -u root `
  -e AWS_ACCESS_KEY_ID=AKIA... `
  -e AWS_SECRET_ACCESS_KEY=... `
  -e S3_ENDPOINT=https://s3.amazonaws.com `
  mapr-edge-node bash /test-distcp.sh `
  hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders `
  s3a://your-bucket/dest-db/orders
```

**What to look for in the output:**

- `INCREMENTAL=true/false` — whether the destination already existed
- `DISTCP_EXIT_CODE=0` — must be 0 for success
- `S3_FILE_COUNT_BEFORE/AFTER` — file counts at destination before and after
- `S3_TOTAL_SIZE_BEFORE/AFTER` — byte totals before and after
- `S3_FILES_TRANSFERRED`, `S3_BYTES_TRANSFERRED` — delta metrics
- `BYTES_COPIED`, `FILES_COPIED` — from distcp's own counters (full-table path only)

---

## Useful Commands

| Task                   | Command                                                       |
| ---------------------- | ------------------------------------------------------------- |
| Start container        | `docker compose up -d`                                        |
| Stop (keep data)       | `docker compose down`                                         |
| Full reset (wipe data) | `docker compose down -v && docker compose up -d`              |
| View logs              | `docker logs -f mapr-edge-node`                               |
| Restart                | `docker restart mapr-edge-node`                               |
| Run a command          | `docker exec -u root mapr-edge-node <cmd>`                    |
| Populate test data     | `docker exec -u root mapr-edge-node bash /setup-test-data.sh` |
| HDFS Web UI            | http://localhost:9871                                         |
| YARN Web UI            | http://localhost:8089                                         |

---

## Test Data Reference

`setup-test-data.sh` creates 17 databases with 57 tables. Each table is designed
to exercise a specific scenario in the migration DAG, the `hive_type_to_spark_ddl`
struct/complex-type converter, or the error-based-retry (transient/permanent
failure) test suite.

## PART A — Migration DAG scenarios + struct/complex-type tests

### `sales_db`

| Table          | Format   | Partitioned | Partition Keys  | Rows | Tests                                                                                    |
| -------------- | -------- | ----------- | --------------- | ---- | ---------------------------------------------------------------------------------------- |
| `orders`       | TEXTFILE | Yes         | `year`, `month` | 10   | Partitioned table, 3 partitions — exact filter, `>=` comparison, all-partition migration |
| `customers`    | TEXTFILE | No          | —               | 5    | Non-partitioned CSV (`,` delimiter)                                                      |
| `transactions` | TEXTFILE | No          | —               | 4    | Pipe-delimited (`\|`) — tests `field.delim` serde property                               |
| `orders_empty` | TEXTFILE | Yes         | `year`, `month` | 0    | EMPTY_SOURCE — zero files, no registered partitions                                      |

### `hr_db`

| Table                     | Format   | Partitioned | Partition Keys | Rows | Tests                                                                  |
| ------------------------- | -------- | ----------- | -------------- | ---- | ---------------------------------------------------------------------- |
| `employees`               | TEXTFILE | No          | —              | 5    | `\N` null values — tests `serialization.null.format` serde property    |
| `departments`             | TEXTFILE | No          | —              | 0    | EMPTY_SOURCE — empty non-partitioned table                             |
| `employees_parquet`       | PARQUET  | No          | —              | 5    | PARQUET format — schema inferred from files                            |
| `employees_avro`          | AVRO     | No          | —              | 3    | AVRO format — tests `STORED AS AVRO` DDL                               |
| `employees_parquet_empty` | PARQUET  | No          | —              | 0    | EMPTY_SOURCE for PARQUET — schema read from metastore, not file footer |

> `hr_db` is used for the **wildcard `*` test** — discovers all tables in one call.

### `analytics_db`

| Table                              | Format   | Partitioned | Partition Keys            | Rows | Tests                                                                                                                          |
| ---------------------------------- | -------- | ----------- | ------------------------- | ---- | ------------------------------------------------------------------------------------------------------------------------------ |
| `events`                           | TEXTFILE | Yes         | `region`, `year`, `month` | 9    | 3-level partition, 4 partitions (3 US + 1 EU) — wildcard filter `region=US/*` and exact 3-level filter                         |
| `sessions`                         | TEXTFILE | Yes         | `year`, `month`           | 4    | **Unregistered partitions** — data exists in HDFS but not registered in metastore. DAG must set `unregistered_partitions=True` |
| `events_orc`                       | ORC      | Yes         | `region`, `year`, `month` | 5    | ORC format — tests `STORED AS ORC` DDL                                                                                         |
| `events_empty_partitioned`         | TEXTFILE | Yes         | `region`, `year`, `month` | 0    | EMPTY_SOURCE — 3-level partition key, registered partitions, zero files                                                        |
| `events_parquet_empty_partitioned` | PARQUET  | Yes         | `region`, `year`, `month` | 0    | EMPTY_SOURCE for PARQUET partitioned table — same shape as above, Parquet-specific code path                                   |

### `logs_db`

| Table                          | Format   | Partitioned | Partition Keys | Rows | Tests                                                                     |
| ------------------------------ | -------- | ----------- | -------------- | ---- | ------------------------------------------------------------------------- |
| `app_logs`                     | TEXTFILE | Yes         | `dt`           | 7    | 3 date partitions — used for all partition filter expression types        |
| `error_logs_empty`             | TEXTFILE | Yes         | `dt`           | 0    | EMPTY_SOURCE — most common real-world case: log table never received data |
| `metrics_empty_nonpartitioned` | TEXTFILE | No          | —              | 0    | EMPTY_SOURCE — simplest case: flat table, no partition logic              |

### `struct_db`

Covers the `hive_type_to_spark_ddl` converter (colon-to-space conversion for
nested Hive type strings) and full-DDL integration tests against a
production-scale replica table.

| Table                    | Format  | Partitioned                      | Rows | Tests                                                                           |
| ------------------------ | ------- | -------------------------------- | ---- | ------------------------------------------------------------------------------- |
| `simple_struct`          | PARQUET | No                               | 3    | `struct<a:int,b:string>` — basic colon removal                                  |
| `array_of_struct`        | PARQUET | No                               | 3    | `array<struct<x:int,y:string>>`                                                 |
| `map_of_struct`          | PARQUET | No                               | 3    | `map<string,struct<a:int>>`                                                     |
| `deep_nested_struct`     | PARQUET | No                               | 3    | `struct<a:array<struct<b:map<string,int>>>>` — multi-level nesting              |
| `array_of_array_struct`  | PARQUET | No                               | 3    | `array<array<struct<x:int>>>`                                                   |
| `struct_with_decimal`    | PARQUET | No                               | 3    | `struct<amount:decimal(18,4),qty:int>` — decimal comma must not split fields    |
| `struct_with_varchar`    | PARQUET | No                               | 3    | `struct<name:varchar(255),code:char(10)>` — parameterised types pass through    |
| `already_converted`      | PARQUET | No                               | 3    | Idempotency — already-space-separated struct is left unchanged                  |
| `mixed_case_struct`      | PARQUET | No                               | 3    | `STRUCT<A:INT,B:STRING>` — case-insensitive matching                            |
| `flex_rules_result_mini` | PARQUET | No                               | 3    | Realistic nested struct + array-of-struct (mini production replica)             |
| `flex_rules_result_full` | PARQUET | Yes (`capbusinesseffectivedate`) | 4    | Full production-width struct DDL — integration test for `CREATE EXTERNAL TABLE` |

---

## PART B — Error-based-retry regression and retry-policy suite

> **Naming note:** `hr_db`, `sales_db`, `analytics_db`, and `logs_db` already
> exist in PART A with different, incompatible schemas. PART B's equivalents
> are suffixed `_tz` (`hr_db_tz`, `sales_db_tz`, `analytics_db_tz`,
> `logs_db_tz`) so both test suites can coexist in the same metastore without
> one overwriting the other.

### `migration_db`

| Table          | Format  | Partitioned | Rows | Tests                                   |
| -------------- | ------- | ----------- | ---- | --------------------------------------- |
| `customers`    | PARQUET | No          | 10   | Basic non-partitioned regression case   |
| `products`     | PARQUET | No          | 10   | Basic non-partitioned, no NULLs         |
| `transactions` | PARQUET | No          | 15   | Non-partitioned with `TIMESTAMP` column |

### `metrics_db`

| Table                | Format  | Partitioned | Rows | Tests                             |
| -------------------- | ------- | ----------- | ---- | --------------------------------- |
| `daily_active_users` | PARQUET | No          | 4    | Wildcard `*` migration (3 tables) |
| `conversion_rates`   | PARQUET | No          | 4    | Wildcard `*` migration            |
| `revenue_summary`    | PARQUET | No          | 4    | Wildcard `*` migration            |

### `audit_db`

| Table        | Format  | Partitioned | Rows | Tests                           |
| ------------ | ------- | ----------- | ---- | ------------------------------- |
| `access_log` | PARQUET | No          | 5    | Comma-separated table list test |
| `change_log` | PARQUET | No          | 4    | Comma-separated table list test |

### `hr_db_tz`

| Table         | Format  | Partitioned | Rows | Tests                                  |
| ------------- | ------- | ----------- | ---- | -------------------------------------- |
| `employees`   | PARQUET | No          | 6    | Explicit `dest_database` override test |
| `departments` | PARQUET | No          | 4    | Explicit `dest_database` override test |

### `sales_db_tz`

| Table           | Format  | Partitioned | Partition Keys  | Rows | Tests                                         |
| --------------- | ------- | ----------- | --------------- | ---- | --------------------------------------------- |
| `orders`        | PARQUET | Yes         | `dt`            | 9    | `dt` STRING partition, 3 partitions           |
| `returns`       | PARQUET | Yes         | `dt`            | 4    | `dt` STRING partition, range filter test      |
| `daily_summary` | PARQUET | Yes         | `year`, `month` | 6    | INT `year`/`month` partition, `last_n` filter |

### `analytics_db_tz`

| Table      | Format  | Partitioned | Partition Keys            | Rows | Tests                        |
| ---------- | ------- | ----------- | ------------------------- | ---- | ---------------------------- |
| `events`   | PARQUET | Yes         | `region`, `year`, `month` | 9    | 3-level partition key        |
| `sessions` | PARQUET | Yes         | `year`, `month`           | 6    | INT `year`/`month` partition |

### `logs_db_tz`

| Table      | Format  | Partitioned | Partition Keys | Rows | Tests                  |
| ---------- | ------- | ----------- | -------------- | ---- | ---------------------- |
| `app_logs` | PARQUET | Yes         | `dt`           | 10   | `dt` range filter test |

### `edge_cases_db`

| Table           | Format  | Partitioned | Rows | Tests                                    |
| --------------- | ------- | ----------- | ---- | ---------------------------------------- |
| `empty_table`   | PARQUET | No          | 0    | EMPTY_SOURCE                             |
| `nulls_table`   | PARQUET | No          | 5    | NULLs across every supported column type |
| `complex_types` | PARQUET | No          | 3    | `STRUCT`/`ARRAY`/`MAP` columns           |
| `wide_table`    | PARQUET | No          | 5    | Wide table — 50 columns                  |

### `formats_db`

| Table           | Format   | Partitioned | Rows | Tests                     |
| --------------- | -------- | ----------- | ---- | ------------------------- |
| `parquet_table` | PARQUET  | No          | 5    | PARQUET format (explicit) |
| `orc_table`     | ORC      | No          | 5    | ORC format                |
| `text_table`    | TEXTFILE | No          | 5    | TEXTFILE format           |

### `tz_db`

Timezone/`TIMESTAMP` round-trip tests. All source timestamps use a fixed
`17:00:00` local time to surface UTC-offset bugs in the migration path.

| Table                 | Format  | Partitioned | Partition Keys  | Rows | Tests                                |
| --------------------- | ------- | ----------- | --------------- | ---- | ------------------------------------ |
| `events_with_ts_la`   | PARQUET | No          | —               | 8    | `TIMESTAMP` columns, non-partitioned |
| `orders_with_ts_la`   | PARQUET | Yes         | `dt`            | 8    | `TIMESTAMP` columns + `dt` partition |
| `sessions_with_ts_la` | PARQUET | Yes         | `year`, `month` | 6    | `TIMESTAMP` columns + `year`/`month` |

### `retry_test_db`

| Table            | Format  | Partitioned | Rows | Tests                                             |
| ---------------- | ------- | ----------- | ---- | ------------------------------------------------- |
| `yarn_oom_table` | PARQUET | No          | 20   | Transient retry: simulated YARN OOM during DistCp |

### `corrupt_test_db`

Multi-table batch test: one table is corrupted **after** DAG1 copies it to S3
(see script comments), the other two must still complete successfully.

| Table           | Format  | Partitioned | Rows | Tests                                                          |
| --------------- | ------- | ----------- | ---- | -------------------------------------------------------------- |
| `table_a`       | PARQUET | No          | 3    | Healthy table, processed before the corrupt one                |
| `corrupt_table` | PARQUET | No          | 2    | Corrupted post-copy — must FAIL permanently, no retries        |
| `table_c`       | PARQUET | No          | 4    | Healthy table, processed after the corrupt one (key assertion) |

> `migration_db.nonexistent_tbl` and `does_not_exist_db.*` are intentionally
> **not** created — they test `TABLE_NOT_FOUND` / `DATABASE_NOT_FOUND` error
> handling. |

### Partition Keys Note

Hive partition specs use **unpadded integers** (`month=1`, `month=2`,
`month=3`). The HDFS directories match exactly. Using zero-padded paths
(`month=01`) would create a mismatch between the metastore partition spec and
the HDFS location, causing `COUNT(*) = 0` for all partitioned tables.

### Partition Filter Expressions

These all apply to `logs_db.app_logs` (partitions: `dt=2024-01-01`, `dt=2024-01-15`, `dt=2024-02-01`):

| Expression                     | Partitions matched               | Type        |
| ------------------------------ | -------------------------------- | ----------- |
| `dt=2024-01-01`                | `dt=2024-01-01` only             | Exact match |
| `dt>=2024-01-15`               | `dt=2024-01-15`, `dt=2024-02-01` | Comparison  |
| `last_n_partitions=2`          | `dt=2024-01-15`, `dt=2024-02-01` | Last N      |
| `dt=2024-01-01, dt=2024-02-01` | `dt=2024-01-01`, `dt=2024-02-01` | Multi-term  |
| _(blank)_                      | All 3                            | No filter   |

For `analytics_db.events`:

| Expression                    | Partitions matched  | Type          |
| ----------------------------- | ------------------- | ------------- |
| `region=US/*`                 | All 3 US partitions | Wildcard      |
| `region=EU/year=2024/month=1` | EU partition only   | Exact 3-level |

---

## Troubleshooting

### Container never prints the ready banner

HiveServer2 takes ~90 seconds to start after a fresh build because the metastore
thrift service must bind on port 9083 before HiveServer2 can connect to it.
Check if both are up:

```powershell
docker exec -u root mapr-edge-node netstat -tlnp 2>/dev/null | grep -e 9083 -e 10000
```

If only 9083 is shown, HiveServer2 is still starting — wait another 30 seconds.
If neither port is shown after 3 minutes, the HDFS volume may be corrupted. Wipe it:

```powershell
docker compose down -v
docker compose up -d
```

### Hive errors after `docker restart`

After a restart the container takes ~90 seconds to become ready, not 30. The
metastore thrift service must start and bind to 9083 before HiveServer2 can
start. Wait for both ports to appear in netstat before running tests:

```powershell
docker exec -u root mapr-edge-node netstat -tlnp 2>/dev/null | grep -e 9083 -e 10000
```

**Never kill HiveServer2 manually.** If you do, restart the whole container
with `docker restart mapr-edge-node` and wait ~90 seconds. Manually restarting
just HiveServer2 does not work in this container because the entropy and startup
environment are only set correctly by the entrypoint.

### `Database 'X' not found` in PySpark discover

Test data has not been populated. Run:

```powershell
docker exec -u root mapr-edge-node bash /setup-test-data.sh
```

### `Unable to instantiate SessionHiveMetaStoreClient` in PySpark

The metastore thrift service on port 9083 is not running. This should not
happen after a normal start, but if it does, restart the container:

```powershell
docker restart mapr-edge-node
```

Wait 90 seconds, verify both 9083 and 10000 are listening, then re-run setup.

### YARN container launch failure in distcp

```
AM Container for appattempt_XXX exited with exitCode: 1
```

YARN cannot launch MapReduce containers when the NodeManager runs as root
(Hadoop 2.7.x `DefaultContainerExecutor` hard block in `ContainerLaunch.java`).
This image sets `mapreduce.framework.name=local` in `conf/mapred-site.xml` so
DistCp always runs in-process without needing YARN. The DAG's distcp commands
include `-Dmapreduce.framework.name=local` when targeting this container. Real
MapR clusters run YARN as a non-root service user and are unaffected.

### `hadoop fs -ls s3a://...` fails with `ClassNotFoundException`

The Hadoop FsShell does not auto-load `share/hadoop/tools/lib/*`. The image
sets `HADOOP_CLASSPATH` in `profile-additions.sh` (copied to
`/etc/profile.d/hadoop.sh`) and bakes it into `/root/.profile`, so this should
not occur after a normal image build. If it appears, run:

```bash
source /root/.profile
hadoop fs -Dfs.s3a.access.key=... -Dfs.s3a.secret.key=... -ls s3a://your-bucket/
```

### Row counts show 0 for partitioned tables

This is caused by a mismatch between HDFS path format and Hive partition specs.
Hive registers partitions as `month=1` (integer, unpadded). If HDFS directories
use `month=01`, the partition points to a non-existent path and `COUNT(*)` returns 0.
Always use unpadded integers in HDFS paths. The `setup-test-data.sh` script
already does this correctly.

### PySpark exits with `ClassNotFoundException` or `NoSuchMethodError`

The Spark and Hive JAR versions in this image are pinned to match MapR MEP 5.0.
Do not modify the versions in the Dockerfile without testing the combination.

### Script fails with `\r: command not found`

Windows line endings crept in. Fix all scripts at once:

```powershell
docker exec -u root mapr-edge-node bash -c 'sed -i "s/\r//" /test-discover.sh /test-distcp.sh /setup-test-data.sh /entrypoint.sh'
```

### bore tunnel drops during a DAG run

The DAG's SSH connection will time out. Restart bore, update the Airflow
connection with the new port, and re-trigger the DAG from the failed task.

### Passing JSON partition arrays from PowerShell

PowerShell strips double-quotes from arguments before passing them to external
programs. To pass a JSON array to `test-distcp.sh`, write it via Python first:

```powershell
docker exec -u root mapr-edge-node python2.7 -c "import json; open('/tmp/parts.json','w').write(json.dumps(['dt=2024-01-15','dt=2024-02-01'],separators=(',',':')))"
docker exec -u root mapr-edge-node bash -c 'PARTS=$(cat /tmp/parts.json) && bash /test-distcp.sh <source> <dest> 1 100 true "$PARTS"'
```

### Running alongside other edge node containers

All three edge node containers use different host ports and can run simultaneously:

| Container          | SSH  | HDFS UI | YARN UI | HiveServer2 |
| ------------------ | ---- | ------- | ------- | ----------- |
| `hadoop-edge-node` | 2222 | 9870    | 8088    | 10000       |
| `mapr-edge-node`   | 2223 | 9871    | 8089    | 10001       |
| `hdp-edge-node`    | 2224 | 9872    | 8090    | 10002       |
