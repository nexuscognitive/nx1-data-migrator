# Local Hadoop Edge Node — Dev Setup

Docker-based environment that replicates a Hadoop/Hive/PySpark edge node
for end-to-end local testing of the `data-iceberg-migrator` DAGs against
the nx1 tenant platform.

---

## Stack

| Component | Version                               |
| --------- | ------------------------------------- |
| OS        | Ubuntu 18.04                          |
| Java      | OpenJDK 8                             |
| Hadoop    | 2.10.2                                |
| Hive      | 2.3.9                                 |
| Spark     | 2.4.8                                 |
| Python    | 2.7.17                                |
| SSH port  | 2222 (user: `root`, password: `root`) |

---

## Prerequisites

- **Windows + WSL 2** (Ubuntu) — all commands run in a WSL terminal
- **Docker Desktop** with WSL 2 integration enabled
- At least **8 GB RAM** and **20 GB free disk** allocated to Docker

---

## Directory Structure

```
dev-tools/hadoop-edge-node/
├── Dockerfile
├── docker-compose.yml
├── README.md
├── conf/
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── mapred-site.xml
│   ├── yarn-site.xml
│   └── hive-site.xml
└── scripts/
    ├── entrypoint.sh        # container startup — starts SSH, HDFS, YARN, HiveServer2
    ├── bootstrap-hdfs.sh    # first-run only — formats namenode, inits Hive metastore
    ├── profile-additions.sh # sets PATH/PYTHONPATH for all SSH sessions
    └── setup-test-data.sh   # populates HDFS + Hive with test databases and tables
```

---

## Quick Start

All commands run in WSL (outside the container).

### Step 1 — Navigate to the dev-tools directory

```bash
cd dev-tools/hadoop-edge-node
```

### Step 2 — Build the image

> **First build downloads ~1.5 GB and takes 20–40 minutes.
> Subsequent builds use Docker layer cache and finish in under 30 seconds.**

```bash
docker compose build
```

### Step 3 — Start the container

```bash
docker compose up -d
sleep 20
docker logs hadoop-edge-node --tail 5
```

Expected output:

```
===============================
Container is ready!
SSH:  ssh root@localhost -p 2222  (password: root)
HDFS: http://localhost:9870
YARN: http://localhost:8088
===============================
```

### Step 4 — Verify services

```bash
docker exec -u root hadoop-edge-node hdfs dfsadmin -report
# Expected: Live datanodes (1)

docker exec -u root hadoop-edge-node yarn node -list
# Expected: RUNNING hadoop-edge-node:XXXXX

docker exec -u root hadoop-edge-node hive -e "SHOW DATABASES;" 2>/dev/null
# Expected: default
```

### Step 5 — Populate test data

Run once after starting the container (or after any full reset):

```bash
bash scripts/setup-test-data.sh
```

This creates 4 databases and 10 tables covering every migration test case.
See [Test Data Reference](#test-data-reference) below for the full table inventory.

### Step 6 — SSH into the container

```bash
# Clear stale host key if you rebuilt the image
ssh-keygen -f ~/.ssh/known_hosts -R '[localhost]:2222'

ssh root@localhost -p 2222
# password: root
```

---

## Connecting to Airflow (nx1 Tenant)

The nx1 tenant Airflow runs in Kubernetes and **cannot reach `localhost:2222`** directly.
You need a TCP tunnel to expose the container's SSH port publicly.

### Using bore (recommended — free, no account needed)

```bash
# Install bore (one time)
curl -sSL https://github.com/ekzhang/bore/releases/download/v0.5.0/bore-v0.5.0-x86_64-unknown-linux-musl.tar.gz \
  | sudo tar -xz -C /usr/local/bin

# Start tunnel — keep this terminal open during testing
bore local 2222 --to bore.pub
# Output: listening at bore.pub:XXXXX  ← note this port
```

Then in **Airflow UI → Admin → Connections → `cluster_edge_ssh`**:

| Field     | Value                                                                             |
| --------- | --------------------------------------------------------------------------------- |
| Conn Type | SSH                                                                               |
| Host      | `bore.pub`                                                                        |
| Port      | `XXXXX` (from bore output — changes on each restart)                              |
| Username  | `root`                                                                            |
| Password  | `root`                                                                            |
| Extra     | `{"timeout": "30", "no_host_key_check": "true", "allow_host_key_change": "true"}` |

> The bore port changes every time you restart the tunnel.
> Update the Airflow connection whenever you restart bore.

---

## Useful Commands

| Task                   | Command                                          |
| ---------------------- | ------------------------------------------------ |
| Start container        | `docker compose up -d`                           |
| Stop (keep data)       | `docker compose down`                            |
| Full reset (wipe data) | `docker compose down -v && docker compose up -d` |
| View logs              | `docker logs -f hadoop-edge-node`                |
| Restart                | `docker compose restart`                         |
| Run command            | `docker exec -u root hadoop-edge-node <cmd>`     |
| HDFS Web UI            | http://localhost:9870                            |
| YARN Web UI            | http://localhost:8088                            |

### Fix Hive metastore (if `SHOW DATABASES` fails after restart)

```bash
docker exec -u root hadoop-edge-node bash -c \
  "rm -rf /opt/hive/metastore_db && \$HIVE_HOME/bin/schematool -initSchema -dbType derby"

# Then re-run test data setup
bash scripts/setup-test-data.sh
```

### Fix Windows line endings (if script fails with `\r` errors)

```bash
sed -i 's/\r//' scripts/setup-test-data.sh
```

---

## Test Data Reference

`setup-test-data.sh` creates 4 databases with 10 tables. The table below
documents every table's properties and what migration scenario it tests.

### `sales_db`

| Table          | Format   | Partitioned | Partition Keys  | Rows | What It Tests                                                                                                                                      |
| -------------- | -------- | ----------- | --------------- | ---- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `customers`    | TEXTFILE | No          | —               | 5    | Non-partitioned CSV (`,` delimiter)                                                                                                                |
| `products`     | TEXTFILE | No          | —               | 5    | Non-partitioned TSV (`\t` delimiter)                                                                                                               |
| `orders`       | TEXTFILE | Yes         | `year`, `month` | 10   | Partitioned table, 3 partitions registered (`year=2024/month=01`, `02`, `03`). Used for exact filter, `>=` comparison, and all-partition migration |
| `transactions` | TEXTFILE | No          | —               | 4    | Non-partitioned pipe-delimited (`\|`). Tests `field.delim` serde property preservation                                                             |
| `orders_empty` | TEXTFILE | Yes         | `year`, `month` | 0    | Empty partitioned table — zero rows, zero files, no registered partitions                                                                          |

### `hr_db`

| Table         | Format   | Partitioned | Partition Keys | Rows | What It Tests                                                                   |
| ------------- | -------- | ----------- | -------------- | ---- | ------------------------------------------------------------------------------- |
| `employees`   | TEXTFILE | No          | —              | 5    | `\N` null values. Tests `serialization.null.format` serde property preservation |
| `departments` | TEXTFILE | No          | —              | 0    | Empty non-partitioned table — zero rows, zero files                             |

> `hr_db` is used for the **wildcard `*` test** — setting `table=*` in the Excel config migrates both `employees` and `departments` in one row.

### `analytics_db`

| Table      | Format   | Partitioned | Partition Keys            | Rows | What It Tests                                                                                                                                                                                                                                |
| ---------- | -------- | ----------- | ------------------------- | ---- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `events`   | TEXTFILE | Yes         | `region`, `year`, `month` | 9    | 3-level partition, 4 partitions registered (`region=US/year=2024/month=01`, `region=EU/year=2024/month=01`, `region=US/year=2024/month=02`, `region=US/year=2024/month=03`). Used for wildcard filter `region=US/*` and exact 3-level filter |
| `sessions` | TEXTFILE | Yes         | `year`, `month`           | 4    | **Unregistered partitions** — data exists in HDFS (`year=2024/month=01` and `month=02`) but partitions are NOT registered in the Hive metastore. DAG must detect `unregistered_partitions=True` and run `MSCK REPAIR TABLE`                  |

### `logs_db`

| Table      | Format   | Partitioned | Partition Keys | Rows | What It Tests                                                                                                                                             |
| ---------- | -------- | ----------- | -------------- | ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `app_logs` | TEXTFILE | Yes         | `dt`           | 7    | Single date-key partition, 3 partitions (`dt=2024-01-01`, `dt=2024-01-15`, `dt=2024-02-01`). Used for all `partition_filter` expression types (see below) |

### Partition Filter Test Expressions

All of the following expressions apply to `logs_db.app_logs`:

| Expression                     | Partitions matched               | Type                  |
| ------------------------------ | -------------------------------- | --------------------- |
| `dt=2024-01-01`                | `dt=2024-01-01` only             | Exact match           |
| `dt>=2024-01-15`               | `dt=2024-01-15`, `dt=2024-02-01` | Comparison `>=`       |
| `last_n_partitions=2`          | `dt=2024-01-15`, `dt=2024-02-01` | Last N by sort order  |
| `dt=2024-01-01, dt=2024-02-01` | `dt=2024-01-01`, `dt=2024-02-01` | Multi-term comma list |
| _(blank)_                      | all 3 partitions                 | No filter             |

For `analytics_db.events`:

| Expression                     | Partitions matched  | Type          |
| ------------------------------ | ------------------- | ------------- |
| `region=US/*`                  | All 3 US partitions | Wildcard      |
| `region=EU/year=2024/month=01` | EU partition only   | Exact 3-level |

---
