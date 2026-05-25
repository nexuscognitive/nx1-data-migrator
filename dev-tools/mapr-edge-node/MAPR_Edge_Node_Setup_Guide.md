# MapR Edge Node: Setup Guide

## Overview

This page describes how to use the shared MapR edge node that is already running on our Kubernetes cluster (namespace: `hadoop-ssh`). It simulates a real MapR Hadoop edge node so you can test the end-to-end data migration feature, Airflow connects to it over SSH, runs the discover and distcp steps, and migrates data to S3.

You do **not** need to deploy anything. The pod is already running and the Airflow SSH connection is already configured. Just follow the steps below to verify everything is wired up and load test data.

---

## Connection details

| Field    | Value                                          |
| -------- | ---------------------------------------------- |
| Host     | `hadoop-edge-ssh.hadoop-ssh.svc.cluster.local` |
| Port     | `22`                                           |
| User     | `root`                                         |
| Password | `root`                                         |

The SSH endpoint is reachable only from inside the Kubernetes cluster (e.g. from Airflow pods). It is not accessible from your local machine.

---

## Airflow connection

The Airflow connection `cluster_edge_ssh` on our tenant is already configured to point at the pod above.

| Field     | Expected value                                                                    |
| --------- | --------------------------------------------------------------------------------- |
| Conn Type | SSH                                                                               |
| Host      | `hadoop-edge-ssh.hadoop-ssh.svc.cluster.local`                                    |
| Port      | `22`                                                                              |
| Username  | `root`                                                                            |
| Password  | `root`                                                                            |
| Extra     | `{"timeout": "30", "no_host_key_check": "true", "allow_host_key_change": "true"}` |

---

## Airflow Variables

Set these in Airflow UI → **Admin → Variables** before triggering a DAG run:

| Key                        | Value                      |
| -------------------------- | -------------------------- |
| `auth_method`              | `mapr`                     |
| `mapr_user`                | `root`                     |
| `mapr_ticketfile_location` | `/tmp/maprticket`          |
| `s3_access_key`            | Your AWS Access Key ID     |
| `s3_secret_key`            | Your AWS Secret Access Key |
| `migration_distcp_mappers` | `1`                        |

---

## Pod Setup (Required After Every Restart)

The pod filesystem is ephemeral — environment variables, Hadoop XML configs, and the MapR ticket are lost when the pod restarts. A single script handles all of this.

### Run the restart setup script

```bash
kubectl exec deploy/hadoop-edge -n hadoop-ssh -- bash /pod-restart-setup.sh
```

**`pod-restart-setup.sh` Script:**

```bash
#!/bin/bash
# =============================================================================
# pod-restart-setup.sh
# Run this script after every pod restart to restore the ephemeral filesystem.
# Usage (from your local machine):
#   kubectl exec deploy/hadoop-edge -n hadoop-ssh -- bash /pod-restart-setup.sh
# =============================================================================

set -e

echo "=== [1/5] Setting HADOOP environment variables ==="

export HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar:/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-core-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-databind-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-annotations-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-mapper-asl-1.9.13.jar
export HADOOP_HEAPSIZE=512
export HADOOP_CLIENT_OPTS="-Xmx512m"

# Remove any stale entries, then persist to both profile files
sed -i '/HADOOP_CLASSPATH/d; /HADOOP_HEAPSIZE/d; /HADOOP_CLIENT_OPTS/d' /root/.profile
sed -i '/HADOOP_CLASSPATH/d; /HADOOP_HEAPSIZE/d; /HADOOP_CLIENT_OPTS/d' /etc/profile.d/hadoop.sh

cat >> /root/.profile << 'EOF'
export HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar:/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-core-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-databind-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-annotations-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-mapper-asl-1.9.13.jar
export HADOOP_HEAPSIZE=512
export HADOOP_CLIENT_OPTS="-Xmx512m"
EOF

cat >> /etc/profile.d/hadoop.sh << 'EOF'
export HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar:/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-core-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-databind-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-annotations-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-mapper-asl-1.9.13.jar
export HADOOP_HEAPSIZE=512
export HADOOP_CLIENT_OPTS="-Xmx512m"
EOF

echo "✓ HADOOP environment variables set"

# =============================================================================
echo "=== [2/5] Writing core-site.xml (S3A configuration) ==="

cat > /opt/hadoop/etc/hadoop/core-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>/hadoop/data/tmp</value></property>
  <property><name>fs.maprfs.impl</name><value>org.apache.hadoop.hdfs.DistributedFileSystem</value></property>
  <property><name>fs.s3a.impl</name><value>org.apache.hadoop.fs.s3a.S3AFileSystem</value></property>
  <property><name>fs.AbstractFileSystem.s3a.impl</name><value>org.apache.hadoop.fs.s3a.S3A</value></property>
  <property><name>fs.s3a.path.style.access</name><value>false</value></property>
  <property><name>fs.s3a.committer.name</name><value>file</value></property>
  <property><name>mapreduce.outputcommitter.factory.scheme.s3a</name><value>org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter</value></property>
  <property><name>fs.s3a.connection.maximum</name><value>100</value></property>
  <property><name>fs.s3a.threads.max</name><value>20</value></property>
</configuration>
EOF

cp /opt/hadoop/etc/hadoop/core-site.xml /opt/spark/conf/core-site.xml
echo "✓ core-site.xml written and synced to Spark"

# =============================================================================
echo "=== [3/5] Writing mapred-site.xml (local mode — no YARN) ==="

cat > /opt/hadoop/etc/hadoop/mapred-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property><name>mapreduce.framework.name</name><value>local</value></property>
  <property><name>mapreduce.local.map.tasks.maximum</name><value>1</value></property>
  <property><name>mapreduce.local.reduce.tasks.maximum</name><value>1</value></property>
</configuration>
EOF

echo "✓ mapred-site.xml written"

# =============================================================================
echo "=== [4/5] Restoring maprlogin binary ==="

cat > /usr/local/bin/maprlogin << 'EOF'
#!/bin/bash
if [ "$1" = "print" ]; then
  echo "MapR credentials (UID 0) for user: root"
  echo "  created: $(date)"
  echo "  expires: $(date -d '+7 days' 2>/dev/null || date)"
  echo "  cluster: test-cluster"
  exit 0
fi
echo "maprlogin: unknown command '$1'"
exit 1
EOF
chmod +x /usr/local/bin/maprlogin

echo "✓ maprlogin binary restored"

# =============================================================================
echo "=== [5/5] Restoring maprticket ==="

mkdir -p /tmp
cat > /tmp/maprticket << 'EOF'
MAPR_TICKET
cluster=test-cluster
user=root
uid=0
created=0
expires=9999999999
EOF

export MAPR_TICKETFILE_LOCATION=/tmp/maprticket
echo 'export MAPR_TICKETFILE_LOCATION=/tmp/maprticket' >> /root/.profile

echo "✓ maprticket created"

# =============================================================================
echo ""
echo "=== Verification ==="

source /root/.profile

# MapR ticket check
maprlogin print 2>/dev/null | grep -q "root" \
  && echo "✓ MapR ticket:        PASSED" \
  || echo "✗ MapR ticket:        FAILED"

# Hadoop classpath sanity check
[ -f /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar ] \
  && echo "✓ Hadoop AWS JAR:     FOUND" \
  || echo "✗ Hadoop AWS JAR:     NOT FOUND (check image)"

# core-site.xml in place
[ -f /opt/hadoop/etc/hadoop/core-site.xml ] \
  && echo "✓ core-site.xml:      IN PLACE" \
  || echo "✗ core-site.xml:      MISSING"

echo ""
echo "Pod setup complete. You can now trigger DAG runs."
```

The script handles all five steps automatically:

1. Sets and persists `HADOOP_CLASSPATH`, `HADOOP_HEAPSIZE`, and `HADOOP_CLIENT_OPTS`
2. Writes `core-site.xml` with S3A configuration (and syncs to Spark)
3. Writes `mapred-site.xml` in local MapReduce mode (YARN cannot run as root — see [Why YARN doesn't work](https://claude.ai/new?incognito=#why-yarn-doesnt-work))
4. Restores the `maprlogin` binary
5. Restores the `/tmp/maprticket` file and sets `MAPR_TICKETFILE_LOCATION`

At the end it prints a verification summary:

```
=== Verification ===
✓ MapR ticket:        PASSED
✓ Hadoop AWS JAR:     FOUND
✓ core-site.xml:      IN PLACE

Pod setup complete. You can now trigger DAG runs.
```

---

## First-Time Setup: Load Test Data

This is only needed once (or after the pod is rescheduled and HDFS data is lost).

```bash
kubectl exec deploy/hadoop-edge -n hadoop-ssh -- bash /setup-test-data.sh
```

Expected output (last few lines):

```
============================================================
 [6/6] Test scenario reference
============================================================
 #  | Excel row                             | What is tested
----|---------------------------------------|-------------------------------------------
 1  | sales_db / * / sales_db_dest          | Wildcard: full DB migration
 2  | hr_db / employees / hr_db_dest        | Single explicit table, non-partitioned
 3  | hr_db / employees,departments / ...   | Comma-separated tables (one EMPTY_SOURCE)
 4  | sales_db / orders / ... / year=2024 AND month=1 | Partition filter active
 5  | sales_db / orders / ... / year=2099   | Partition filter → 0 partitions → SKIP
 6  | analytics_db / sessions / ...         | Unregistered partitions → MSCK REPAIR
 7  | analytics_db / events / ...           | 3-level partition key (region/year/month)
 8  | analytics_db / events / ... / region='US' AND year=2024 | Filter on 3-level key
 9  | logs_db / error_logs_empty / ...      | EMPTY_SOURCE: dt-partitioned, 0 files
10  | logs_db / metrics_empty_nonpartitioned| EMPTY_SOURCE: flat non-partitioned, 0 files
11  | analytics_db / events_empty_partitioned| EMPTY_SOURCE: 3-level partition, 0 files
12  | sales_db / orders_empty / ...         | EMPTY_SOURCE: year/month partition, 0 files
13  | analytics_db / events_orc / ...       | ORC format → STORED AS ORC in DDL
14  | hr_db / employees_parquet / ...       | PARQUET format, schema inferred from files
15  | hr_db / employees_avro / ...          | AVRO format → STORED AS AVRO in DDL
16  | logs_db / app_logs / ...              | TEXTFILE + serde field.delim property
17  | logs_db / app_logs / ... / dt>='2024-01-15' | String dt partition filter
18  | sales_db / orders* / ...              | Glob pattern → expands matching tables
19  | hr_db / employees / hr_renamed / alt-bucket | Different dest_db + alt bucket
20  | sales_db / orders / ... / https://... | Custom S3 endpoint (MinIO/Ceph)
21  | logs_db / * / ... / dt='2024-01-01'   | Wildcard + partition_filter (warning path)
22  | analytics_db / sessions,events / ... / year=2024 AND month=1 | CSV tables + filter
23  | sales_db / orders / ... (2nd run)     | Incremental re-run (INCR=true in DistCp)
24  | hr_db / employees / ...               | \N NULL values survive round-trip
25  | sales_db / orders / (blank dest_db)   | Blank dest_db → defaults to source name

Done.  Test data is ready.
```

The script is safe to re-run — existing data is overwritten or skipped cleanly.

---

## Triggering an End-to-End Migration Test

Once test data is loaded and the pod setup script has been run:

1. Open the Airflow UI
2. Trigger the `source_to_s3_migration` DAG with an Excel config pointing at any of the test tables below
3. Airflow will SSH into the pod using `cluster_edge_ssh`, run the discover PySpark script, and execute distcp

**Mapper count:** DistCp runs in local MapReduce mode (not YARN). Set `migration_distcp_mappers = 1`. No DAG code changes are needed — the flag is already in the distcp commands.

---

## Test Data Reference

`setup-test-data.sh` creates **4 databases** with **16 tables**. Each table exercises a specific scenario in the migration DAG.

### `sales_db`

| Table          | Format   | Partitioned | Partition Keys  | Rows | Tests                                                                                    |
| -------------- | -------- | ----------- | --------------- | ---- | ---------------------------------------------------------------------------------------- | --------------------------------------- |
| `orders`       | TEXTFILE | Yes         | `year`, `month` | 10   | Partitioned table, 3 partitions — exact filter, `>=` comparison, all-partition migration |
| `customers`    | TEXTFILE | No          | —               | 5    | Non-partitioned CSV (`,` delimiter)                                                      |
| `products`     | TEXTFILE | No          | —               | 5    | Non-partitioned TSV (`\t` delimiter)                                                     |
| `transactions` | TEXTFILE | No          | —               | 4    | Pipe-delimited (`                                                                        | `) — tests `field.delim` serde property |
| `orders_empty` | TEXTFILE | Yes         | `year`, `month` | 0    | EMPTY_SOURCE — zero files, no registered partitions                                      |

### `hr_db`

| Table               | Format   | Partitioned | Partition Keys | Rows | Tests                                                               |
| ------------------- | -------- | ----------- | -------------- | ---- | ------------------------------------------------------------------- |
| `employees`         | TEXTFILE | No          | —              | 5    | `\N` null values — tests `serialization.null.format` serde property |
| `departments`       | TEXTFILE | No          | —              | 0    | EMPTY_SOURCE — empty non-partitioned table                          |
| `employees_parquet` | PARQUET  | No          | —              | 5    | PARQUET format — schema inferred from files                         |
| `employees_avro`    | AVRO     | No          | —              | 3    | AVRO format — tests `STORED AS AVRO` DDL                            |

> `hr_db` is used for the **wildcard `*` test** — discovers all tables in one call.

### `analytics_db`

| Table                      | Format   | Partitioned | Partition Keys            | Rows | Tests                                                                                                                          |
| -------------------------- | -------- | ----------- | ------------------------- | ---- | ------------------------------------------------------------------------------------------------------------------------------ |
| `events`                   | TEXTFILE | Yes         | `region`, `year`, `month` | 9    | 3-level partition, 4 partitions — wildcard filter `region=US/*` and exact 3-level filter                                       |
| `sessions`                 | TEXTFILE | Yes         | `year`, `month`           | 4    | **Unregistered partitions** — data exists in HDFS but not registered in metastore. DAG must set `unregistered_partitions=True` |
| `events_orc`               | ORC      | Yes         | `region`, `year`, `month` | 5    | ORC format — tests `STORED AS ORC` DDL                                                                                         |
| `events_empty_partitioned` | TEXTFILE | Yes         | `region`, `year`, `month` | 0    | EMPTY_SOURCE — 3-level partition key, zero files                                                                               |

### `logs_db`

| Table                          | Format   | Partitioned | Partition Keys | Rows | Tests                                                                     |
| ------------------------------ | -------- | ----------- | -------------- | ---- | ------------------------------------------------------------------------- |
| `app_logs`                     | TEXTFILE | Yes         | `dt`           | 7    | 3 date partitions — used for all partition filter expression types        |
| `error_logs_empty`             | TEXTFILE | Yes         | `dt`           | 0    | EMPTY_SOURCE — most common real-world case: log table never received data |
| `metrics_empty_nonpartitioned` | TEXTFILE | No          | —              | 0    | EMPTY_SOURCE — simplest case: flat table, no partition logic              |

### Partition Keys Note

Hive partition specs use **unpadded integers** (`month=1`, `month=2`, `month=3`). The HDFS directories match exactly. Using zero-padded paths (`month=01`) would create a mismatch between the metastore partition spec and the HDFS location, causing `COUNT(*) = 0` for all partitioned tables.

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

## Why YARN Doesn't Work

YARN is running on this pod but cannot launch MapReduce containers. The root cause is a hard block in Hadoop 2.7.x `DefaultContainerExecutor`: when the NodeManager process runs as `root` (as it does in this container), every container launch throws `Exception from container-launch` with exit code 1, regardless of memory settings or XML configuration.

This is not fixable via configuration — it is compiled into the Hadoop 2.7.x binary. The workaround is `mapreduce.framework.name=local`, which runs DistCp in-process without YARN. The restart setup script sets this automatically. Real MapR clusters run YARN as a non-root service user and are unaffected.

---

## Useful Commands

| Task                       | Command                                                                     |
| -------------------------- | --------------------------------------------------------------------------- | ---------------------- |
| Check pod is running       | `kubectl get pods -n hadoop-ssh`                                            |
| Open a shell in the pod    | `kubectl exec -it deploy/hadoop-edge -n hadoop-ssh -- bash`                 |
| (Re-)populate test data    | `kubectl exec deploy/hadoop-edge -n hadoop-ssh -- bash /setup-test-data.sh` |
| Verify SSH is listening    | `kubectl exec deploy/hadoop-edge -n hadoop-ssh -- netstat -tlnp             | grep :22`              |
| Verify Hive services       | `kubectl exec deploy/hadoop-edge -n hadoop-ssh -- netstat -tlnp             | grep -e 9083 -e 10000` |
| Copy a file into the pod   | `kubectl cp -n hadoop-ssh ./local-file deploy/hadoop-edge:/tmp/local-file`  |
| Copy a file out of the pod | `kubectl cp -n hadoop-ssh deploy/hadoop-edge:/tmp/output.log ./output.log`  |

---

## Available ports (inside the cluster)

| Port  | Name        | Description                  |
| ----- | ----------- | ---------------------------- |
| 22    | ssh         | SSH access (used by Airflow) |
| 8088  | yarn-ui     | YARN ResourceManager UI      |
| 9870  | hdfs-ui     | HDFS NameNode UI             |
| 10000 | hiveserver2 | HiveServer2 endpoint         |

> These ports are accessible from other pods inside the cluster only — they are not exposed externally.

---

## Notes

- The pod uses `Recreate` strategy (single replica) — if you need to restart it, `kubectl rollout restart deployment/hadoop-edge -n hadoop-ssh`.
- HDFS data uses an `emptyDir` volume, so it is lost if the pod is rescheduled. Re-run `setup-test-data.sh` if that happens.
- HiveServer2 takes ~90 seconds to fully start after a pod restart. If Hive commands fail shortly after a restart, wait and retry.

## Links

1. Deploying of the docker image to the cluster: [https://www.notion.so/Hadoop-SSH-ready-host-for-data-migration-34f984e820aa805ca3d5e34bf4eab8c9?source=copy_link](https://www.notion.so/Hadoop-SSH-ready-host-for-data-migration-34f984e820aa805ca3d5e34bf4eab8c9?pvs=21)
2. Docker image repo: [nx1-data-migrator/dev-tools/mapr-edge-node at deployment_setup · nexuscognitive/nx1-data-migrator](https://github.com/nexuscognitive/nx1-data-migrator/tree/deployment_setup/dev-tools/mapr-edge-node)
