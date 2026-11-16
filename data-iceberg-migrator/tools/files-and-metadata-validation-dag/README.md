# NX1 migration validation

DAG id: `nx1_migration_validation`. It checks a DPC1.0 -> DPC2.0 migration two ways: table metadata (rows,
schema, partitions, files) and S3 object counts/sizes. See `RUNBOOK.md` for the steps to run it.

## The two scripts

| Script | Runs where | What it does |
|---|---|---|
| `migration_metrics_collector_v2.py` (existing, in `data-iceberg-migrator/tools`) | DPC1.0 edge node, by whoever has access | Collects source metrics, uploads `source_metrics.json` to S3 |
| `dags/nx1_migration_validation.py` | Airflow | Files + metadata validation in one DAG |

Airflow never connects to the edge node.

The DAG imports `migration_metrics_collector.py`, the shared measuring/comparison code that V2 also needs.
Copy it unchanged into the same folder as the DAG file, so DPC2.0 is measured and compared exactly like the source.

## Tasks

```
Metadata:  wait_for_source_snapshot -> load_source_snapshot -> collect_dest_metrics -> compare_metadata
Files:     get_bucket_pairs -> validate_bucket_pair (one per pair) -> summarize_s3_validation
```

The two branches run in parallel, and the run fails if either finds a problem. Reports are written to S3
before the failure is raised.

- **wait_for_source_snapshot**: waits (up to 12 h) for a source snapshot newer than `max_snapshot_age_hours`.
- **load_source_snapshot**: checks the snapshot and keeps a copy as `<run>/source_metrics.json`.
- **collect_dest_metrics**: starts a Spark session on the worker, collects the same databases/tables from DPC2.0
  (default Hive catalog, `<db>.<table>`), writes `dest_metrics.json`. Fails early if the destination database is
  not visible to Spark.
- **compare_metadata**: compares the two snapshots, writes `summary.csv`, `partition_diffs.csv`, `report.html`.
- **validate_bucket_pair / summarize_s3_validation**: object count and total size per bucket pair on both tenants.

## Trigger parameters

Required:

| Param | Meaning |
|---|---|
| `work_bucket` | Bucket on the destination tenant for snapshots and reports |
| `bucket_pairs` | Buckets to compare, single-line JSON: `[{"src_bucket":"a","src_prefix":"","dst_bucket":"b","dst_prefix":""}]`. `src_bucket` and `dst_bucket` are required; prefixes are optional |

Optional (leave empty for the default; defined in `DEFAULTS` in the DAG file):

| Param | Default | Meaning |
|---|---|---|
| `run_label` | `dpc1-to-dpc2` | Groups snapshots/reports under `work_prefix/run_label` |
| `work_prefix` | `migration-validation` | Folder in `work_bucket`; no leading/trailing slash |
| `source_snapshot_key` | `<work_prefix>/<run_label>/source_metrics.json` | Where V2 uploaded the source snapshot |
| `max_snapshot_age_hours` | `48` | A source snapshot older than this is ignored |
| `collect_storage_metrics` | `true` | Count files/bytes on storage |
| `parallelism` | `8` | Tables collected in parallel |
| `hadoop_user` | the DAG owner (`DAG_OWNER`) | Sets `HADOOP_USER_NAME` for the DPC2.0 Spark session |
| `fail_on_warn` | `false` | Also fail on `WARN` tables (file/partition count differences) |

## Airflow setup

Connections (S3: login = access key, password = secret key,
extra = `{"endpoint_url": "https://host:10443", "verify": false}`; `verify` can be a CA bundle path):

| Conn id | Needed | Purpose |
|---|---|---|
| `nx1_s3_source` | yes | Source S3 tenant |
| `nx1_s3_dest` | yes | Destination S3 tenant; also holds snapshots and reports (`work_bucket`) |
| `nx1_dpc2_spark` | no | Extra Spark settings (`master`, `spark_conf`, `hadoop_user`). Not needed if the worker's own Spark config already reaches DPC2.0 |

Workers need `boto3`, `pyspark` with a Java runtime, and a Spark configuration (`spark-defaults`,
`hive-site.xml`) pointing at the DPC2.0 Hive metastore and S3.

The Hadoop user: tasks run as the pod user (`airflow`) unless `HADOOP_USER_NAME` is set. The DAG sets it from
`hadoop_user`, or from the DAG owner (`DAG_OWNER` at the top of the DAG file) when that is empty. It must be a
user allowed to read the tables. This works with simple authentication; under Kerberos it is ignored.

## Outputs (in `work_bucket`)

```
<work_prefix>/<run_label>/
  source_metrics.json, dest_metrics.json
  s3_object_validation_<ts>.json
  report_<ts>/summary.csv, partition_diffs.csv, report.html
```

The run fails if any table is `FAIL`, `ERROR` or `MISSING_ON_DEST` (also `WARN` with `fail_on_warn`), or any
bucket pair is `MISMATCH` or `ERROR`.
