# Runbook: running `nx1_migration_validation`

Validates a DPC1.0 -> DPC2.0 migration: table metadata (rows, schema, partitions, files) and S3 object
counts/sizes. The design and the parameter reference are in README.md; this page is the steps.

## Before you start (one-time)

1. `nx1_migration_validation.py` and `migration_metrics_collector.py` are in the same Airflow DAGs folder.
2. Airflow connections `nx1_s3_source` and `nx1_s3_dest` exist.
3. The worker image has `boto3`, `pyspark` and Java, and its Spark config reaches the DPC2.0 Hive metastore.
4. You know the `work_bucket` (on the destination S3 tenant) and the user `DAG_OWNER` / `hadoop_user` that can
   read the DPC2.0 tables.

## Each run

### 1. Collect the source metrics (edge node, whoever has access)

```
export AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...
spark-submit --master 'local[*]' migration_metrics_collector_v2.py \
  --config source_config.json --output source_metrics.json \
  --s3-output s3a://<work_bucket>/<work_prefix>/<run_label>/source_metrics.json \
  --upload-method aws --s3-endpoint https://<s3 endpoint>
```

Success: the log ends with `SNAPSHOT_COPIED_TO_S3 ...`. Exit code 2 means the upload failed; the local
`source_metrics.json` is intact, so upload it to that S3 key by hand.

### 2. Trigger the DAG

Airflow UI -> `nx1_migration_validation` -> Trigger. Fill in:

| Param | Value |
|---|---|
| `work_bucket` (required) | bucket from step 1 |
| `bucket_pairs` (required) | single-line JSON, one object per bucket pair (see below) |
| everything else | leave empty to use the defaults; set `work_prefix` / `run_label` if you used other values in step 1 |

`bucket_pairs` example (all on one line):

```
[{"src_bucket":"<src>","src_prefix":"<folder/>","dst_bucket":"<dst>","dst_prefix":"<folder/>"}]
```

From the command line, pass real objects instead:

```
airflow dags trigger nx1_migration_validation --conf '{"work_bucket":"<bucket>","bucket_pairs":[{"src_bucket":"<src>","dst_bucket":"<dst>"}]}'
```

The first task waits (up to 12 h) for a source snapshot newer than `max_snapshot_age_hours` (default 48).

### 3. Read the results

Everything is under `s3://<work_bucket>/<work_prefix>/<run_label>/`:

- `report_<ts>/report.html`, `summary.csv`, `partition_diffs.csv`: metadata result per table
- `s3_object_validation_<ts>.json`: object counts and sizes per bucket pair

The task logs also print the status counts and the bucket comparison table.

## Reading a failure

| Failed task | Meaning | Action |
|---|---|---|
| `get_bucket_pairs` | `bucket_pairs` is malformed | The message names the bad field. Use single-line JSON with `src_bucket` and `dst_bucket` |
| `wait_for_source_snapshot` (timeout) | No fresh snapshot in S3 | Redo step 1; check the key matches `work_prefix/run_label` or set `source_snapshot_key` |
| `load_source_snapshot` | Snapshot missing, partial or not a source snapshot | Re-upload; check it has `"side": "source"` |
| `collect_dest_metrics` | Spark/Hive/S3 problem on DPC2.0, or database not visible | Read the `[dest]` lines in the log: Spark version, metastore URI, visible databases. If the database is missing, fix the worker's Spark config or `hadoop_user` |
| `compare_metadata` | Tables are `FAIL`, `ERROR` or `MISSING_ON_DEST` | Open `report.html`; drill into `partition_diffs.csv` |
| `validate_bucket_pair` / `summarize_s3_validation` | `MISMATCH` or `ERROR` for a pair | See the table in the log; `ERROR` is usually credentials or endpoint |

Statuses: `FAIL` = rows or schema differ. `WARN` = files/partitions differ (unregistered partitions can be fixed
with `MSCK REPAIR TABLE`). `MISSING_ON_DEST` = table not found on DPC2.0.

For `MISSING_ON_DEST`, check the log line `[dest] <db>: N table(s)`. `0 table(s)` means Spark did not list the
table for the user it ran as. Compare with a manual query as that same user:

```
kubectl exec -n <ns> <airflow-pod> -- env HADOOP_USER_NAME=<user> python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.enableHiveSupport().getOrCreate(); s.sql('SHOW TABLES IN <db>').show()"
```

## Re-running

After fixing the data, repeat step 1 (fresh snapshot) and trigger a new run. A changed parameter needs a new
trigger; clearing a failed task reuses the old run's parameters. To re-run only the comparison, clear
`compare_metadata`; it reuses the saved snapshots.
