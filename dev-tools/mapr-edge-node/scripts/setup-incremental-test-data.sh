#!/bin/bash
# =============================================================================
# setup-incremental-test-data.sh
#
# Adds NEW data to sales_db.orders (a table already migrated by the initial
# setup-test-data-combined.sh run) so you can trigger DAG 1 a second time
# and observe an *incremental* DistCp run (INCR=true, -update flag active,
# only the new files are copied).
#
# TARGET TABLE:  sales_db.orders
#   Partitioned by (year INT, month INT)
#   Existing partitions after full migration: 2024/month=1,2,3
#   New partition added here:                 year=2024/month=4
#                                             year=2024/month=5
#
# ADDITIONAL CHANGE:
#   A second file is appended to an EXISTING partition (year=2024/month=3)
#   to simulate a "late-arriving records" scenario — DistCp -update picks
#   up the new file but leaves the already-copied files untouched.
#
# HOW TO USE:
#   1. Complete the full migration DAG run first (setup-test-data-combined.sh
#      must already have run and DAG 1 must have completed with VALIDATED).
#   2. Run this script:
#        kubectl exec deploy/hadoop-edge -n hadoop-ssh -- \
#          bash /setup-incremental-test-data.sh
#   3. Trigger DAG 1 again with the SAME Excel config.
#      Expected results:
#        - sales_db.orders      → INCR=true, only 2 new partition dirs + 1
#                                  new file in month=3 copied; prior files skipped
#        - All other tables     → 0 new files, DistCp -update is a no-op
#                                  (bytes_copied=0, files_copied=0)
#
# WHAT THIS TESTS (incremental scenarios from the scenario reference):
#   Scenario 23 — Re-run / incremental: existing S3 destination present,
#                  -update only copies delta, row counts increase on S3 side.
#   Scenario 4  — Partition filter still works on a re-run (apply
#                  partition_filter=year=2024 AND month=4 in Excel to test
#                  a filter-scoped incremental run).
# =============================================================================

set -euo pipefail

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

BEELINE="/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true"
WH="hdfs://localhost:9000/user/hive/warehouse"

echo "============================================================"
echo " [1/4] Writing new TEXTFILE data to HDFS"
echo "       Target: sales_db.orders"
echo "============================================================"

# ── NEW PARTITION: year=2024 / month=4 ─────────────────────────────────────────
# 4 orders — fresh partition that did not exist during the initial migration
hdfs dfs -mkdir -p ${WH}/sales_db.db/orders/year=2024/month=4

printf '11,Karen,275\n12,Leo,330\n13,Mia,190\n14,Nate,420\n' \
  | hdfs dfs -put -f - ${WH}/sales_db.db/orders/year=2024/month=4/data.csv

echo "  NEW partition year=2024/month=4 written (4 rows)"

# ── NEW PARTITION: year=2024 / month=5 ─────────────────────────────────────────
# 3 orders — second fresh partition
hdfs dfs -mkdir -p ${WH}/sales_db.db/orders/year=2024/month=5

printf '15,Olivia,515\n16,Paul,88\n17,Quinn,370\n' \
  | hdfs dfs -put -f - ${WH}/sales_db.db/orders/year=2024/month=5/data.csv

echo "  NEW partition year=2024/month=5 written (3 rows)"

# ── ADDITIONAL FILE IN EXISTING PARTITION: year=2024 / month=3 ────────────────
# Late-arriving records in a partition that was already migrated.
# DistCp -update sees this as a NEW file (different filename) and copies only it.
# The previously copied data.csv on S3 is left untouched.
printf '18,Rita,260\n19,Sam,145\n' \
  | hdfs dfs -put -f - ${WH}/sales_db.db/orders/year=2024/month=3/data_late.csv

echo "  LATE FILE year=2024/month=3/data_late.csv written (2 rows)"
echo "  (this file is NEW on HDFS — prior data.csv already on S3 is unchanged)"

echo ""
echo "============================================================"
echo " [2/4] Registering new partitions in Hive metastore"
echo "============================================================"

$BEELINE << 'SQL_EOF'
ALTER TABLE sales_db.orders ADD IF NOT EXISTS PARTITION (year=2024, month=4);
ALTER TABLE sales_db.orders ADD IF NOT EXISTS PARTITION (year=2024, month=5);
SQL_EOF

echo "  Partitions year=2024/month=4 and year=2024/month=5 registered."

echo ""
echo "============================================================"
echo " [3/4] Verification"
echo "============================================================"

pyspark --master local[*] << 'PYEOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n--- sales_db.orders: total row count (all partitions) ---")
total = spark.sql("SELECT COUNT(*) as c FROM sales_db.orders").collect()[0]["c"]
print(f"  Total rows: {total}  (expect 19: 10 original + 4 month4 + 3 month5 + 2 late)")

print("\n--- sales_db.orders: row count per partition ---")
rows = spark.sql("""
    SELECT year, month, COUNT(*) as cnt
    FROM sales_db.orders
    GROUP BY year, month
    ORDER BY year, month
""").collect()
for r in rows:
    note = ""
    if r["month"] == 3 and r["cnt"] == 7:
        note = "  <- 5 original + 2 late-arriving"
    elif r["month"] == 4:
        note = "  <- NEW partition (incremental)"
    elif r["month"] == 5:
        note = "  <- NEW partition (incremental)"
    else:
        note = "  <- original (unchanged)"
    print(f"  year={r['year']}, month={r['month']:>2}: {r['cnt']} rows{note}")

print("\n--- sales_db.orders: registered partitions ---")
parts = spark.sql("SHOW PARTITIONS sales_db.orders").collect()
for p in parts:
    print(f"  {p[0]}")
print(f"  Total: {len(parts)} partitions (expect 5)")

print("\n--- HDFS file listing for sales_db.orders ---")
import subprocess
r = subprocess.run(
    ["hdfs", "dfs", "-ls", "-R",
     "hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders"],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE
)
r.stdout = r.stdout.decode("utf-8", errors="replace")
r.stderr = r.stderr.decode("utf-8", errors="replace")
for line in r.stdout.splitlines():
    if line.startswith("-"):
        print(f"  {line}")

spark.stop()
PYEOF

echo ""
echo "============================================================"
echo " [4/4] Incremental run instructions"
echo "============================================================"
cat << 'INST_EOF'

SOURCE DATA DELTA IS READY
================================================================

Changes made to sales_db.orders:
  NEW  year=2024/month=4/data.csv        (4 rows)
  NEW  year=2024/month=5/data.csv        (3 rows)
  NEW  year=2024/month=3/data_late.csv   (2 late-arriving rows, new file)
  UNCHANGED  year=2024/month=1/data.csv  (already on S3)
  UNCHANGED  year=2024/month=2/data.csv  (already on S3)
  UNCHANGED  year=2024/month=3/data.csv  (already on S3)

TRIGGERING THE INCREMENTAL DAG RUN:
  1. Open Airflow → source_to_s3_migration DAG
  2. Trigger with the SAME Excel config used for the initial run.
     The DAG auto-detects incremental because the S3 destination
     directory already exists (INCR=true is set by the distcp shell).
  3. Watch the run. Expected per-table outcomes:

     sales_db.orders     → VALIDATED (INCR=true)
       - month=4 and month=5 dirs do NOT exist on S3 → copied in full
       - month=3/data_late.csv is a new filename → copied
       - month=1, month=2, month=3/data.csv already on S3 → skipped by -update
       - bytes_copied  > 0
       - files_copied  = 3 (two new partition files + one late file)
       - row count on S3 matches Hive: 19 total

     All other tables → VALIDATED (INCR=true)
       - S3 already matches HDFS → -update is a no-op
       - bytes_copied = 0 / files_copied = 0
       - Row counts unchanged

OPTIONAL — PARTITION-FILTER INCREMENTAL TEST:
  To test scenario 4 (partition_filter on an incremental run), add a row
  to your Excel config that targets ONLY the new partitions:

    database     : sales_db
    table        : orders
    partition_filter: year=2024 AND month=4
    dest_database: sales_db_copy   (same as your initial run)
    bucket       : <your bucket>

  Expected: only month=4 files checked/copied, month=5 not touched.

RESET (if you want to re-run from scratch):
  Delete the new files on HDFS and drop the new partitions:

    hdfs dfs -rm hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders/year=2024/month=4/data.csv
    hdfs dfs -rm hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders/year=2024/month=5/data.csv
    hdfs dfs -rm hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders/year=2024/month=3/data_late.csv
    /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true \
      -e "ALTER TABLE sales_db.orders DROP PARTITION (year=2024, month=4);
          ALTER TABLE sales_db.orders DROP PARTITION (year=2024, month=5);"

  Then wipe the corresponding S3 prefixes and re-run DAG 1 for a clean full-load.

INST_EOF

echo ""
echo "Done. Run DAG 1 again with the same Excel config to trigger the incremental migration."