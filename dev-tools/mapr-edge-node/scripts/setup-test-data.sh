#!/bin/bash
# =============================================================================
# setup-test-data.sh
# Populates HDFS + Hive metastore with tables covering ALL migration DAG
# test scenarios.  Run INSIDE the container:
#
#   docker exec -u root mapr-edge-node bash /setup-test-data.sh
#
# Safe to re-run — existing HDFS files and Hive tables are overwritten/skipped.
# =============================================================================

set -euo pipefail

# Force pyspark to use Python 3 (Python 2.7 is default but python3 is available)
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

BEELINE="/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true"
WH="hdfs://localhost:9000/user/hive/warehouse"

# ─────────────────────────────────────────────────────────────────────────────
# Helper: write a tiny ORC file via pyspark (Hive CTAS from a temp text table)

echo "============================================================"
echo " [1/6] Creating HDFS directory structure"
echo "============================================================"

# ── sales_db ──────────────────────────────────────────────────────────────────
# orders: TEXTFILE, year/month partitions (registered)
hdfs dfs -mkdir -p ${WH}/sales_db.db/orders/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/sales_db.db/orders/year=2024/month=2
hdfs dfs -mkdir -p ${WH}/sales_db.db/orders/year=2024/month=3

# orders_empty: TEXTFILE, year/month, EMPTY_SOURCE
hdfs dfs -mkdir -p ${WH}/sales_db.db/orders_empty

hdfs dfs -mkdir -p ${WH}/sales_db.db/customers
hdfs dfs -mkdir -p ${WH}/sales_db.db/transactions

# ── hr_db ─────────────────────────────────────────────────────────────────────
# employees: TEXTFILE, non-partitioned, contains \N NULLs
hdfs dfs -mkdir -p ${WH}/hr_db.db/employees
# departments: TEXTFILE, non-partitioned, EMPTY_SOURCE
hdfs dfs -mkdir -p ${WH}/hr_db.db/departments
# employees_parquet: PARQUET, non-partitioned
hdfs dfs -mkdir -p ${WH}/hr_db.db/employees_parquet
# employees_avro: AVRO, non-partitioned
hdfs dfs -mkdir -p ${WH}/hr_db.db/employees_avro

# ── analytics_db ──────────────────────────────────────────────────────────────
# events: TEXTFILE, region/year/month, registered partitions
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events/region=US/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events/region=EU/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events/region=US/year=2024/month=2
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events/region=US/year=2024/month=3
# sessions: TEXTFILE, year/month, UNREGISTERED partitions
hdfs dfs -mkdir -p ${WH}/analytics_db.db/sessions/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db.db/sessions/year=2024/month=2
# events_orc: ORC, region/year/month, registered
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events_orc/region=US/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events_orc/region=EU/year=2024/month=1
# events_empty_partitioned: TEXTFILE, 3-level, EMPTY_SOURCE
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events_empty_partitioned

# ── logs_db ───────────────────────────────────────────────────────────────────
# app_logs: TEXTFILE, dt string partition, registered
hdfs dfs -mkdir -p ${WH}/logs_db.db/app_logs/dt=2024-01-01
hdfs dfs -mkdir -p ${WH}/logs_db.db/app_logs/dt=2024-01-15
hdfs dfs -mkdir -p ${WH}/logs_db.db/app_logs/dt=2024-02-01
# error_logs_empty: TEXTFILE, dt, EMPTY_SOURCE
hdfs dfs -mkdir -p ${WH}/logs_db.db/error_logs_empty
# metrics_empty_nonpartitioned: TEXTFILE, non-partitioned, EMPTY_SOURCE
hdfs dfs -mkdir -p ${WH}/logs_db.db/metrics_empty_nonpartitioned

echo "  HDFS directories created."

echo ""
echo "============================================================"
echo " [2/6] Writing TEXTFILE data to HDFS"
echo "============================================================"

# ── sales_db.orders ───────────────────────────────────────────────────────────
echo -e "1,Alice,100\n2,Bob,200\n3,Carol,150" \
  | hdfs dfs -put -f - ${WH}/sales_db.db/orders/year=2024/month=1/data.csv
echo -e "4,Dave,90\n5,Eve,310" \
  | hdfs dfs -put -f - ${WH}/sales_db.db/orders/year=2024/month=2/data.csv
echo -e "6,Frank,400\n7,Grace,250\n8,Hank,180\n9,Ivy,320\n10,Jack,110" \
  | hdfs dfs -put -f - ${WH}/sales_db.db/orders/year=2024/month=3/data.csv

echo -e "1,Alice,alice@example.com\n2,Bob,bob@example.com\n3,Carol,carol@example.com\n4,Dave,dave@example.com\n5,Eve,eve@example.com" \
  | hdfs dfs -put -f - ${WH}/sales_db.db/customers/data.csv
echo -e "1|101|2024-01-05|50.00\n2|102|2024-01-10|75.50\n3|103|2024-02-01|120.00\n4|104|2024-02-15|30.25" \
  | hdfs dfs -put -f - ${WH}/sales_db.db/transactions/data.csv

# ── hr_db.employees (with \N NULLs) ──────────────────────────────────────────
echo -e "1,Alice,Engineering,\\N\n2,Bob,HR,50000\n3,Carol,Engineering,55000\n4,Dave,\\N,48000\n5,Eve,Finance,62000" \
  | hdfs dfs -put -f - ${WH}/hr_db.db/employees/data.csv
# hr_db.departments — dir exists, no data (EMPTY_SOURCE)

# ── analytics_db.events ───────────────────────────────────────────────────────
echo -e "e1,click,US\ne2,view,US\ne3,buy,US" \
  | hdfs dfs -put -f - ${WH}/analytics_db.db/events/region=US/year=2024/month=1/data.csv
echo -e "e4,click,EU\ne5,view,EU" \
  | hdfs dfs -put -f - ${WH}/analytics_db.db/events/region=EU/year=2024/month=1/data.csv
echo -e "e6,view,US\ne7,buy,US" \
  | hdfs dfs -put -f - ${WH}/analytics_db.db/events/region=US/year=2024/month=2/data.csv
echo -e "e8,click,US\ne9,view,US" \
  | hdfs dfs -put -f - ${WH}/analytics_db.db/events/region=US/year=2024/month=3/data.csv

# ── analytics_db.sessions (UNREGISTERED partitions — data exists, no ALTER TABLE) ──
echo -e "s1,userA,300\ns2,userB,450" \
  | hdfs dfs -put -f - ${WH}/analytics_db.db/sessions/year=2024/month=1/data.csv
echo -e "s3,userC,120\ns4,userD,600" \
  | hdfs dfs -put -f - ${WH}/analytics_db.db/sessions/year=2024/month=2/data.csv

# ── logs_db.app_logs ──────────────────────────────────────────────────────────
echo -e "2024-01-01,INFO,app started\n2024-01-01,WARN,slow query" \
  | hdfs dfs -put -f - ${WH}/logs_db.db/app_logs/dt=2024-01-01/data.csv
echo -e "2024-01-15,ERROR,timeout\n2024-01-15,INFO,retry ok" \
  | hdfs dfs -put -f - ${WH}/logs_db.db/app_logs/dt=2024-01-15/data.csv
echo -e "2024-02-01,INFO,deploy\n2024-02-01,INFO,health ok\n2024-02-01,DEBUG,trace" \
  | hdfs dfs -put -f - ${WH}/logs_db.db/app_logs/dt=2024-02-01/data.csv

echo "  TEXTFILE data written."

echo ""
echo "============================================================"
echo " [3/6] Writing ORC / PARQUET / AVRO data via PySpark"
echo "============================================================"

# ── analytics_db.events_orc (ORC, 3-level partition) ─────────────────────────
pyspark --master local[*] << 'PYEOF'
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
WH = "hdfs://localhost:9000/user/hive/warehouse"
data = [
    ("e1","click","US","US",2024,1),
    ("e2","view","US","US",2024,1),
    ("e3","buy","US","US",2024,1),
    ("e4","click","EU","EU",2024,1),
    ("e5","view","EU","EU",2024,1),
]
schema = StructType([
    StructField("event_id",   StringType()),
    StructField("event_type", StringType()),
    StructField("user_region",StringType()),
    StructField("region",     StringType()),
    StructField("year",       IntegerType()),
    StructField("month",      IntegerType()),
])
df = spark.createDataFrame(data, schema)
df.filter("region = 'US'").drop("region","year","month").write.mode("overwrite").orc(
    "{0}/analytics_db.db/events_orc/region=US/year=2024/month=1".format(WH))
df.filter("region = 'EU'").drop("region","year","month").write.mode("overwrite").orc(
    "{0}/analytics_db.db/events_orc/region=EU/year=2024/month=1".format(WH))
df.filter("region = 'US' AND month = 2").drop("region","year","month").write.mode("overwrite").orc(
    "{0}/analytics_db.db/events_orc/region=US/year=2024/month=2".format(WH))
df.filter("region = 'US' AND month = 3").drop("region","year","month").write.mode("overwrite").orc(
    "{0}/analytics_db.db/events_orc/region=US/year=2024/month=3".format(WH))
spark.stop()
PYEOF
echo "  ORC data written."

# ── hr_db.employees_parquet (PARQUET, non-partitioned) ───────────────────────
pyspark --master local[*] << 'PYEOF'
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
WH = "hdfs://localhost:9000/user/hive/warehouse"
data = [
    (1,"Alice","Engineering",75000.0),
    (2,"Bob","HR",50000.0),
    (3,"Carol","Engineering",55000.0),
    (4,"Dave","Finance",48000.0),
    (5,"Eve","Finance",62000.0),
]
schema = StructType([
    StructField("emp_id",  IntegerType()),
    StructField("name",    StringType()),
    StructField("dept",    StringType()),
    StructField("salary",  DoubleType()),
])
df = spark.createDataFrame(data, schema)
df.write.mode("overwrite").parquet("{0}/hr_db.db/employees_parquet".format(WH))
spark.stop()
PYEOF
echo "  PARQUET data written."

# -- hr_db.employees_avro (AVRO, non-partitioned) --
# No pip, no avro library available. Write a valid Avro binary file using
# pure Python3 stdlib only (struct + io + json + hashlib).
# Avro container file format spec: https://avro.apache.org/docs/1.7.7/spec.html
echo "  Writing AVRO data using pure Python3 (no pip/no avro lib)..."
python3 << 'PYEOF'
import struct, io, json, os, subprocess, sys, hashlib, random

# ---------------------------------------------------------------------------
# Minimal Avro container writer (stdlib only, no avro-python3 needed)
# Implements: Avro Object Container File format v1
# ---------------------------------------------------------------------------
SCHEMA = {
    "type": "record",
    "name": "Employee",
    "fields": [
        {"name": "emp_id",  "type": "int"},
        {"name": "name",    "type": "string"},
        {"name": "dept",    "type": "string"},
        {"name": "salary",  "type": "double"}
    ]
}
RECORDS = [
    {"emp_id": 1, "name": "Alice", "dept": "Engineering", "salary": 75000.0},
    {"emp_id": 2, "name": "Bob",   "dept": "HR",          "salary": 50000.0},
    {"emp_id": 3, "name": "Carol", "dept": "Engineering", "salary": 55000.0},
]

def encode_long(n):
    """Avro zigzag + varint encoding for long/int."""
    n = (n << 1) ^ (n >> 63)
    buf = b""
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            buf += bytes([b | 0x80])
        else:
            buf += bytes([b])
            break
    return buf

def encode_string(s):
    b = s.encode("utf-8")
    return encode_long(len(b)) + b

def encode_double(f):
    return struct.pack("<d", f)

def encode_record(rec):
    return (
        encode_long(rec["emp_id"]) +
        encode_string(rec["name"]) +
        encode_string(rec["dept"]) +
        encode_double(rec["salary"])
    )

def write_avro(records, schema_dict):
    MAGIC = b"Obj\x01"
    sync_marker = os.urandom(16)
    schema_str = json.dumps(schema_dict)

    # File-level metadata block (map encoding)
    def encode_map(kv):
        out = encode_long(len(kv))
        for k, v in kv.items():
            if isinstance(v, str):
                v = v.encode("utf-8")
            out += encode_string(k) + encode_long(len(v)) + v
        return out + encode_long(0)  # end of map

    meta = encode_map({
        "avro.schema": schema_str,
        "avro.codec":  "null",
    })

    # Encode all records into one data block
    block = b"".join(encode_record(r) for r in records)
    block_bytes = (
        encode_long(len(records)) +   # object count
        encode_long(len(block)) +     # byte count
        block +
        sync_marker
    )

    return MAGIC + meta + sync_marker + block_bytes

avro_bytes = write_avro(RECORDS, SCHEMA)

local_path = "/tmp/employees.avro"
with open(local_path, "wb") as f:
    f.write(avro_bytes)

ret = subprocess.call([
    "hdfs", "dfs", "-put", "-f", local_path,
    "hdfs://localhost:9000/user/hive/warehouse/hr_db.db/employees_avro/employees.avro"
])
if ret == 0:
    print("AVRO file written to HDFS successfully ({} bytes)".format(len(avro_bytes)))
else:
    print("WARNING: hdfs put failed — employees_avro will have 0 rows (DDL still created in [4/6])")
PYEOF
echo "  AVRO data written."



echo ""
echo "============================================================"
echo " [4/6] Creating Hive databases and tables"
echo "============================================================"

cat > /tmp/setup-test-data.sql << 'SQL_EOF'
-- =============================================================================
-- sales_db
-- =============================================================================
CREATE DATABASE IF NOT EXISTS sales_db;

-- SCENARIO 1 / 4 / 18 / 23 / 25: TEXTFILE, year/month partitions, registered
CREATE TABLE IF NOT EXISTS sales_db.orders (
  order_id INT, customer STRING, amount DOUBLE
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders';

ALTER TABLE sales_db.orders ADD IF NOT EXISTS PARTITION (year=2024, month=1);
ALTER TABLE sales_db.orders ADD IF NOT EXISTS PARTITION (year=2024, month=2);
ALTER TABLE sales_db.orders ADD IF NOT EXISTS PARTITION (year=2024, month=3);

-- SCENARIO 12: EMPTY_SOURCE, year/month partition, 0 files, 0 registered partitions
CREATE TABLE IF NOT EXISTS sales_db.orders_empty (
  order_id INT, customer STRING, amount DOUBLE
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders_empty';
-- Intentionally: no ALTER TABLE ADD PARTITION

CREATE TABLE IF NOT EXISTS sales_db.customers (
  cust_id INT, name STRING, email STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/sales_db.db/customers';

CREATE TABLE IF NOT EXISTS sales_db.transactions (
  txn_id INT, order_id INT, txn_date STRING, amount DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/sales_db.db/transactions';

-- =============================================================================
-- hr_db
-- =============================================================================
CREATE DATABASE IF NOT EXISTS hr_db;

-- SCENARIO 2 / 3 / 24: TEXTFILE, non-partitioned, \N NULLs
CREATE TABLE IF NOT EXISTS hr_db.employees (
  emp_id INT, name STRING, dept STRING, salary DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
NULL DEFINED AS '\N'
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db.db/employees';

-- SCENARIO 3: EMPTY_SOURCE, non-partitioned (listed via comma-sep with employees)
CREATE TABLE IF NOT EXISTS hr_db.departments (
  dept_id INT, dept_name STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db.db/departments';
-- Intentionally: no data written

-- SCENARIO 14 / 19: PARQUET, non-partitioned
CREATE TABLE IF NOT EXISTS hr_db.employees_parquet (
  emp_id INT, name STRING, dept STRING, salary DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db.db/employees_parquet';

-- SCENARIO 15: AVRO, non-partitioned
CREATE TABLE IF NOT EXISTS hr_db.employees_avro (
  emp_id INT, name STRING, dept STRING, salary DOUBLE
)
STORED AS AVRO
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db.db/employees_avro';

-- =============================================================================
-- analytics_db
-- =============================================================================
CREATE DATABASE IF NOT EXISTS analytics_db;

-- SCENARIO 7 / 8 / 22: TEXTFILE, 3-level partition region/year/month, registered
CREATE TABLE IF NOT EXISTS analytics_db.events (
  event_id STRING, event_type STRING, user_region STRING
)
PARTITIONED BY (region STRING, year INT, month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/analytics_db.db/events';

ALTER TABLE analytics_db.events ADD IF NOT EXISTS PARTITION (region='US', year=2024, month=1);
ALTER TABLE analytics_db.events ADD IF NOT EXISTS PARTITION (region='EU', year=2024, month=1);
ALTER TABLE analytics_db.events ADD IF NOT EXISTS PARTITION (region='US', year=2024, month=2);
ALTER TABLE analytics_db.events ADD IF NOT EXISTS PARTITION (region='US', year=2024, month=3);

-- SCENARIO 6 / 22: TEXTFILE, year/month, UNREGISTERED partitions
--   Data files exist in HDFS but no ALTER TABLE run → unregistered_partitions=True
--   DAG must detect this and run MSCK REPAIR on destination
CREATE TABLE IF NOT EXISTS analytics_db.sessions (
  session_id STRING, user_id STRING, duration_sec INT
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/analytics_db.db/sessions';
-- Intentionally: no ALTER TABLE ADD PARTITION

-- SCENARIO 13: ORC, 3-level partition region/year/month, registered
CREATE TABLE IF NOT EXISTS analytics_db.events_orc (
  event_id STRING, event_type STRING, user_region STRING
)
PARTITIONED BY (region STRING, year INT, month INT)
STORED AS ORC
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/analytics_db.db/events_orc';

ALTER TABLE analytics_db.events_orc ADD IF NOT EXISTS PARTITION (region='US', year=2024, month=1);
ALTER TABLE analytics_db.events_orc ADD IF NOT EXISTS PARTITION (region='EU', year=2024, month=1);
ALTER TABLE analytics_db.events_orc ADD IF NOT EXISTS PARTITION (region='US', year=2024, month=2);
ALTER TABLE analytics_db.events_orc ADD IF NOT EXISTS PARTITION (region='US', year=2024, month=3);

-- SCENARIO 11: EMPTY_SOURCE, 3-level partition, 0 files, 0 registered partitions
CREATE TABLE IF NOT EXISTS analytics_db.events_empty_partitioned (
  event_id STRING, event_type STRING, user_region STRING
)
PARTITIONED BY (region STRING, year INT, month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/analytics_db.db/events_empty_partitioned';
-- Intentionally: no ALTER TABLE ADD PARTITION

-- =============================================================================
-- logs_db
-- =============================================================================
CREATE DATABASE IF NOT EXISTS logs_db;

-- SCENARIO 16 / 17 / 21: TEXTFILE with dt string partition, custom delimiter, registered
CREATE TABLE IF NOT EXISTS logs_db.app_logs (
  log_date STRING, level STRING, message STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/logs_db.db/app_logs';

ALTER TABLE logs_db.app_logs ADD IF NOT EXISTS PARTITION (dt='2024-01-01');
ALTER TABLE logs_db.app_logs ADD IF NOT EXISTS PARTITION (dt='2024-01-15');
ALTER TABLE logs_db.app_logs ADD IF NOT EXISTS PARTITION (dt='2024-02-01');

-- SCENARIO 9: EMPTY_SOURCE, dt partition, 0 files, 0 registered partitions
--   Typical real-world case: log table in metastore that has never received data
CREATE TABLE IF NOT EXISTS logs_db.error_logs_empty (
  log_date STRING, level STRING, message STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/logs_db.db/error_logs_empty';
-- Intentionally: no ALTER TABLE ADD PARTITION

-- SCENARIO 10: EMPTY_SOURCE, non-partitioned flat table, 0 files
--   Cleanest isolated test for EMPTY_SOURCE guard with no partition logic
CREATE TABLE IF NOT EXISTS logs_db.metrics_empty_nonpartitioned (
  metric_id INT, name STRING, value DOUBLE, ts BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/logs_db.db/metrics_empty_nonpartitioned';



SQL_EOF

$BEELINE -f /tmp/setup-test-data.sql
echo "  Hive DDL executed."

echo ""
echo "============================================================"
echo " [5/6] Verification"
echo "============================================================"

echo ""
echo "--- Hive databases ---"
$BEELINE -e "SHOW DATABASES;"

echo ""
echo "--- Row counts + partition registrations (via PySpark to avoid MR) ---"
pyspark --master local[*] << 'PYEOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

checks = [
    ("sales_db.orders",                         None),
    ("sales_db.orders",                         "year=2024 AND month=1"),
    ("sales_db.orders_empty",                   None),
    ("hr_db.employees",                         None),
    ("hr_db.departments",                       None),
    ("hr_db.employees_parquet",                 None),
    ("hr_db.employees_avro",                    None),
    ("analytics_db.events",                     None),
    ("analytics_db.events",                     "region='US' AND year=2024"),
    ("analytics_db.sessions",                   None),
    ("analytics_db.events_orc",                 None),
    ("analytics_db.events_empty_partitioned",   None),
    ("logs_db.app_logs",                        None),
    ("logs_db.app_logs",                        "dt>='2024-01-15'"),
    ("logs_db.error_logs_empty",                None),
    ("logs_db.metrics_empty_nonpartitioned",    None),
]

print("")
print("{:<50} {:>8}  {}".format("TABLE", "ROWS", "STATUS"))
print("-" * 70)
for tbl, where in checks:
    label = "{} WHERE {}".format(tbl, where) if where else tbl
    try:
        q = "SELECT COUNT(*) as c FROM {}".format(tbl)
        if where:
            q += " WHERE {}".format(where)
        cnt = spark.sql(q).collect()[0]["c"]
        # For empty-source tables 0 is expected; for data tables >0 is expected
        empty_tables = {
            "sales_db.orders_empty", "hr_db.departments",
            "analytics_db.events_empty_partitioned",
            "logs_db.error_logs_empty", "logs_db.metrics_empty_nonpartitioned"
        }
        expected_empty = tbl in empty_tables and where is None
        ok = (cnt == 0 and expected_empty) or (cnt > 0 and not expected_empty)
        status = "OK" if ok else "WARN"
        print("{:<50} {:>8}  {}".format(label[:50], cnt, status))
    except Exception as e:
        print("{:<50} {:>8}  ERROR: {}".format(label[:50], "?", str(e)[:60]))

print("")
print("--- Partition registrations ---")
part_tables = [
    "sales_db.orders",
    "analytics_db.events",
    "analytics_db.sessions",
    "analytics_db.events_orc",
    "logs_db.app_logs",
]
for tbl in part_tables:
    try:
        parts = spark.sql("SHOW PARTITIONS {}".format(tbl)).collect()
        partlist = [r[0] for r in parts]
        registered = len(partlist)
        note = ""
        if tbl == "analytics_db.sessions" and registered == 0:
            note = "  <- CORRECT: unregistered partitions test"
        elif tbl == "analytics_db.sessions" and registered > 0:
            note = "  <- WARN: expected 0 registered partitions"
        print("  {}: {} partition(s){}".format(tbl, registered, note))
        for p in partlist:
            print("      {}".format(p))
    except Exception as e:
        print("  {}: ERROR - {}".format(tbl, str(e)[:80]))

spark.stop()
PYEOF

echo ""
echo "--- Unregistered-partition check ---"
echo "  (See 'analytics_db.sessions' line above — should show 0 registered partitions)"

echo ""
echo "============================================================"
echo " [6/6] Test scenario reference"
echo "============================================================"
cat << 'SCENARIO_EOF'
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
SCENARIO_EOF

echo ""
echo "Done.  Test data is ready."