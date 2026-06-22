#!/bin/bash
# =============================================================================
# setup-test-data-combined.sh
#
# Combined source test data script — merges:
#   PART A: setup-test-data.sh       (DAG migration scenarios 1-25, 26-34,
#                                      hive_type_to_spark_ddl struct/complex
#                                      type test cases, empty-parquet schema)
#   PART B: setup-test-data-tz.sh    (error-based-retry regression suite,
#                                      transient/permanent retry test cases,
#                                      timezone/TIMESTAMP test tables)
#
# Run INSIDE the container:
#   docker exec -u root mapr-edge-node bash /setup-test-data-combined.sh
#   (or)  kubectl exec -n hadoop-ssh <pod> -- bash /setup-test-data-combined.sh
#
# Safe to re-run — existing HDFS files and Hive tables are overwritten/skipped
# (PART A uses CREATE TABLE IF NOT EXISTS; PART B uses DROP+CREATE TABLE).
#
# -----------------------------------------------------------------------------
# DATABASE NAME COLLISION NOTICE
# -----------------------------------------------------------------------------
# Both source scripts originally defined databases named hr_db, sales_db,
# analytics_db, and logs_db -- but with different, INCOMPATIBLE schemas and
# partition layouts for each (different columns, different formats, different
# partition keys). Combining them under the same names would silently corrupt
# one test suite's tables with the other's DROP/CREATE statements.
#
# To preserve every test scenario from both scripts unchanged, PART B's
# (setup-test-data-tz.sh) copies of these four databases have been renamed
# with a "_tz" suffix:
#
#     ORIGINAL (setup-test-data-tz.sh)   ->   RENAMED IN THIS SCRIPT
#     hr_db                              ->   hr_db_tz
#     sales_db                           ->   sales_db_tz
#     analytics_db                       ->   analytics_db_tz
#     logs_db                            ->   logs_db_tz
#
# All other databases were already unique across both scripts and are
# untouched: migration_db, metrics_db, audit_db, edge_cases_db, formats_db,
# tz_db, retry_test_db, corrupt_test_db, struct_db, does_not_exist_db.
#
# -----------------------------------------------------------------------------
# COMBINED TEST CASE COVERAGE
# -----------------------------------------------------------------------------
# PART A (original DBs: sales_db, hr_db, analytics_db, logs_db, struct_db)
#   DAG migration scenarios      : 1-25
#   hive_type_to_spark_ddl       : TC-6,7,9,11,12,13,14,15,16,20 (struct/complex)
#   Integration tests            : TC-26,27,28,29,30,31,32
#   Empty parquet schema         : TC-33 (hr_db.employees_parquet_empty)
#   Empty parquet partitioned    : TC-34 (analytics_db.events_parquet_empty_partitioned)
#
# PART B (renamed DBs: hr_db_tz, sales_db_tz, analytics_db_tz, logs_db_tz;
#         plus migration_db, metrics_db, audit_db, edge_cases_db, formats_db,
#         tz_db, retry_test_db, corrupt_test_db)
#   Regression baseline           : R-01..R-20
#   Transient retry cases         : T-01..T-05
#   Permanent error cases         : P-01..P-07
#   TZ / TIMESTAMP test tables    : R-20 (tz_db.*_la)
# =============================================================================

set -euo pipefail

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

BEELINE="/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true"
WH="hdfs://localhost:9000/user/hive/warehouse"

# ─────────────────────────────────────────────────────────────────────────────
echo "============================================================"
echo " [1/11] Creating HDFS directory structure"
echo "============================================================"

# ── sales_db ──────────────────────────────────────────────────────────────────
hdfs dfs -mkdir -p ${WH}/sales_db.db/orders/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/sales_db.db/orders/year=2024/month=2
hdfs dfs -mkdir -p ${WH}/sales_db.db/orders/year=2024/month=3
hdfs dfs -mkdir -p ${WH}/sales_db.db/orders_empty
hdfs dfs -mkdir -p ${WH}/sales_db.db/customers
hdfs dfs -mkdir -p ${WH}/sales_db.db/transactions

# ── hr_db ─────────────────────────────────────────────────────────────────────
hdfs dfs -mkdir -p ${WH}/hr_db.db/employees
hdfs dfs -mkdir -p ${WH}/hr_db.db/departments
hdfs dfs -mkdir -p ${WH}/hr_db.db/employees_parquet
hdfs dfs -mkdir -p ${WH}/hr_db.db/employees_avro
# TC-33: empty parquet table with schema — directory exists but no data files
hdfs dfs -mkdir -p ${WH}/hr_db.db/employees_parquet_empty

# ── analytics_db ──────────────────────────────────────────────────────────────
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events/region=US/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events/region=EU/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events/region=US/year=2024/month=2
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events/region=US/year=2024/month=3
hdfs dfs -mkdir -p ${WH}/analytics_db.db/sessions/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db.db/sessions/year=2024/month=2
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events_orc/region=US/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events_orc/region=EU/year=2024/month=1
# TC-11: partitions registered but no data files in any partition
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events_empty_partitioned/region=US/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events_empty_partitioned/region=EU/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events_empty_partitioned/region=US/year=2024/month=2
# TC-34: PARQUET equivalent of TC-11 — same shape, same registered-but-empty semantics,
# exercises the EMPTY_SOURCE path for Parquet-format partitioned tables specifically
# (distinct from TC-11 which is TEXTFILE and TC-33 which is non-partitioned Parquet)
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events_parquet_empty_partitioned/region=US/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events_parquet_empty_partitioned/region=EU/year=2024/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db.db/events_parquet_empty_partitioned/region=US/year=2024/month=2

# ── logs_db ───────────────────────────────────────────────────────────────────
hdfs dfs -mkdir -p ${WH}/logs_db.db/app_logs/dt=2024-01-01
hdfs dfs -mkdir -p ${WH}/logs_db.db/app_logs/dt=2024-01-15
hdfs dfs -mkdir -p ${WH}/logs_db.db/app_logs/dt=2024-02-01
hdfs dfs -mkdir -p ${WH}/logs_db.db/error_logs_empty
hdfs dfs -mkdir -p ${WH}/logs_db.db/metrics_empty_nonpartitioned

# ── struct_db (NEW) ───────────────────────────────────────────────────────────
# Covers hive_type_to_spark_ddl test cases:
#   TC-6,7   simple_struct            struct<a:int,b:string>
#   TC-9     array_of_struct          array<struct<x:int,y:string>>
#   TC-11    map_of_struct            map<string,struct<a:int>>
#   TC-12    deep_nested_struct       struct<a:array<struct<b:map<string,int>>>>
#   TC-13    array_of_array_struct    array<array<struct<x:int>>>
#   TC-14    struct_with_decimal      struct<amount:decimal(18,4),qty:int>
#   TC-15    struct_with_varchar      struct<name:varchar(255),code:char(10)>
#   TC-16    already_converted        struct<a int> (idempotent check)
#   TC-20    mixed_case_struct        STRUCT<A:INT,B:STRING>
#   TC-26/27 flex_rules_result_mini   real customerAttributes + response columns
#   TC-28    flex_rules_result_full   full production table replica
#   TC-29    struct_partition_col     partitioned by a struct column (unusual)
hdfs dfs -mkdir -p ${WH}/struct_db.db/simple_struct
hdfs dfs -mkdir -p ${WH}/struct_db.db/array_of_struct
hdfs dfs -mkdir -p ${WH}/struct_db.db/map_of_struct
hdfs dfs -mkdir -p ${WH}/struct_db.db/deep_nested_struct
hdfs dfs -mkdir -p ${WH}/struct_db.db/array_of_array_struct
hdfs dfs -mkdir -p ${WH}/struct_db.db/struct_with_decimal
hdfs dfs -mkdir -p ${WH}/struct_db.db/struct_with_varchar
hdfs dfs -mkdir -p ${WH}/struct_db.db/already_converted
hdfs dfs -mkdir -p ${WH}/struct_db.db/mixed_case_struct
hdfs dfs -mkdir -p ${WH}/struct_db.db/flex_rules_result_mini
hdfs dfs -mkdir -p ${WH}/struct_db.db/flex_rules_result_full/capbusinesseffectivedate=2024-01-01
hdfs dfs -mkdir -p ${WH}/struct_db.db/flex_rules_result_full/capbusinesseffectivedate=2024-02-01

echo "  HDFS directories created."

echo ""
echo "============================================================"
echo " [2/11] Writing TEXTFILE data to HDFS"
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

# ── analytics_db.events ───────────────────────────────────────────────────────
echo -e "e1,click,US\ne2,view,US\ne3,buy,US" \
  | hdfs dfs -put -f - ${WH}/analytics_db.db/events/region=US/year=2024/month=1/data.csv
echo -e "e4,click,EU\ne5,view,EU" \
  | hdfs dfs -put -f - ${WH}/analytics_db.db/events/region=EU/year=2024/month=1/data.csv
echo -e "e6,view,US\ne7,buy,US" \
  | hdfs dfs -put -f - ${WH}/analytics_db.db/events/region=US/year=2024/month=2/data.csv
echo -e "e8,click,US\ne9,view,US" \
  | hdfs dfs -put -f - ${WH}/analytics_db.db/events/region=US/year=2024/month=3/data.csv

# ── analytics_db.sessions (unregistered partitions) ──────────────────────────
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

# ── analytics_db.events_empty_partitioned ────────────────────────────────────
# TC-11: partition directories exist and are registered in the metastore,
# but no data files are written — every partition is empty.
# ── analytics_db.events_parquet_empty_partitioned ─────────────────────────────
# TC-34: same as TC-11 but STORED AS PARQUET — no data written into any partition.

echo "  TEXTFILE data written."

echo ""
echo "============================================================"
echo " [3/11] Writing ORC / PARQUET / AVRO / struct data via PySpark"
echo "============================================================"

# ── analytics_db.events_orc ───────────────────────────────────────────────────
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
spark.stop()
PYEOF
echo "  ORC data written."

# ── hr_db.employees_parquet ───────────────────────────────────────────────────
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

# ── hr_db.employees_parquet_empty (TC-33) ────────────────────────────────────
# Empty Parquet table: writes a schema-only Parquet file (zero rows) so that
# the metastore knows the column definitions but COUNT(*) returns 0.
# This tests that the migration DAG handles EMPTY_SOURCE correctly for a
# Parquet-format table — distinct from TEXTFILE empty tables already covered.
pyspark --master local[*] << 'PYEOF'
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
WH = "hdfs://localhost:9000/user/hive/warehouse"

# Schema matches the Hive DDL defined in [4/11] below.
# Uses a realistic employee-profile shape with several common column types
# to exercise schema inference from an empty-but-schema-bearing Parquet file.
schema = StructType([
    StructField("emp_id",      IntegerType(),  True),
    StructField("name",        StringType(),   True),
    StructField("dept",        StringType(),   True),
    StructField("job_title",   StringType(),   True),
    StructField("salary",      DoubleType(),   True),
    StructField("hire_date",   StringType(),   True),   # stored as string; DDL uses STRING
    StructField("is_active",   StringType(),   True),   # 'Y'/'N' flag
    StructField("cost_centre", StringType(),   True),
])

empty_df = spark.createDataFrame([], schema)
empty_df.write.mode("overwrite").parquet(
    "{}/hr_db.db/employees_parquet_empty".format(WH))
print("  employees_parquet_empty written (0 rows, schema preserved in Parquet footer)")
spark.stop()
PYEOF
echo "  Empty PARQUET schema written."

# ── hr_db.employees_avro ──────────────────────────────────────────────────────
echo "  Writing AVRO data using pure Python3 (no pip/no avro lib)..."
python3 << 'PYEOF'
import struct, io, json, os, subprocess

SCHEMA = {
    "type": "record", "name": "Employee",
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
    return (encode_long(rec["emp_id"]) + encode_string(rec["name"]) +
            encode_string(rec["dept"]) + encode_double(rec["salary"]))

def write_avro(records, schema_dict):
    MAGIC = b"Obj\x01"
    sync_marker = os.urandom(16)
    schema_str = json.dumps(schema_dict)
    def encode_map(kv):
        out = encode_long(len(kv))
        for k, v in kv.items():
            if isinstance(v, str):
                v = v.encode("utf-8")
            out += encode_string(k) + encode_long(len(v)) + v
        return out + encode_long(0)
    meta = encode_map({"avro.schema": schema_str, "avro.codec": "null"})
    block = b"".join(encode_record(r) for r in records)
    block_bytes = (encode_long(len(records)) + encode_long(len(block)) +
                   block + sync_marker)
    return MAGIC + meta + sync_marker + block_bytes

avro_bytes = write_avro(RECORDS, SCHEMA)
local_path = "/tmp/employees.avro"
with open(local_path, "wb") as f:
    f.write(avro_bytes)
ret = subprocess.call([
    "hdfs", "dfs", "-put", "-f", local_path,
    "hdfs://localhost:9000/user/hive/warehouse/hr_db.db/employees_avro/employees.avro"
])
print("AVRO written ({} bytes), hdfs exit={}".format(len(avro_bytes), ret))
PYEOF
echo "  AVRO data written."

# =============================================================================
# struct_db — Parquet data for all hive_type_to_spark_ddl test cases
# Written via PySpark so Spark can read them back for row-count validation.
# =============================================================================
pyspark --master local[*] << 'PYEOF'
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    ArrayType, MapType, DecimalType, ShortType, LongType
)
from decimal import Decimal
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
WH = "hdfs://localhost:9000/user/hive/warehouse"

# ── TC-6/7: simple_struct  struct<a:int,b:string> ─────────────────────────────
# Hive metastore will return "struct<a:int,b:string>" from DESCRIBE.
# After hive_type_to_spark_ddl → "struct<a int,b string>"
schema_simple = StructType([
    StructField("id",  IntegerType(), True),
    StructField("info", StructType([
        StructField("a", IntegerType(),  True),
        StructField("b", StringType(),   True),
    ]), True),
])
data_simple = [
    (1, (10, "hello")),
    (2, (20, "world")),
    (3, (30, "foo")),
]
spark.createDataFrame(data_simple, schema_simple) \
     .write.mode("overwrite").parquet("{}/struct_db.db/simple_struct".format(WH))
print("  simple_struct written (3 rows)")

# ── TC-9: array_of_struct  array<struct<x:int,y:string>> ─────────────────────
schema_aos = StructType([
    StructField("id",    IntegerType(), True),
    StructField("items", ArrayType(StructType([
        StructField("x", IntegerType(), True),
        StructField("y", StringType(),  True),
    ])), True),
])
data_aos = [
    (1, [(1, "a"), (2, "b")]),
    (2, [(3, "c")]),
    (3, []),
]
spark.createDataFrame(data_aos, schema_aos) \
     .write.mode("overwrite").parquet("{}/struct_db.db/array_of_struct".format(WH))
print("  array_of_struct written (3 rows)")

# ── TC-11: map_of_struct  map<string,struct<a:int>> ───────────────────────────
schema_mos = StructType([
    StructField("id",  IntegerType(), True),
    StructField("tags", MapType(StringType(), StructType([
        StructField("a", IntegerType(), True),
    ])), True),
])
data_mos = [
    (1, {"k1": (100,), "k2": (200,)}),
    (2, {"k3": (300,)}),
    (3, {}),
]
spark.createDataFrame(data_mos, schema_mos) \
     .write.mode("overwrite").parquet("{}/struct_db.db/map_of_struct".format(WH))
print("  map_of_struct written (3 rows)")

# ── TC-12: deep_nested_struct  struct<a:array<struct<b:map<string,int>>>> ──────
schema_deep = StructType([
    StructField("id", IntegerType(), True),
    StructField("payload", StructType([
        StructField("a", ArrayType(StructType([
            StructField("b", MapType(StringType(), IntegerType()), True),
        ])), True),
    ]), True),
])
data_deep = [
    (1, ([( {"k": 1},)],)),
    (2, ([( {"x": 2, "y": 3},)],)),
    (3, (None,)),
]
spark.createDataFrame(data_deep, schema_deep) \
     .write.mode("overwrite").parquet("{}/struct_db.db/deep_nested_struct".format(WH))
print("  deep_nested_struct written (3 rows)")

# ── TC-13: array_of_array_struct  array<array<struct<x:int>>> ────────────────
schema_aas = StructType([
    StructField("id",   IntegerType(), True),
    StructField("grid", ArrayType(ArrayType(StructType([
        StructField("x", IntegerType(), True),
    ]))), True),
])
data_aas = [
    (1, [[(1,), (2,)], [(3,)]]),
    (2, [[(4,)]]),
    (3, []),
]
spark.createDataFrame(data_aas, schema_aas) \
     .write.mode("overwrite").parquet("{}/struct_db.db/array_of_array_struct".format(WH))
print("  array_of_array_struct written (3 rows)")

# ── TC-14: struct_with_decimal  struct<amount:decimal(18,4),qty:int> ──────────
# Hive returns "struct<amount:decimal(18,4),qty:int>"
# After conversion → "struct<amount decimal(18,4),qty int>"
# decimal(18,4) must pass through the converter unchanged (parameterised primitive)
schema_dec = StructType([
    StructField("id", IntegerType(), True),
    StructField("price_info", StructType([
        StructField("amount", DecimalType(18, 4), True),
        StructField("qty",    IntegerType(),       True),
    ]), True),
])
data_dec = [
    (1, (Decimal("99.9900"),  5)),
    (2, (Decimal("1234.5678"), 1)),
    (3, (Decimal("0.0001"),  100)),
]
spark.createDataFrame(data_dec, schema_dec) \
     .write.mode("overwrite").parquet("{}/struct_db.db/struct_with_decimal".format(WH))
print("  struct_with_decimal written (3 rows)")

# ── TC-15: struct_with_varchar  struct<name:varchar(255),code:char(10)> ───────
# Hive returns varchar/char types in DESCRIBE; converter must pass them through.
# Spark internally maps varchar(n) and char(n) to string in Parquet storage.
schema_vc = StructType([
    StructField("id",     IntegerType(), True),
    StructField("labels", StructType([
        StructField("name", StringType(), True),   # stored as string, DDL uses varchar(255)
        StructField("code", StringType(), True),   # stored as string, DDL uses char(10)
    ]), True),
])
data_vc = [
    (1, ("Alice Smith",  "ENG001")),
    (2, ("Bob Jones",    "HR0002")),
    (3, ("Carol Davis",  "FIN003")),
]
spark.createDataFrame(data_vc, schema_vc) \
     .write.mode("overwrite").parquet("{}/struct_db.db/struct_with_varchar".format(WH))
print("  struct_with_varchar written (3 rows)")

# ── TC-16: already_converted  struct<a int,b string>  (idempotent check) ──────
# The Hive DDL for this table deliberately uses space-separated notation.
# hive_type_to_spark_ddl must return it unchanged (no double-conversion).
# Data schema is identical to simple_struct so we can reuse the same rows.
spark.createDataFrame(data_simple, schema_simple) \
     .write.mode("overwrite").parquet("{}/struct_db.db/already_converted".format(WH))
print("  already_converted written (3 rows)")

# ── TC-20: mixed_case_struct  STRUCT<A:INT,B:STRING> ─────────────────────────
# Hive metastore can return type strings in uppercase for some configurations.
# hive_type_to_spark_ddl uses case-insensitive matching; verify conversion works.
schema_mc = StructType([
    StructField("id",   IntegerType(), True),
    StructField("info", StructType([
        StructField("A", IntegerType(), True),
        StructField("B", StringType(),  True),
    ]), True),
])
data_mc = [
    (1, (1, "X")),
    (2, (2, "Y")),
    (3, (3, "Z")),
]
spark.createDataFrame(data_mc, schema_mc) \
     .write.mode("overwrite").parquet("{}/struct_db.db/mixed_case_struct".format(WH))
print("  mixed_case_struct written (3 rows)")

# ── TC-26/27/28: flex_rules_result_mini ──────────────────────────────────────
# Minimal replica of the production flex_rules_result table.
# Contains a realistic customerAttributes struct (10 fields) and a response
# struct with nested arrays — enough to exercise the full colon-to-space
# conversion path without needing all 100+ production fields.
# The full production table (TC-28) is handled separately below.
schema_mini = StructType([
    StructField("mcr_id",    StringType(), True),
    StructField("ecn",       StringType(), True),
    StructField("actionCode",StringType(), True),
    StructField("runDate",   StringType(), True),
    StructField("customerAttributes", StructType([
        StructField("CAR_CAT_CD",           StringType(), True),
        StructField("CDB_ACCT_TYP_CD",      StringType(), True),
        StructField("ACCT_PROD_OWN_CD",     StringType(), True),
        StructField("FICO_SCORE_NUM",        StringType(), True),
        StructField("CUST_HAS_FLEX_LN_ACCT_IND", StringType(), True),
        StructField("NOT_ENRLL_IN_OLB_IND", StringType(), True),
        StructField("AVG_DDA_BAL_CYC_1_AMT",StringType(), True),
        StructField("SOFT_CR_DT",           StringType(), True),
        StructField("TIN_TYPE_CD",          StringType(), True),
        StructField("ECN_BRTH_DT",          StringType(), True),
    ]), True),
    StructField("response", StructType([
        StructField("eligibilities", ArrayType(StructType([
            StructField("category",      StringType(), True),
            StructField("result",        StringType(), True),
            StructField("status",        StringType(), True),
            StructField("effectiveDate", StringType(), True),
            StructField("derivedElements", ArrayType(StructType([
                StructField("elementName",  StringType(), True),
                StructField("elementType",  StringType(), True),
                StructField("elementValue", StringType(), True),
            ])), True),
            StructField("attributes", ArrayType(StructType([
                StructField("type",  StringType(), True),
                StructField("value", StringType(), True),
            ])), True),
        ])), True),
        StructField("decisions", ArrayType(StructType([
            StructField("category", StringType(), True),
            StructField("status",   StringType(), True),
            StructField("rules",    ArrayType(StructType([
                StructField("ruleId", StringType(), True),
                StructField("status", StringType(), True),
            ])), True),
        ])), True),
    ]), True),
])

data_mini = [
    (
        "MCR001", "ECN001", "APPROVE", "2024-01-01",
        ("GOLD", "CHK", "PRM", "720", "Y", "N", "5000.00", "2024-01-01", "SSN", "1980-05-15"),
        (
            [("FLEX_LOAN", "ELIGIBLE", "ACTIVE", "2024-01-01",
              [("FICO", "SCORE", "720")],
              [("CHANNEL", "ONLINE")])],
            [("CREDIT", "APPROVED",
              [("RULE_001", "PASS"), ("RULE_002", "PASS")])]
        )
    ),
    (
        "MCR002", "ECN002", "DECLINE", "2024-01-02",
        ("SILVER", "SAV", "STD", "580", "N", "Y", "1200.00", "2024-01-02", "SSN", "1975-03-20"),
        (
            [("FLEX_LOAN", "INELIGIBLE", "INACTIVE", "2024-01-02",
              [("FICO", "SCORE", "580")],
              [("CHANNEL", "BRANCH")])],
            [("CREDIT", "DECLINED",
              [("RULE_001", "PASS"), ("RULE_003", "FAIL")])]
        )
    ),
    (
        "MCR003", "ECN003", "PENDING", "2024-01-03",
        ("BASIC", "CHK", "STD", "640", "N", "N", "800.00", "2024-01-03", "ITIN", "1990-11-30"),
        (
            [("FLEX_LOAN", "PENDING", "REVIEW", "2024-01-03",
              [("FICO", "SCORE", "640")],
              [("CHANNEL", "MOBILE")])],
            [("CREDIT", "PENDING",
              [("RULE_001", "PASS"), ("RULE_004", "WARN")])]
        )
    ),
]

spark.createDataFrame(data_mini, schema_mini) \
     .write.mode("overwrite").parquet("{}/struct_db.db/flex_rules_result_mini".format(WH))
print("  flex_rules_result_mini written (3 rows)")

# ── TC-28: flex_rules_result_full ─────────────────────────────────────────────
# Full production-width replica of the customerAttributes struct with all
# key fields from the actual DDL. Partitioned by capbusinesseffectivedate.
# This is the integration test table — creating its DDL in Hive must succeed
# after hive_type_to_spark_ddl is applied to the type string from DESCRIBE.
schema_full = StructType([
    StructField("mcr_id",     StringType(), True),
    StructField("ecn",        StringType(), True),
    StructField("actionCode", StringType(), True),
    StructField("runDate",    StringType(), True),
    StructField("customerAttributes", StructType([
        StructField("CAR_CAT_CD",                              StringType(), True),
        StructField("CDB_ACCT_TYP_CD",                         StringType(), True),
        StructField("ACCT_PROD_OWN_CD",                        StringType(), True),
        StructField("ACCT_OPN_CLSD_CD",                        StringType(), True),
        StructField("NOT_ENRLL_IN_OLB_IND",                    StringType(), True),
        StructField("ERLST_WF_DDA_ACCT_OPN_LT_12_MOS_IND",    StringType(), True),
        StructField("CUST_DDA_CDA_REA_RSTR_CD_22_IND",         StringType(), True),
        StructField("OLB_PROF_CHG_IN_THE_LST_45_DYS_IND",      StringType(), True),
        StructField("TOT_WF_BNK_DDA_CHRG_OFFS_WTHN_5_YRS_GT_250_DLR_IND", StringType(), True),
        StructField("WF_HEQ_CHRG_OFF_IND",                     StringType(), True),
        StructField("CUST_HAS_FLEX_LN_ACCT_IND",               StringType(), True),
        StructField("CUST_DOES_NOT_HAVE_A_CNSMR_LEND_RELT_IND",StringType(), True),
        StructField("AVG_DDA_BAL_CYC_1_AMT",                   StringType(), True),
        StructField("AVG_DDA_BAL_CYC_2_AMT",                   StringType(), True),
        StructField("AVG_DDA_BAL_CYC_3_AMT",                   StringType(), True),
        StructField("CR_INFLOW_INTO_DDA_PRD_1_AMT",            StringType(), True),
        StructField("CR_INFLOW_INTO_DDA_PRD_2_AMT",            StringType(), True),
        StructField("DDA_OD_CYC_1_QTY",                        StringType(), True),
        StructField("DDA_OD_CYC_2_QTY",                        StringType(), True),
        StructField("FICO_SCORE_NUM",                           StringType(), True),
        StructField("SOFT_CR_DT",                               StringType(), True),
        StructField("WF_PD_CHRG_OFF_WTHN_7_YRS_IND",           StringType(), True),
        StructField("BNKRPCY_2_TO_7_YRS_TOB_LT_13_MOS_FICO_SCORE_LT_640_IND", StringType(), True),
        StructField("WF_BNK_BNKRPCY_WTHN_LST_7_YRS_IND",       StringType(), True),
        StructField("REAL_ESTATE_SHRT_SL_IND",                  StringType(), True),
        StructField("INDIV_CRNT_DELQ_AT_THE_BUR_IND",           StringType(), True),
        StructField("FLEX_HLD_CD_IND",                          StringType(), True),
        StructField("BK_IN_PAST_2_YR_IND",                     StringType(), True),
        StructField("CUST_WTH_REPO_2YR_OR_REPO_PAST_2_7YR_WHEN_TOB_LT13_MO_AND_FICO_LT640_IND", StringType(), True),
        StructField("CUST_HAVING_BK_OR_NON_DISCH_BK_IND",      StringType(), True),
        StructField("CUST_DOB_PRE_DT_CUST_PROF_OPN_DT_IND",    StringType(), True),
        StructField("CUST_HAS_SNGL_DGT_OR_SPCL_CHAR_LST_NM_IND",StringType(), True),
        StructField("CUST_PERM_ADDR_NON_US_ADDR_IND",           StringType(), True),
        StructField("CUST_ADDR_IS_A_MLTRY_ADDR_IND",            StringType(), True),
        StructField("CUST_ADDR_IS_A_PO_BOX_IND",                StringType(), True),
        StructField("CUST_WTH_INV_ITIN_IND",                    StringType(), True),
        StructField("CUST_WTH_INV_SSN_IND",                     StringType(), True),
        StructField("EXPN_CUST_OPT_IN_IND",                     StringType(), True),
        StructField("MO_SNC_ERLST_DDA_ACCT_OPN_CNT",            StringType(), True),
        StructField("TIN_TYPE_CD",                              StringType(), True),
        StructField("ECN_BRTH_DT",                              StringType(), True),
        StructField("ECN_RSDNC_ST_CD",                          StringType(), True),
        StructField("ST_ANL_INCM_AMT",                          StringType(), True),
        StructField("VRFY_ANL_INCM_AMT",                        StringType(), True),
    ]), True),
    StructField("response", StructType([
        StructField("eligibilities", ArrayType(StructType([
            StructField("category",      StringType(), True),
            StructField("result",        StringType(), True),
            StructField("status",        StringType(), True),
            StructField("effectiveDate", StringType(), True),
            StructField("expirationDate",StringType(), True),
            StructField("derivedElements", ArrayType(StructType([
                StructField("elementName",  StringType(), True),
                StructField("elementType",  StringType(), True),
                StructField("elementValue", StringType(), True),
            ])), True),
            StructField("attributes", ArrayType(StructType([
                StructField("type",  StringType(), True),
                StructField("value", StringType(), True),
            ])), True),
        ])), True),
        StructField("decisions", ArrayType(StructType([
            StructField("category", StringType(), True),
            StructField("status",   StringType(), True),
            StructField("rules",    ArrayType(StructType([
                StructField("ruleId", StringType(), True),
                StructField("status", StringType(), True),
            ])), True),
        ])), True),
    ]), True),
    StructField("capbusinesseffectivedate", StringType(), True),
])

def make_full_row(mcr, ecn, action, dt, eff):
    # 44 values — one per customerAttributes field in schema_full order:
    # [0-3]   CAR_CAT_CD, CDB_ACCT_TYP_CD, ACCT_PROD_OWN_CD, ACCT_OPN_CLSD_CD
    # [4-11]  8 x IND flags (NOT_ENRLL .. CUST_DOES_NOT_HAVE)
    # [12-16] 5 x AMT fields (AVG_DDA x3, CR_INFLOW x2)
    # [17-19] DDA_OD_CYC_1, DDA_OD_CYC_2, FICO_SCORE_NUM
    # [20-21] SOFT_CR_DT, WF_PD_CHRG_OFF
    # [22-28] 7 x IND flags (BNKRPCY .. BK_IN_PAST)
    # [29-37] 9 x IND flags (CUST_WTH_REPO .. EXPN_CUST_OPT_IN)
    # [38-43] MO_SNC, TIN_TYPE_CD, ECN_BRTH_DT, ECN_RSDNC_ST_CD, ST_ANL_INCM, VRFY_ANL_INCM
    attrs = (
        "GOLD","CHK","PRM","OPEN",                          # [0-3]
        "N","N","N","N","N","N","N","N",                    # [4-11]  8 flags
        "5000","4800","5100","2000","1900",                  # [12-16] AMTs
        "0","1","720",                                       # [17-19]
        "2024-01-01","N",                                   # [20-21]
        "N","N","N","N","N","N","N",                        # [22-28] 7 flags
        "N","N","N","N","N","N","N","N","N",                # [29-37] 9 flags
        "12","SSN","1980-05-15","CA","60000","58000"        # [38-43]
    )
    elig = [("FLEX_LOAN","ELIGIBLE","ACTIVE",dt,dt,
             [("FICO","SCORE","720")],[("CHANNEL","ONLINE")])]
    decs = [("CREDIT","APPROVED",[("RULE_001","PASS"),("RULE_002","PASS")])]
    return (mcr, ecn, action, dt, attrs, (elig, decs), eff)

data_full = [
    make_full_row("MCR001","ECN001","APPROVE","2024-01-01","2024-01-01"),
    make_full_row("MCR002","ECN002","DECLINE","2024-01-02","2024-01-01"),
    make_full_row("MCR003","ECN003","APPROVE","2024-02-01","2024-02-01"),
    make_full_row("MCR004","ECN004","PENDING","2024-02-02","2024-02-01"),
]

(spark.createDataFrame(data_full, schema_full)
      .write.mode("overwrite")
      .partitionBy("capbusinesseffectivedate")
      .parquet("{}/struct_db.db/flex_rules_result_full".format(WH)))
print("  flex_rules_result_full written (4 rows, 2 partitions)")

spark.stop()
PYEOF
echo "  All struct_db Parquet data written."

echo ""
echo "============================================================"
echo " [4/11] Creating Hive databases and tables"
echo "============================================================"

cat > /tmp/setup-test-data.sql << 'SQL_EOF'
-- =============================================================================
-- sales_db
-- =============================================================================
CREATE DATABASE IF NOT EXISTS sales_db;

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

CREATE TABLE IF NOT EXISTS sales_db.orders_empty (
  order_id INT, customer STRING, amount DOUBLE
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders_empty';

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

CREATE TABLE IF NOT EXISTS hr_db.employees (
  emp_id INT, name STRING, dept STRING, salary DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
NULL DEFINED AS '\N'
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db.db/employees';

CREATE TABLE IF NOT EXISTS hr_db.departments (
  dept_id INT, dept_name STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db.db/departments';

CREATE TABLE IF NOT EXISTS hr_db.employees_parquet (
  emp_id INT, name STRING, dept STRING, salary DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db.db/employees_parquet';

CREATE TABLE IF NOT EXISTS hr_db.employees_avro (
  emp_id INT, name STRING, dept STRING, salary DOUBLE
)
STORED AS AVRO
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db.db/employees_avro';

-- TC-33: empty Parquet table with a known schema and zero data files.
-- Tests that the DAG correctly detects EMPTY_SOURCE for a Parquet-format table,
-- propagates the schema from the metastore rather than inferring it from files
-- (there are none), and skips data copy while still creating the destination table.
-- Schema is deliberately richer than employees_parquet to make the column list
-- distinguishable in logs and assertions.
CREATE TABLE IF NOT EXISTS hr_db.employees_parquet_empty (
  emp_id      INT,
  name        STRING,
  dept        STRING,
  job_title   STRING,
  salary      DOUBLE,
  hire_date   STRING,
  is_active   STRING,
  cost_centre STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db.db/employees_parquet_empty';

-- =============================================================================
-- analytics_db
-- =============================================================================
CREATE DATABASE IF NOT EXISTS analytics_db;

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

CREATE TABLE IF NOT EXISTS analytics_db.sessions (
  session_id STRING, user_id STRING, duration_sec INT
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/analytics_db.db/sessions';
-- Intentionally: no ALTER TABLE (unregistered partitions test)

CREATE TABLE IF NOT EXISTS analytics_db.events_orc (
  event_id STRING, event_type STRING, user_region STRING
)
PARTITIONED BY (region STRING, year INT, month INT)
STORED AS ORC
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/analytics_db.db/events_orc';

ALTER TABLE analytics_db.events_orc ADD IF NOT EXISTS PARTITION (region='US', year=2024, month=1);
ALTER TABLE analytics_db.events_orc ADD IF NOT EXISTS PARTITION (region='EU', year=2024, month=1);

-- TC-11: 3-level partitioned table where partitions are registered in the
-- metastore but every partition directory contains no data files.
-- This is distinct from orders_empty / error_logs_empty (no partitions at all)
-- and from sessions (partitions exist on disk but are NOT registered).
-- The DAG must detect EMPTY_SOURCE despite partitions being present and
-- registered, then skip data copy while still creating the destination table
-- with the correct partition scheme.
CREATE TABLE IF NOT EXISTS analytics_db.events_empty_partitioned (
  event_id STRING, event_type STRING, user_region STRING
)
PARTITIONED BY (region STRING, year INT, month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/analytics_db.db/events_empty_partitioned';

ALTER TABLE analytics_db.events_empty_partitioned ADD IF NOT EXISTS PARTITION (region='US', year=2024, month=1);
ALTER TABLE analytics_db.events_empty_partitioned ADD IF NOT EXISTS PARTITION (region='EU', year=2024, month=1);
ALTER TABLE analytics_db.events_empty_partitioned ADD IF NOT EXISTS PARTITION (region='US', year=2024, month=2);

-- TC-34: PARQUET equivalent of events_empty_partitioned.
-- Same 3-level partition scheme and same registered-but-empty semantics as TC-11,
-- but STORED AS PARQUET. This isolates the EMPTY_SOURCE code path for Parquet
-- partitioned tables: no schema can be inferred from data files (there are none),
-- so the DAG must use the metastore schema, create the dest table correctly, and
-- skip the data copy step — identical logic to TC-33 but with active partitions.
CREATE TABLE IF NOT EXISTS analytics_db.events_parquet_empty_partitioned (
  event_id STRING, event_type STRING, user_region STRING
)
PARTITIONED BY (region STRING, year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/analytics_db.db/events_parquet_empty_partitioned';

ALTER TABLE analytics_db.events_parquet_empty_partitioned ADD IF NOT EXISTS PARTITION (region='US', year=2024, month=1);
ALTER TABLE analytics_db.events_parquet_empty_partitioned ADD IF NOT EXISTS PARTITION (region='EU', year=2024, month=1);
ALTER TABLE analytics_db.events_parquet_empty_partitioned ADD IF NOT EXISTS PARTITION (region='US', year=2024, month=2);

-- =============================================================================
-- logs_db
-- =============================================================================
CREATE DATABASE IF NOT EXISTS logs_db;

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

CREATE TABLE IF NOT EXISTS logs_db.error_logs_empty (
  log_date STRING, level STRING, message STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/logs_db.db/error_logs_empty';

CREATE TABLE IF NOT EXISTS logs_db.metrics_empty_nonpartitioned (
  metric_id INT, name STRING, value DOUBLE, ts BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/logs_db.db/metrics_empty_nonpartitioned';

-- =============================================================================
-- struct_db  (NEW — hive_type_to_spark_ddl test cases)
-- =============================================================================
CREATE DATABASE IF NOT EXISTS struct_db;

-- TC-6/7: simple struct — DESCRIBE returns "struct<a:int,b:string>"
-- converter must produce "struct<a int,b string>"
CREATE TABLE IF NOT EXISTS struct_db.simple_struct (
  id INT,
  info struct<a:int,b:string>
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/struct_db.db/simple_struct';

-- TC-9: array of struct — array<struct<x:int,y:string>>
CREATE TABLE IF NOT EXISTS struct_db.array_of_struct (
  id INT,
  items array<struct<x:int,y:string>>
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/struct_db.db/array_of_struct';

-- TC-11: map of struct — map<string,struct<a:int>>
CREATE TABLE IF NOT EXISTS struct_db.map_of_struct (
  id INT,
  tags map<string,struct<a:int>>
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/struct_db.db/map_of_struct';

-- TC-12: deeply nested — struct<a:array<struct<b:map<string,int>>>>
CREATE TABLE IF NOT EXISTS struct_db.deep_nested_struct (
  id INT,
  payload struct<a:array<struct<b:map<string,int>>>>
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/struct_db.db/deep_nested_struct';

-- TC-13: array of array of struct — array<array<struct<x:int>>>
CREATE TABLE IF NOT EXISTS struct_db.array_of_array_struct (
  id INT,
  grid array<array<struct<x:int>>>
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/struct_db.db/array_of_array_struct';

-- TC-14: struct with decimal — decimal(18,4) comma must not split field list
CREATE TABLE IF NOT EXISTS struct_db.struct_with_decimal (
  id INT,
  price_info struct<amount:decimal(18,4),qty:int>
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/struct_db.db/struct_with_decimal';

-- TC-15: struct with varchar/char — parameterised types must pass through unchanged
CREATE TABLE IF NOT EXISTS struct_db.struct_with_varchar (
  id INT,
  labels struct<name:varchar(255),code:char(10)>
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/struct_db.db/struct_with_varchar';

-- TC-16: idempotency — uses colon notation (required by HiveQL).
-- Idempotency is verified at the DAG level: hive_type_to_spark_ddl applied to
-- a string that already has no word-flanked colons returns it unchanged.
-- The verification script in [5/11] applies the regex twice and confirms the
-- second pass produces identical output to the first.
CREATE TABLE IF NOT EXISTS struct_db.already_converted (
  id INT,
  info struct<a:int,b:string>
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/struct_db.db/already_converted';

-- TC-20: mixed case — STRUCT<A:INT,B:STRING> uppercase keywords
CREATE TABLE IF NOT EXISTS struct_db.mixed_case_struct (
  id INT,
  info STRUCT<A:INT,B:STRING>
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/struct_db.db/mixed_case_struct';

-- TC-26/27: flex_rules_result_mini — realistic nested struct + array-of-struct
-- Single-line struct definition to avoid Hive parser whitespace issues
CREATE TABLE IF NOT EXISTS struct_db.flex_rules_result_mini (
  mcr_id STRING,
  ecn STRING,
  actionCode STRING,
  runDate STRING,
  customerAttributes struct<CAR_CAT_CD:string,CDB_ACCT_TYP_CD:string,ACCT_PROD_OWN_CD:string,FICO_SCORE_NUM:string,CUST_HAS_FLEX_LN_ACCT_IND:string,NOT_ENRLL_IN_OLB_IND:string,AVG_DDA_BAL_CYC_1_AMT:string,SOFT_CR_DT:string,TIN_TYPE_CD:string,ECN_BRTH_DT:string>,
  response struct<eligibilities:array<struct<category:string,result:string,status:string,effectiveDate:string,derivedElements:array<struct<elementName:string,elementType:string,elementValue:string>>,attributes:array<struct<type:string,value:string>>>>,decisions:array<struct<category:string,status:string,rules:array<struct<ruleId:string,status:string>>>>>
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/struct_db.db/flex_rules_result_mini';

-- TC-28/29: flex_rules_result_full — full production-width struct, partitioned
-- Single-line struct definition to avoid Hive parser whitespace issues
CREATE TABLE IF NOT EXISTS struct_db.flex_rules_result_full (
  mcr_id STRING,
  ecn STRING,
  actionCode STRING,
  runDate STRING,
  customerAttributes struct<CAR_CAT_CD:string,CDB_ACCT_TYP_CD:string,ACCT_PROD_OWN_CD:string,ACCT_OPN_CLSD_CD:string,NOT_ENRLL_IN_OLB_IND:string,ERLST_WF_DDA_ACCT_OPN_LT_12_MOS_IND:string,CUST_DDA_CDA_REA_RSTR_CD_22_IND:string,OLB_PROF_CHG_IN_THE_LST_45_DYS_IND:string,TOT_WF_BNK_DDA_CHRG_OFFS_WTHN_5_YRS_GT_250_DLR_IND:string,WF_HEQ_CHRG_OFF_IND:string,CUST_HAS_FLEX_LN_ACCT_IND:string,CUST_DOES_NOT_HAVE_A_CNSMR_LEND_RELT_IND:string,AVG_DDA_BAL_CYC_1_AMT:string,AVG_DDA_BAL_CYC_2_AMT:string,AVG_DDA_BAL_CYC_3_AMT:string,CR_INFLOW_INTO_DDA_PRD_1_AMT:string,CR_INFLOW_INTO_DDA_PRD_2_AMT:string,DDA_OD_CYC_1_QTY:string,DDA_OD_CYC_2_QTY:string,FICO_SCORE_NUM:string,SOFT_CR_DT:string,WF_PD_CHRG_OFF_WTHN_7_YRS_IND:string,BNKRPCY_2_TO_7_YRS_TOB_LT_13_MOS_FICO_SCORE_LT_640_IND:string,WF_BNK_BNKRPCY_WTHN_LST_7_YRS_IND:string,REAL_ESTATE_SHRT_SL_IND:string,INDIV_CRNT_DELQ_AT_THE_BUR_IND:string,FLEX_HLD_CD_IND:string,BK_IN_PAST_2_YR_IND:string,CUST_WTH_REPO_2YR_OR_REPO_PAST_2_7YR_WHEN_TOB_LT13_MO_AND_FICO_LT640_IND:string,CUST_HAVING_BK_OR_NON_DISCH_BK_IND:string,CUST_DOB_PRE_DT_CUST_PROF_OPN_DT_IND:string,CUST_HAS_SNGL_DGT_OR_SPCL_CHAR_LST_NM_IND:string,CUST_PERM_ADDR_NON_US_ADDR_IND:string,CUST_ADDR_IS_A_MLTRY_ADDR_IND:string,CUST_ADDR_IS_A_PO_BOX_IND:string,CUST_WTH_INV_ITIN_IND:string,CUST_WTH_INV_SSN_IND:string,EXPN_CUST_OPT_IN_IND:string,MO_SNC_ERLST_DDA_ACCT_OPN_CNT:string,TIN_TYPE_CD:string,ECN_BRTH_DT:string,ECN_RSDNC_ST_CD:string,ST_ANL_INCM_AMT:string,VRFY_ANL_INCM_AMT:string>,
  response struct<eligibilities:array<struct<category:string,result:string,status:string,effectiveDate:string,expirationDate:string,derivedElements:array<struct<elementName:string,elementType:string,elementValue:string>>,attributes:array<struct<type:string,value:string>>>>,decisions:array<struct<category:string,status:string,rules:array<struct<ruleId:string,status:string>>>>>
)
PARTITIONED BY (capbusinesseffectivedate STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/struct_db.db/flex_rules_result_full';

ALTER TABLE struct_db.flex_rules_result_full
  ADD IF NOT EXISTS PARTITION (capbusinesseffectivedate='2024-01-01');
ALTER TABLE struct_db.flex_rules_result_full
  ADD IF NOT EXISTS PARTITION (capbusinesseffectivedate='2024-02-01');

SQL_EOF

$BEELINE -f /tmp/setup-test-data.sql
echo "  Hive DDL executed."

echo ""
echo "============================================================"
echo " [5/11] Verification"
echo "============================================================"

echo ""
echo "--- Hive databases ---"
$BEELINE -e "SHOW DATABASES;"

echo ""
echo "--- Row counts + partition registrations ---"
pyspark --master local[*] << 'PYEOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

checks = [
    # ── original DAG migration scenarios ─────────────────────────────────────
    ("sales_db.orders",                         None),
    ("sales_db.orders",                         "year=2024 AND month=1"),
    ("sales_db.orders_empty",                   None),
    ("hr_db.employees",                         None),
    ("hr_db.departments",                       None),
    ("hr_db.employees_parquet",                 None),
    ("hr_db.employees_avro",                    None),
    # TC-33: empty Parquet table — must report 0 rows and be in empty_tables set
    ("hr_db.employees_parquet_empty",           None),
    ("analytics_db.events",                     None),
    ("analytics_db.events",                     "region='US' AND year=2024"),
    ("analytics_db.sessions",                   None),
    ("analytics_db.events_orc",                 None),
    # TC-11: partitions registered, but all are empty — COUNT(*) must be 0
    ("analytics_db.events_empty_partitioned",   None),
    # TC-34: PARQUET partitioned table, same registered-but-empty semantics as TC-11
    ("analytics_db.events_parquet_empty_partitioned", None),
    ("logs_db.app_logs",                        None),
    ("logs_db.app_logs",                        "dt>='2024-01-15'"),
    ("logs_db.error_logs_empty",                None),
    ("logs_db.metrics_empty_nonpartitioned",    None),
    # ── struct_db test tables ─────────────────────────────────────────────────
    ("struct_db.simple_struct",                 None),   # TC-6/7
    ("struct_db.array_of_struct",               None),   # TC-9
    ("struct_db.map_of_struct",                 None),   # TC-11
    ("struct_db.deep_nested_struct",            None),   # TC-12
    ("struct_db.array_of_array_struct",         None),   # TC-13
    ("struct_db.struct_with_decimal",           None),   # TC-14
    ("struct_db.struct_with_varchar",           None),   # TC-15
    ("struct_db.already_converted",             None),   # TC-16
    ("struct_db.mixed_case_struct",             None),   # TC-20
    ("struct_db.flex_rules_result_mini",        None),   # TC-26/27
    ("struct_db.flex_rules_result_full",        None),   # TC-28
]

empty_tables = {
    "sales_db.orders_empty", "hr_db.departments",
    "hr_db.employees_parquet_empty",                  # TC-33
    # TC-11: partitions are registered but contain no data files — still EMPTY_SOURCE
    "analytics_db.events_empty_partitioned",
    # TC-34: PARQUET variant of TC-11 — registered partitions, zero data files
    "analytics_db.events_parquet_empty_partitioned",
    "logs_db.error_logs_empty", "logs_db.metrics_empty_nonpartitioned"
}

print("")
print("{:<55} {:>8}  {}".format("TABLE", "ROWS", "STATUS"))
print("-" * 72)
for tbl, where in checks:
    label = "{} WHERE {}".format(tbl, where) if where else tbl
    try:
        q = "SELECT COUNT(*) as c FROM {}".format(tbl)
        if where:
            q += " WHERE {}".format(where)
        cnt = spark.sql(q).collect()[0]["c"]
        expected_empty = tbl in empty_tables and where is None
        ok = (cnt == 0 and expected_empty) or (cnt > 0 and not expected_empty)
        status = "OK" if ok else "WARN"
        print("{:<55} {:>8}  {}".format(label[:55], cnt, status))
    except Exception as e:
        print("{:<55} {:>8}  ERROR: {}".format(label[:55], "?", str(e)[:60]))

# ── TC-33 extra: confirm schema is readable despite zero rows ─────────────────
print("")
print("--- TC-33 schema check: hr_db.employees_parquet_empty ---")
try:
    desc = spark.sql("DESCRIBE hr_db.employees_parquet_empty").collect()
    col_names = [r.col_name for r in desc if not r.col_name.startswith("#")]
    expected_cols = ["emp_id","name","dept","job_title","salary","hire_date","is_active","cost_centre"]
    missing = [c for c in expected_cols if c not in col_names]
    extra   = [c for c in col_names if c not in expected_cols]
    if not missing and not extra:
        print("  Columns match expected schema exactly: OK")
    else:
        if missing: print("  WARN: missing columns: {}".format(missing))
        if extra:   print("  WARN: unexpected columns: {}".format(extra))
    for r in desc:
        if not r.col_name.startswith("#"):
            print("    {:<20} {}".format(r.col_name, r.data_type))
except Exception as e:
    print("  ERROR: {}".format(str(e)))

# ── TC-11 extra: confirm partitions are registered despite zero rows ───────────
print("")
print("--- TC-11 partition check: analytics_db.events_empty_partitioned ---")
try:
    parts = spark.sql("SHOW PARTITIONS analytics_db.events_empty_partitioned").collect()
    expected_parts = {
        "region=EU/year=2024/month=1",
        "region=US/year=2024/month=1",
        "region=US/year=2024/month=2",
    }
    found_parts = {p[0] for p in parts}
    missing_parts = expected_parts - found_parts
    extra_parts   = found_parts - expected_parts
    if not missing_parts and not extra_parts:
        print("  Partitions match exactly (3 registered, 0 data rows): OK")
    else:
        if missing_parts: print("  WARN: missing partitions: {}".format(missing_parts))
        if extra_parts:   print("  WARN: unexpected partitions: {}".format(extra_parts))
    for p in parts:
        print("    {}".format(p[0]))
except Exception as e:
    print("  ERROR: {}".format(str(e)))

print("")
print("--- TC-34 partition check: analytics_db.events_parquet_empty_partitioned ---")
try:
    parts = spark.sql("SHOW PARTITIONS analytics_db.events_parquet_empty_partitioned").collect()
    expected_parts = {
        "region=EU/year=2024/month=1",
        "region=US/year=2024/month=1",
        "region=US/year=2024/month=2",
    }
    found_parts = {p[0] for p in parts}
    missing_parts = expected_parts - found_parts
    extra_parts   = found_parts - expected_parts
    if not missing_parts and not extra_parts:
        print("  Partitions match exactly (3 registered, 0 data rows): OK")
    else:
        if missing_parts: print("  WARN: missing partitions: {}".format(missing_parts))
        if extra_parts:   print("  WARN: unexpected partitions: {}".format(extra_parts))
    for p in parts:
        print("    {}".format(p[0]))
except Exception as e:
    print("  ERROR: {}".format(str(e)))

print("")
print("--- Partition registrations ---")
part_tables = [
    "sales_db.orders",
    "analytics_db.events",
    "analytics_db.sessions",
    "analytics_db.events_orc",
    "analytics_db.events_empty_partitioned",
    "analytics_db.events_parquet_empty_partitioned",
    "logs_db.app_logs",
    "struct_db.flex_rules_result_full",
]
for tbl in part_tables:
    try:
        parts = spark.sql("SHOW PARTITIONS {}".format(tbl)).collect()
        registered = len(parts)
        note = ""
        if tbl == "analytics_db.sessions" and registered == 0:
            note = "  <- CORRECT: unregistered partitions test"
        elif tbl == "analytics_db.sessions" and registered > 0:
            note = "  <- WARN: expected 0 registered partitions"
        elif tbl == "analytics_db.events_empty_partitioned" and registered == 3:
            note = "  <- CORRECT: 3 registered partitions, all empty (no data files)"
        elif tbl == "analytics_db.events_empty_partitioned" and registered != 3:
            note = "  <- WARN: expected exactly 3 registered partitions"
        elif tbl == "analytics_db.events_parquet_empty_partitioned" and registered == 3:
            note = "  <- CORRECT: 3 registered PARQUET partitions, all empty (no data files)"
        elif tbl == "analytics_db.events_parquet_empty_partitioned" and registered != 3:
            note = "  <- WARN: expected exactly 3 registered partitions"
        print("  {}: {} partition(s){}".format(tbl, registered, note))
        for p in parts:
            print("      {}".format(p[0]))
    except Exception as e:
        print("  {}: ERROR - {}".format(tbl, str(e)[:80]))

print("")
print("--- hive_type_to_spark_ddl smoke test via DESCRIBE ---")
# Confirms DESCRIBE returns colon-separated type strings (raw converter input)
# and that applying the regex removes all word-flanked colons in one pass.
# Also verifies idempotency: applying the regex a second time to the already-
# converted output produces the same string (no further changes).
struct_checks = [
    ("struct_db.simple_struct",          "info"),
    ("struct_db.array_of_struct",        "items"),
    ("struct_db.map_of_struct",          "tags"),
    ("struct_db.deep_nested_struct",     "payload"),
    ("struct_db.struct_with_decimal",    "price_info"),
    ("struct_db.already_converted",      "info"),
    ("struct_db.flex_rules_result_mini", "customerAttributes"),
    ("struct_db.flex_rules_result_mini", "response"),
    ("struct_db.flex_rules_result_full", "customerAttributes"),
    ("struct_db.flex_rules_result_full", "response"),
]
import re
regex = re.compile(r"(?<=\w):(?=\w)")
print("")
print("{:<52} {:<22} {:<12} {}".format("TABLE.COLUMN", "COLONS", "IDEMPOTENT", "STATUS"))
print("-" * 92)
for tbl, col in struct_checks:
    try:
        rows = spark.sql("DESCRIBE {}".format(tbl)).collect()
        type_str = ""
        for r in rows:
            if r.col_name.strip() == col:
                type_str = r.data_type.strip()
                break
        if not type_str:
            print("{:<52} {:<22} {:<12} WARN: column not found".format(
                "{}.{}".format(tbl, col), "-", "-"))
            continue
        converted_once  = regex.sub(" ", type_str)
        colons_before   = len(regex.findall(type_str))
        colons_after    = len(regex.findall(converted_once))
        converted_twice = regex.sub(" ", converted_once)
        idempotent = (converted_once == converted_twice)
        ok = colons_after == 0 and idempotent
        colon_label = "{} -> {}".format(colons_before, colons_after)
        print("{:<52} {:<22} {:<12} {}".format(
            "{}.{}".format(tbl, col)[:52],
            colon_label,
            "YES" if idempotent else "NO",
            "OK" if ok else "WARN"
        ))
    except Exception as e:
        print("{:<52} {:<22} {:<12} ERROR: {}".format(
            "{}.{}".format(tbl, col)[:52], "-", "-", str(e)[:50]))

spark.stop()
PYEOF

echo ""
echo "============================================================"
echo " [6/11] Test scenario reference"
echo "============================================================"
cat << 'SCENARIO_EOF'
DAG MIGRATION SCENARIOS (original)
 #  | Excel row                               | What is tested
----|------------------------------------------------|----------------------------
 1  | sales_db / * / sales_db_dest            | Wildcard: full DB migration
 2  | hr_db / employees / hr_db_dest          | Single explicit table, non-partitioned
 3  | hr_db / employees,departments / ...     | Comma-sep tables (one EMPTY_SOURCE)
 4  | sales_db / orders / ... / year=2024 AND month=1 | Partition filter active
 5  | sales_db / orders / ... / year=2099     | Partition filter → 0 partitions → SKIP
 6  | analytics_db / sessions / ...           | Unregistered partitions → MSCK REPAIR
 7  | analytics_db / events / ...             | 3-level partition key
 8  | analytics_db / events / ... / region='US' AND year=2024 | Filter on 3-level key
 9  | logs_db / error_logs_empty / ...        | EMPTY_SOURCE: dt-partitioned, 0 files
10  | logs_db / metrics_empty_nonpartitioned  | EMPTY_SOURCE: flat non-partitioned
11  | analytics_db / events_empty_partitioned | EMPTY_SOURCE: 3-level partitions registered, all empty (no data files)
11b | analytics_db / events_parquet_empty_partitioned | EMPTY_SOURCE: same as TC-11 but STORED AS PARQUET
12  | sales_db / orders_empty / ...           | EMPTY_SOURCE: year/month partition
13  | analytics_db / events_orc / ...         | ORC format
14  | hr_db / employees_parquet / ...         | PARQUET, schema inferred from files
15  | hr_db / employees_avro / ...            | AVRO format
16  | logs_db / app_logs / ...                | TEXTFILE + serde field.delim
17  | logs_db / app_logs / ... / dt>='2024-01-15' | String dt partition filter
18  | sales_db / orders* / ...                | Glob pattern
19  | hr_db / employees / hr_renamed / alt-bucket | Different dest_db + alt bucket
20  | sales_db / orders / ... / https://...   | Custom S3 endpoint
21  | logs_db / * / ... / dt='2024-01-01'     | Wildcard + partition_filter
22  | analytics_db / sessions,events / ... / year=2024 AND month=1 | CSV + filter
23  | sales_db / orders / ... (2nd run)       | Incremental re-run
24  | hr_db / employees / ...                 | \N NULL values survive round-trip
25  | sales_db / orders / (blank dest_db)     | Blank dest_db → defaults to source

HIVE_TYPE_TO_SPARK_DDL TEST CASES (new — struct_db)
 TC | Table                           | What is tested
----|----------------------------------------------|------------------------------
  6 | struct_db.simple_struct         | struct<a:int,b:string> → colon removal
  7 | struct_db.simple_struct         | struct<a:int,b:string> → all fields converted
  9 | struct_db.array_of_struct       | array<struct<x:int,y:string>>
 11 | struct_db.map_of_struct         | map<string,struct<a:int>>
 12 | struct_db.deep_nested_struct    | struct<a:array<struct<b:map<string,int>>>>
 13 | struct_db.array_of_array_struct | array<array<struct<x:int>>>
 14 | struct_db.struct_with_decimal   | decimal(18,4) comma does not split fields
 15 | struct_db.struct_with_varchar   | varchar(255)/char(10) pass through unchanged
 16 | struct_db.already_converted     | idempotent: already-space-separated unchanged
 20 | struct_db.mixed_case_struct     | STRUCT<A:INT> uppercase recognised
 26 | struct_db.flex_rules_result_mini| customerAttributes real type string parses
 27 | struct_db.flex_rules_result_mini| response nested array-of-struct parses
 28 | struct_db.flex_rules_result_full| full CREATE EXTERNAL TABLE succeeds; DESCRIBE matches
 29 | struct_db.flex_rules_result_full| struct partition column DDL valid (capbusinesseffectivedate)
 30 | struct_db.flex_rules_result_full| re-run after prior failure completes through create_hive_tables
 31 | all non-struct tables            | previously succeeding tables still succeed (no regression)
 32 | sales_db.orders / hr_db.employees | primitive-only tables DDL byte-identical to pre-fix output

EMPTY PARQUET SCHEMA TEST CASE
 TC | Table                              | What is tested
----|------------------------------------|-------------------------------------------------
 33 | hr_db.employees_parquet_empty      | EMPTY_SOURCE for Parquet format: zero data files,
    |                                    | schema readable from metastore (not file footer),
    |                                    | dest table created with correct 8-column DDL,
    |                                    | data copy step skipped cleanly
SCENARIO_EOF

echo ""
echo "Done. Test data is ready."


# #############################################################################
# #############################################################################
#
#   PART B -- formerly setup-test-data-tz.sh
#   (error-based-retry regression suite + transient/permanent retry cases
#    + TZ/TIMESTAMP tables; hr_db/sales_db/analytics_db/logs_db renamed
#    to hr_db_tz/sales_db_tz/analytics_db_tz/logs_db_tz to avoid collision
#    with PART A's databases of the same original names)
#
# #############################################################################
# #############################################################################

# NOTE: set -euo pipefail, PYSPARK_PYTHON/DRIVER_PYTHON, BEELINE, and WH were
# already set once at the top of this script (PART A's header) and apply here
# too -- no need to redeclare them for PART B.
export PYTHONIOENCODING=utf-8

# =============================================================================
# [7/11]  HDFS directories
# =============================================================================
echo "============================================================"
echo " [7/11] Creating HDFS directories"
echo "============================================================"

# regression tables
hdfs dfs -mkdir -p ${WH}/migration_db.db/customers
hdfs dfs -mkdir -p ${WH}/migration_db.db/products
hdfs dfs -mkdir -p ${WH}/migration_db.db/transactions
hdfs dfs -mkdir -p ${WH}/metrics_db.db/daily_active_users
hdfs dfs -mkdir -p ${WH}/metrics_db.db/conversion_rates
hdfs dfs -mkdir -p ${WH}/metrics_db.db/revenue_summary
hdfs dfs -mkdir -p ${WH}/audit_db.db/access_log
hdfs dfs -mkdir -p ${WH}/audit_db.db/change_log
hdfs dfs -mkdir -p ${WH}/hr_db_tz.db/employees
hdfs dfs -mkdir -p ${WH}/hr_db_tz.db/departments
hdfs dfs -mkdir -p ${WH}/sales_db_tz.db/orders/dt=2025-01-01
hdfs dfs -mkdir -p ${WH}/sales_db_tz.db/orders/dt=2025-01-15
hdfs dfs -mkdir -p ${WH}/sales_db_tz.db/orders/dt=2025-02-01
hdfs dfs -mkdir -p ${WH}/sales_db_tz.db/returns/dt=2024-12-15
hdfs dfs -mkdir -p ${WH}/sales_db_tz.db/returns/dt=2025-01-01
hdfs dfs -mkdir -p ${WH}/sales_db_tz.db/returns/dt=2025-02-01
hdfs dfs -mkdir -p ${WH}/sales_db_tz.db/daily_summary/year=2025/month=1
hdfs dfs -mkdir -p ${WH}/sales_db_tz.db/daily_summary/year=2025/month=2
hdfs dfs -mkdir -p ${WH}/sales_db_tz.db/daily_summary/year=2025/month=3
hdfs dfs -mkdir -p ${WH}/analytics_db_tz.db/events/region=US/year=2025/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db_tz.db/events/region=US/year=2025/month=2
hdfs dfs -mkdir -p ${WH}/analytics_db_tz.db/events/region=EU/year=2025/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db_tz.db/sessions/year=2025/month=1
hdfs dfs -mkdir -p ${WH}/analytics_db_tz.db/sessions/year=2025/month=2
hdfs dfs -mkdir -p ${WH}/logs_db_tz.db/app_logs/dt=2025-01-01
hdfs dfs -mkdir -p ${WH}/logs_db_tz.db/app_logs/dt=2025-01-15
hdfs dfs -mkdir -p ${WH}/logs_db_tz.db/app_logs/dt=2025-02-01
hdfs dfs -mkdir -p ${WH}/edge_cases_db.db/empty_table
hdfs dfs -mkdir -p ${WH}/edge_cases_db.db/nulls_table
hdfs dfs -mkdir -p ${WH}/edge_cases_db.db/complex_types
hdfs dfs -mkdir -p ${WH}/edge_cases_db.db/wide_table
hdfs dfs -mkdir -p ${WH}/formats_db.db/parquet_table
hdfs dfs -mkdir -p ${WH}/formats_db.db/orc_table
hdfs dfs -mkdir -p ${WH}/formats_db.db/text_table
hdfs dfs -mkdir -p ${WH}/tz_db.db/events_with_ts_la
hdfs dfs -mkdir -p ${WH}/tz_db.db/orders_with_ts_la/dt=2026-04-10
hdfs dfs -mkdir -p ${WH}/tz_db.db/orders_with_ts_la/dt=2026-04-11
hdfs dfs -mkdir -p ${WH}/tz_db.db/sessions_with_ts_la/year=2026/month=4
hdfs dfs -mkdir -p ${WH}/tz_db.db/sessions_with_ts_la/year=2026/month=5

# retry test table (T-01: YARN OOM simulation)
hdfs dfs -mkdir -p ${WH}/retry_test_db.db/yarn_oom_table

# permanent error test tables (P-03/P-04: corrupt Parquet scenarios)
hdfs dfs -mkdir -p ${WH}/corrupt_test_db.db/table_a
hdfs dfs -mkdir -p ${WH}/corrupt_test_db.db/corrupt_table
hdfs dfs -mkdir -p ${WH}/corrupt_test_db.db/table_c

echo "  HDFS directories created."


# =============================================================================
# [8/11]  Hive DDL
#        Every table uses DROP TABLE IF EXISTS + CREATE TABLE (no IF NOT EXISTS)
#        so stale definitions from a prior run never cause errors.
#        Partitioned tables use MSCK REPAIR TABLE instead of ALTER TABLE ADD
#        PARTITION -- MSCK auto-discovers key=value subdirs already written.
# =============================================================================
echo ""
echo "============================================================"
echo " [8/11] Hive DDL"
echo "============================================================"

cat > /tmp/setup-ebr.sql << 'SQL_EOF'

-- =============================================================================
-- All tables: DROP IF EXISTS first so stale schemas never block re-creation.
-- Partitions: MSCK REPAIR TABLE auto-discovers HDFS key=value directories.
-- =============================================================================

-- migration_db ----------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS migration_db;

DROP TABLE IF EXISTS migration_db.customers;
CREATE TABLE migration_db.customers (
    customer_id INT, name STRING, email STRING,
    country STRING, credit_score DOUBLE, is_active BOOLEAN
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/migration_db.db/customers';

DROP TABLE IF EXISTS migration_db.products;
CREATE TABLE migration_db.products (
    product_id INT, name STRING, category STRING,
    price DOUBLE, stock_qty INT
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/migration_db.db/products';

DROP TABLE IF EXISTS migration_db.transactions;
CREATE TABLE migration_db.transactions (
    txn_id INT, customer_id INT, product_id INT,
    amount DOUBLE, status STRING, created_at TIMESTAMP
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/migration_db.db/transactions';

-- metrics_db ------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS metrics_db;

DROP TABLE IF EXISTS metrics_db.daily_active_users;
CREATE TABLE metrics_db.daily_active_users (metric_date STRING, value DOUBLE, region STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/hive/warehouse/metrics_db.db/daily_active_users';

DROP TABLE IF EXISTS metrics_db.conversion_rates;
CREATE TABLE metrics_db.conversion_rates (metric_date STRING, value DOUBLE, region STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/hive/warehouse/metrics_db.db/conversion_rates';

DROP TABLE IF EXISTS metrics_db.revenue_summary;
CREATE TABLE metrics_db.revenue_summary (metric_date STRING, value DOUBLE, region STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/hive/warehouse/metrics_db.db/revenue_summary';

-- audit_db --------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS audit_db;

DROP TABLE IF EXISTS audit_db.access_log;
CREATE TABLE audit_db.access_log (
    event_time STRING, user_id STRING, action STRING, ip_address STRING, result STRING
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/audit_db.db/access_log';

DROP TABLE IF EXISTS audit_db.change_log;
CREATE TABLE audit_db.change_log (
    change_time STRING, changed_by STRING, table_name STRING,
    operation STRING, record_key STRING, column_changed STRING
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/audit_db.db/change_log';

-- hr_db_tz -----------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS hr_db_tz;

DROP TABLE IF EXISTS hr_db_tz.employees;
CREATE TABLE hr_db_tz.employees (
    emp_id INT, name STRING, department STRING, salary DOUBLE, hire_date TIMESTAMP
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db_tz.db/employees';

DROP TABLE IF EXISTS hr_db_tz.departments;
CREATE TABLE hr_db_tz.departments (
    dept_id INT, name STRING, head STRING, headcount INT
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db_tz.db/departments';

-- sales_db_tz (partitioned) ------------------------------------------------------
CREATE DATABASE IF NOT EXISTS sales_db_tz;

DROP TABLE IF EXISTS sales_db_tz.orders;
CREATE TABLE sales_db_tz.orders (
    order_id INT, customer_id INT, product_id INT, qty INT, total_price DOUBLE
) PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/sales_db_tz.db/orders';
MSCK REPAIR TABLE sales_db_tz.orders;

DROP TABLE IF EXISTS sales_db_tz.returns;
CREATE TABLE sales_db_tz.returns (
    return_id INT, txn_id INT, reason STRING, refund_amt DOUBLE
) PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/sales_db_tz.db/returns';
MSCK REPAIR TABLE sales_db_tz.returns;

DROP TABLE IF EXISTS sales_db_tz.daily_summary;
CREATE TABLE sales_db_tz.daily_summary (
    summary_date STRING, total_orders INT, total_revenue DOUBLE, avg_order_value DOUBLE
) PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/sales_db_tz.db/daily_summary';
MSCK REPAIR TABLE sales_db_tz.daily_summary;

-- analytics_db_tz (partitioned) --------------------------------------------------
CREATE DATABASE IF NOT EXISTS analytics_db_tz;

DROP TABLE IF EXISTS analytics_db_tz.events;
CREATE TABLE analytics_db_tz.events (
    event_id STRING, user_id STRING, event_type STRING, properties STRING
) PARTITIONED BY (region STRING, year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/analytics_db_tz.db/events';
MSCK REPAIR TABLE analytics_db_tz.events;

DROP TABLE IF EXISTS analytics_db_tz.sessions;
CREATE TABLE analytics_db_tz.sessions (
    session_id STRING, user_id STRING, start_ts TIMESTAMP,
    end_ts TIMESTAMP, duration_sec INT, page_views INT
) PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/analytics_db_tz.db/sessions';
MSCK REPAIR TABLE analytics_db_tz.sessions;

-- logs_db_tz (partitioned) -------------------------------------------------------
CREATE DATABASE IF NOT EXISTS logs_db_tz;

DROP TABLE IF EXISTS logs_db_tz.app_logs;
CREATE TABLE logs_db_tz.app_logs (
    log_time STRING, level STRING, service STRING, message STRING, trace_id STRING
) PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/logs_db_tz.db/app_logs';
MSCK REPAIR TABLE logs_db_tz.app_logs;

-- edge_cases_db ---------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS edge_cases_db;

DROP TABLE IF EXISTS edge_cases_db.empty_table;
CREATE TABLE edge_cases_db.empty_table (id INT, label STRING, value DOUBLE)
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/hive/warehouse/edge_cases_db.db/empty_table';

DROP TABLE IF EXISTS edge_cases_db.nulls_table;
CREATE TABLE edge_cases_db.nulls_table (
    id INT, str_col STRING, int_col INT, double_col DOUBLE, bool_col BOOLEAN, ts_col TIMESTAMP
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/edge_cases_db.db/nulls_table';

DROP TABLE IF EXISTS edge_cases_db.complex_types;
CREATE TABLE edge_cases_db.complex_types (
    id INT, tags ARRAY<STRING>, scores MAP<STRING,DOUBLE>,
    address STRUCT<street:STRING,city:STRING,zip:STRING>,
    metrics ARRAY<STRUCT<name:STRING,value:DOUBLE>>
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/edge_cases_db.db/complex_types';

-- edge_cases_db.wide_table created separately in [7b/11] (50-col SQL too long)

-- formats_db ------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS formats_db;

DROP TABLE IF EXISTS formats_db.parquet_table;
CREATE TABLE formats_db.parquet_table (id INT, name STRING, amount DOUBLE)
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/hive/warehouse/formats_db.db/parquet_table';

DROP TABLE IF EXISTS formats_db.orc_table;
CREATE TABLE formats_db.orc_table (id INT, name STRING, amount DOUBLE)
    STORED AS ORC
    LOCATION 'hdfs://localhost:9000/user/hive/warehouse/formats_db.db/orc_table';

DROP TABLE IF EXISTS formats_db.text_table;
CREATE TABLE formats_db.text_table (id INT, name STRING, amount DOUBLE)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://localhost:9000/user/hive/warehouse/formats_db.db/text_table';

-- tz_db (partitioned) ---------------------------------------------------------
CREATE DATABASE IF NOT EXISTS tz_db;

DROP TABLE IF EXISTS tz_db.events_with_ts_la;
CREATE TABLE tz_db.events_with_ts_la (
    event_id STRING, ln_nbr_10 STRING,
    application_dt TIMESTAMP, ln_registration_dt TIMESTAMP,
    bus_date STRING, event_type STRING
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/tz_db.db/events_with_ts_la';

DROP TABLE IF EXISTS tz_db.orders_with_ts_la;
CREATE TABLE tz_db.orders_with_ts_la (
    order_id STRING, ln_nbr_10 STRING,
    application_dt TIMESTAMP, ln_registration_dt TIMESTAMP,
    status STRING, amount DOUBLE
) PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/tz_db.db/orders_with_ts_la';
MSCK REPAIR TABLE tz_db.orders_with_ts_la;

DROP TABLE IF EXISTS tz_db.sessions_with_ts_la;
CREATE TABLE tz_db.sessions_with_ts_la (
    session_id STRING, user_id STRING,
    start_ts TIMESTAMP, end_ts TIMESTAMP, duration_sec INT
) PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/tz_db.db/sessions_with_ts_la';
MSCK REPAIR TABLE tz_db.sessions_with_ts_la;

-- retry_test_db (T-01) --------------------------------------------------------
CREATE DATABASE IF NOT EXISTS retry_test_db;

DROP TABLE IF EXISTS retry_test_db.yarn_oom_table;
CREATE TABLE retry_test_db.yarn_oom_table (
    id INT, payload STRING, value DOUBLE
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/retry_test_db.db/yarn_oom_table';

-- corrupt_test_db (P-03 / P-04 multi-table batch test) -----------------------
-- IMPORTANT: After DAG1 copies these to S3, corrupt_table's Parquet files
-- must be manually corrupted (see [11/11] instructions).
CREATE DATABASE IF NOT EXISTS corrupt_test_db;

DROP TABLE IF EXISTS corrupt_test_db.table_a;
CREATE TABLE corrupt_test_db.table_a (
    id INT, name STRING, amount DOUBLE
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/corrupt_test_db.db/table_a';

DROP TABLE IF EXISTS corrupt_test_db.corrupt_table;
CREATE TABLE corrupt_test_db.corrupt_table (
    id INT, name STRING, amount DOUBLE
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/corrupt_test_db.db/corrupt_table';

DROP TABLE IF EXISTS corrupt_test_db.table_c;
CREATE TABLE corrupt_test_db.table_c (
    id INT, name STRING, amount DOUBLE
) STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/corrupt_test_db.db/table_c';

SQL_EOF

$BEELINE -f /tmp/setup-ebr.sql
echo "  Hive DDL executed."


# =============================================================================
# [7b/11]  wide_table DDL (50 columns, Python to avoid SQL length limit)
#         Python 3.6 compatible: no capture_output, no text= kwarg
# =============================================================================
echo ""
echo "============================================================"
echo " [7b/11] wide_table DDL (50 columns)"
echo "============================================================"

python3 - << 'PYEOF'
import subprocess

cols = "id INT"
for i in range(1, 50):
    cols += ",\n    col_{:02d} STRING".format(i)

sql = (
    "DROP TABLE IF EXISTS edge_cases_db.wide_table;\n"
    "CREATE TABLE edge_cases_db.wide_table (\n"
    "    {}\n"
    ")\n"
    "STORED AS PARQUET\n"
    "LOCATION 'hdfs://localhost:9000/user/hive/warehouse/edge_cases_db.db/wide_table';\n"
).format(cols)

with open("/tmp/wide_table.sql", "w") as f:
    f.write(sql)

r = subprocess.run(
    ["/opt/hive/bin/beeline", "-u", "jdbc:hive2://localhost:10000",
     "--silent=true", "-f", "/tmp/wide_table.sql"],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE
)
err = r.stderr.decode("utf-8", errors="replace")
if r.returncode != 0:
    print("  wide_table DDL: ERROR --", err[:300])
else:
    print("  wide_table DDL: OK (50 columns)")
PYEOF


# =============================================================================
# [7c/11]  TEXTFILE data for formats_db.text_table
# =============================================================================
echo ""
echo "============================================================"
echo " [7c/11] TEXTFILE data for formats_db.text_table"
echo "============================================================"

printf '1,Alpha,100.0\n2,Beta,200.0\n3,Gamma,\n4,Delta,400.0\n5,Epsilon,500.0\n' \
  | hdfs dfs -put -f - ${WH}/formats_db.db/text_table/data.csv
echo "  formats_db.text_table: 5 CSV rows written"


# =============================================================================
# [9/11]  PySpark data write
#        Runs AFTER Hive DDL -- write.mode("overwrite") replaces file content
#        without touching the metastore registration.
# =============================================================================
echo ""
echo "============================================================"
echo " [9/11] Writing Parquet / ORC data via PySpark"
echo "============================================================"

pyspark --master local[*] << 'PYEOF'
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, TimestampType,
    ArrayType, MapType,
)
from datetime import datetime

spark = SparkSession.builder \
    .appName("setup_test_data_ebr") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
WH = "hdfs://localhost:9000/user/hive/warehouse"

def w(df, path, fmt="parquet"):
    df.write.mode("overwrite").format(fmt).save(path)

# -- R-01  migration_db.customers (10 rows) ------------------------------------
w(spark.createDataFrame([
    (1,"Alice Chen","alice@example.com","US",720.5,True),
    (2,"Bob Smith","bob@example.com","UK",680.0,True),
    (3,"Carol White","carol@example.com","US",750.0,False),
    (4,"David Kim","david@example.com","KR",690.5,True),
    (5,"Eva Muller","eva@example.com","DE",810.0,True),
    (6,"Frank Lopez","frank@example.com","MX",630.0,False),
    (7,"Grace Tanaka","grace@example.com","JP",760.0,True),
    (8,"Henry Brown","henry@example.com","US",700.5,True),
    (9,"Isabella Rossi","iso@example.com","IT",775.0,True),
    (10,"James Park","james@example.com","KR",645.0,False),
], StructType([
    StructField("customer_id",IntegerType(),False),
    StructField("name",StringType(),False),
    StructField("email",StringType(),True),
    StructField("country",StringType(),True),
    StructField("credit_score",DoubleType(),True),
    StructField("is_active",BooleanType(),True),
])), "{}/migration_db.db/customers".format(WH))
print("  R-01: migration_db.customers (10 rows)")

# -- R-02  migration_db.products (10 rows, no NULLs) ---------------------------
w(spark.createDataFrame([
    (101,"Laptop Pro","Electronics",1299.99,50),
    (102,"Wireless Mouse","Electronics",29.99,200),
    (103,"Standing Desk","Furniture",449.00,30),
    (104,"Coffee Maker","Appliances",89.50,150),
    (105,"Notebook A5","Stationery",4.99,500),
    (106,"USB-C Hub","Electronics",49.99,180),
    (107,"Ergonomic Chair","Furniture",599.00,20),
    (108,"Water Bottle","Accessories",19.99,300),
    (109,"Monitor 27in","Electronics",399.00,45),
    (110,"Mechanical Keyboard","Electronics",149.00,75),
], StructType([
    StructField("product_id",IntegerType(),False),
    StructField("name",StringType(),False),
    StructField("category",StringType(),False),
    StructField("price",DoubleType(),False),
    StructField("stock_qty",IntegerType(),False),
])), "{}/migration_db.db/products".format(WH))
print("  R-02: migration_db.products (10 rows)")

# -- R-03  migration_db.transactions (15 rows, TIMESTAMP) ----------------------
w(spark.createDataFrame([
    (1001,1,101,1299.99,"COMPLETED",datetime(2025,1,5,10,30,0)),
    (1002,2,102,29.99,  "COMPLETED",datetime(2025,1,6,11,15,0)),
    (1003,3,103,449.00, "PENDING",  datetime(2025,1,7,9,0,0)),
    (1004,1,104,89.50,  "COMPLETED",datetime(2025,1,8,14,45,0)),
    (1005,4,105,4.99,   "COMPLETED",datetime(2025,1,9,8,20,0)),
    (1006,5,106,49.99,  "REFUNDED", datetime(2025,1,10,16,0,0)),
    (1007,2,107,599.00, "COMPLETED",datetime(2025,1,11,12,30,0)),
    (1008,6,108,19.99,  "COMPLETED",datetime(2025,1,12,7,45,0)),
    (1009,7,109,399.00, "PENDING",  datetime(2025,1,13,15,10,0)),
    (1010,8,110,149.00, "COMPLETED",datetime(2025,1,14,13,20,0)),
    (1011,9,101,1299.99,"COMPLETED",datetime(2025,1,15,10,0,0)),
    (1012,10,102,29.99, "FAILED",   datetime(2025,1,16,9,30,0)),
    (1013,1,103,449.00, "COMPLETED",datetime(2025,1,17,11,0,0)),
    (1014,3,104,89.50,  "COMPLETED",datetime(2025,1,18,14,0,0)),
    (1015,5,105,4.99,   "COMPLETED",datetime(2025,1,19,8,0,0)),
], StructType([
    StructField("txn_id",IntegerType(),False),
    StructField("customer_id",IntegerType(),False),
    StructField("product_id",IntegerType(),False),
    StructField("amount",DoubleType(),False),
    StructField("status",StringType(),False),
    StructField("created_at",TimestampType(),True),
])), "{}/migration_db.db/transactions".format(WH))
print("  R-03: migration_db.transactions (15 rows)")

# -- R-04  metrics_db.* (4 rows each, wildcard) --------------------------------
ms = StructType([
    StructField("metric_date",StringType(),False),
    StructField("value",DoubleType(),False),
    StructField("region",StringType(),True),
])
for tbl, data in [
    ("daily_active_users",[("2025-01-01",12450.0,"US"),("2025-01-02",13200.0,"US"),("2025-01-01",8900.0,"EU"),("2025-01-02",9100.0,"EU")]),
    ("conversion_rates",  [("2025-01-01",0.034,"US"),("2025-01-02",0.037,"US"),("2025-01-01",0.029,"EU"),("2025-01-02",0.031,"EU")]),
    ("revenue_summary",   [("2025-01-01",48500.0,"US"),("2025-01-02",52100.0,"US"),("2025-01-01",29000.0,"EU"),("2025-01-02",31500.0,"EU")]),
]:
    w(spark.createDataFrame(data, ms), "{}/metrics_db.db/{}".format(WH, tbl))
    print("  R-04: metrics_db.{} (4 rows)".format(tbl))

# -- R-05  audit_db (comma-list) -----------------------------------------------
w(spark.createDataFrame([
    ("2025-01-10 08:00:00","user_001","LOGIN","192.168.1.1","SUCCESS"),
    ("2025-01-10 08:05:00","user_002","VIEW","192.168.1.2","SUCCESS"),
    ("2025-01-10 08:10:00","user_001","EDIT","192.168.1.1","SUCCESS"),
    ("2025-01-10 08:15:00","user_003","LOGIN","10.0.0.5","FAILED"),
    ("2025-01-10 08:20:00","user_002","LOGOUT","192.168.1.2","SUCCESS"),
], StructType([
    StructField("event_time",StringType(),False),StructField("user_id",StringType(),False),
    StructField("action",StringType(),False),StructField("ip_address",StringType(),True),
    StructField("result",StringType(),False),
])), "{}/audit_db.db/access_log".format(WH))

w(spark.createDataFrame([
    ("2025-01-10 09:00:00","user_001","customers","UPDATE","id=1","email"),
    ("2025-01-10 09:30:00","user_002","products","INSERT","id=111",None),
    ("2025-01-10 10:00:00","user_001","products","DELETE","id=99",None),
    ("2025-01-10 10:30:00","user_003","transactions","UPDATE","id=500","status"),
], StructType([
    StructField("change_time",StringType(),False),StructField("changed_by",StringType(),False),
    StructField("table_name",StringType(),False),StructField("operation",StringType(),False),
    StructField("record_key",StringType(),False),StructField("column_changed",StringType(),True),
])), "{}/audit_db.db/change_log".format(WH))
print("  R-05: audit_db.access_log (5), change_log (4)")

# -- R-06  hr_db_tz (dest_database override) --------------------------------------
w(spark.createDataFrame([
    (1,"Alice Chen","Engineering",95000.0,datetime(2020,3,15,9,0,0)),
    (2,"Bob Smith","Marketing",72000.0,datetime(2019,7,1,9,0,0)),
    (3,"Carol White","Engineering",105000.0,datetime(2021,1,10,9,0,0)),
    (4,"David Kim","HR",68000.0,datetime(2018,5,20,9,0,0)),
    (5,"Eva Muller","Finance",88000.0,datetime(2022,2,28,9,0,0)),
    (6,"Frank Lopez","Engineering",98000.0,datetime(2020,8,3,9,0,0)),
], StructType([
    StructField("emp_id",IntegerType(),False),StructField("name",StringType(),False),
    StructField("department",StringType(),True),StructField("salary",DoubleType(),True),
    StructField("hire_date",TimestampType(),True),
])), "{}/hr_db_tz.db/employees".format(WH))

w(spark.createDataFrame([
    (10,"Engineering","Alice Chen",25),
    (20,"Marketing","Bob Smith",8),
    (30,"HR","David Kim",5),
    (40,"Finance","Eva Muller",7),
], StructType([
    StructField("dept_id",IntegerType(),False),StructField("name",StringType(),False),
    StructField("head",StringType(),True),StructField("headcount",IntegerType(),True),
])), "{}/hr_db_tz.db/departments".format(WH))
print("  R-06: hr_db_tz.employees (6), departments (4)")

# -- R-07  sales_db_tz.orders (9 rows, 3 dt partitions) ---------------------------
os = StructType([
    StructField("order_id",IntegerType(),False),StructField("customer_id",IntegerType(),False),
    StructField("product_id",IntegerType(),False),StructField("qty",IntegerType(),False),
    StructField("total_price",DoubleType(),False),
])
for part, rows in {
    "dt=2025-01-01":[(5001,1,101,2,2599.98),(5002,2,102,1,29.99),(5003,3,105,5,24.95)],
    "dt=2025-01-15":[(5004,4,103,1,449.00),(5005,5,106,2,99.98),(5006,1,104,1,89.50),(5007,6,109,1,399.00)],
    "dt=2025-02-01":[(5008,7,107,1,599.00),(5009,8,110,2,298.00)],
}.items():
    w(spark.createDataFrame(rows, os), "{}/sales_db_tz.db/orders/{}".format(WH, part))
print("  R-07: sales_db_tz.orders (9 rows, 3 dt parts)")

# -- R-08  sales_db_tz.returns (4 rows, 3 dt partitions) -------------------------
rs = StructType([
    StructField("return_id",IntegerType(),False),StructField("txn_id",IntegerType(),False),
    StructField("reason",StringType(),False),StructField("refund_amt",DoubleType(),False),
])
for part, rows in {
    "dt=2024-12-15":[(8001,1001,"DAMAGED",89.50)],
    "dt=2025-01-01":[(8002,1003,"WRONG_ITEM",449.00),(8003,1005,"NOT_NEEDED",4.99)],
    "dt=2025-02-01":[(8004,1009,"DAMAGED",399.00)],
}.items():
    w(spark.createDataFrame(rows, rs), "{}/sales_db_tz.db/returns/{}".format(WH, part))
print("  R-08: sales_db_tz.returns (4 rows, 3 dt parts)")

# -- R-09  sales_db_tz.daily_summary (6 rows, 3 year/month partitions) ------------
ds = StructType([
    StructField("summary_date",StringType(),False),StructField("total_orders",IntegerType(),False),
    StructField("total_revenue",DoubleType(),False),StructField("avg_order_value",DoubleType(),False),
])
for part, rows in {
    "year=2025/month=1":[("2025-01-01",18,4200.50,233.36),("2025-01-15",25,6800.00,272.00)],
    "year=2025/month=2":[("2025-02-01",22,5100.75,231.85),("2025-02-15",30,8200.00,273.33)],
    "year=2025/month=3":[("2025-03-01",35,9500.00,271.43),("2025-03-15",40,11200.50,280.01)],
}.items():
    w(spark.createDataFrame(rows, ds), "{}/sales_db_tz.db/daily_summary/{}".format(WH, part))
print("  R-09: sales_db_tz.daily_summary (6 rows, 3 year/month parts)")

# -- R-10  analytics_db_tz.events (9 rows, 3 region/year/month partitions) --------
es = StructType([
    StructField("event_id",StringType(),False),StructField("user_id",StringType(),False),
    StructField("event_type",StringType(),False),StructField("properties",StringType(),True),
])
for part, rows in {
    "region=US/year=2025/month=1":[
        ("EVT001","usr_101","PAGE_VIEW",'{"page":"/home"}'),
        ("EVT002","usr_102","CLICK",'{"btn":"signup"}'),
        ("EVT003","usr_101","PURCHASE",'{"item":101}'),
        ("EVT004","usr_103","PAGE_VIEW",'{"page":"/products"}'),
    ],
    "region=US/year=2025/month=2":[
        ("EVT005","usr_101","PAGE_VIEW",'{"page":"/checkout"}'),
        ("EVT006","usr_104","SIGNUP",'{"plan":"pro"}'),
    ],
    "region=EU/year=2025/month=1":[
        ("EVT007","usr_201","PAGE_VIEW",'{"page":"/home"}'),
        ("EVT008","usr_202","CLICK",'{"btn":"login"}'),
        ("EVT009","usr_201","PURCHASE",'{"item":105}'),
    ],
}.items():
    w(spark.createDataFrame(rows, es), "{}/analytics_db_tz.db/events/{}".format(WH, part))
print("  R-10: analytics_db_tz.events (9 rows, 3 region/year/month parts)")

# -- R-19  analytics_db_tz.sessions (6 rows, 2 year/month partitions) -------------
ss = StructType([
    StructField("session_id",StringType(),False),StructField("user_id",StringType(),False),
    StructField("start_ts",TimestampType(),True),StructField("end_ts",TimestampType(),True),
    StructField("duration_sec",IntegerType(),True),StructField("page_views",IntegerType(),True),
])
for part, rows in {
    "year=2025/month=1":[
        ("SES001","usr_101",datetime(2025,1,5,10,0,0),datetime(2025,1,5,10,25,0),1500,8),
        ("SES002","usr_102",datetime(2025,1,6,14,0,0),datetime(2025,1,6,14,40,0),2400,12),
        ("SES003","usr_201",datetime(2025,1,7,9,0,0),datetime(2025,1,7,9,10,0),600,3),
        ("SES004","usr_103",datetime(2025,1,8,16,0,0),datetime(2025,1,8,16,30,0),1800,10),
    ],
    "year=2025/month=2":[
        ("SES005","usr_101",datetime(2025,2,3,11,0,0),datetime(2025,2,3,11,45,0),2700,15),
        ("SES006","usr_202",datetime(2025,2,10,8,0,0),datetime(2025,2,10,8,20,0),1200,6),
    ],
}.items():
    w(spark.createDataFrame(rows, ss), "{}/analytics_db_tz.db/sessions/{}".format(WH, part))
print("  R-19: analytics_db_tz.sessions (6 rows, 2 year/month parts)")

# -- R-11  logs_db_tz.app_logs (10 rows, 3 dt partitions) ------------------------
ls_ = StructType([
    StructField("log_time",StringType(),False),StructField("level",StringType(),False),
    StructField("service",StringType(),False),StructField("message",StringType(),False),
    StructField("trace_id",StringType(),True),
])
for part, rows in {
    "dt=2025-01-01":[
        ("2025-01-01 00:01:00","INFO","api-gateway","Service started","t001"),
        ("2025-01-01 08:30:00","WARN","database","Slow query detected","t002"),
        ("2025-01-01 10:00:00","ERROR","auth-service","Token expired","t003"),
    ],
    "dt=2025-01-15":[
        ("2025-01-15 03:00:00","INFO","scheduler","Job started","t004"),
        ("2025-01-15 03:45:00","ERROR","scheduler","Job failed: OOM","t005"),
        ("2025-01-15 04:00:00","INFO","scheduler","Retry succeeded","t006"),
        ("2025-01-15 09:00:00","DEBUG","api-gateway","Request traced","t007"),
    ],
    "dt=2025-02-01":[
        ("2025-02-01 00:00:00","INFO","deployer","Deploy started","t008"),
        ("2025-02-01 00:05:00","INFO","deployer","Deploy completed","t009"),
        ("2025-02-01 12:00:00","INFO","health-check","All services OK","t010"),
    ],
}.items():
    w(spark.createDataFrame(rows, ls_), "{}/logs_db_tz.db/app_logs/{}".format(WH, part))
print("  R-11: logs_db_tz.app_logs (10 rows, 3 dt parts)")

# -- R-12  empty_table (0 rows -> EMPTY_SOURCE) --------------------------------
w(spark.createDataFrame([], StructType([
    StructField("id",IntegerType(),False),
    StructField("label",StringType(),True),
    StructField("value",DoubleType(),True),
])), "{}/edge_cases_db.db/empty_table".format(WH))
print("  R-12: edge_cases_db.empty_table (0 rows)")

# -- R-13  nulls_table ---------------------------------------------------------
w(spark.createDataFrame([
    (1,"not null",42,3.14,True,datetime(2025,1,1,0,0,0)),
    (2,None,None,None,None,None),
    (3,"partial",None,2.71,False,None),
    (4,None,99,None,True,datetime(2025,6,15,12,0,0)),
    (5,"all_present",0,0.0,False,datetime(2025,12,31,23,59,59)),
], StructType([
    StructField("id",IntegerType(),False),StructField("str_col",StringType(),True),
    StructField("int_col",IntegerType(),True),StructField("double_col",DoubleType(),True),
    StructField("bool_col",BooleanType(),True),StructField("ts_col",TimestampType(),True),
])), "{}/edge_cases_db.db/nulls_table".format(WH))
print("  R-13: edge_cases_db.nulls_table (5 rows with NULLs)")

# -- R-14  complex_types (STRUCT/ARRAY/MAP) ------------------------------------
cx_s = StructType([
    StructField("id",IntegerType(),False),
    StructField("tags",ArrayType(StringType()),True),
    StructField("scores",MapType(StringType(),DoubleType()),True),
    StructField("address",StructType([
        StructField("street",StringType(),True),
        StructField("city",StringType(),True),
        StructField("zip",StringType(),True),
    ]),True),
    StructField("metrics",ArrayType(StructType([
        StructField("name",StringType(),True),
        StructField("value",DoubleType(),True),
    ])),True),
])
w(spark.createDataFrame([
    (1,["spark","iceberg","migration"],{"accuracy":0.95,"latency_ms":12.5},
       {"street":"123 Main St","city":"San Francisco","zip":"94105"},
       [{"name":"cpu","value":45.2},{"name":"mem","value":78.1}]),
    (2,["hive","parquet"],{"accuracy":0.88},
       {"street":"456 Oak Ave","city":"Seattle","zip":"98101"},
       [{"name":"cpu","value":22.0}]),
    (3,None,None,None,None),
], cx_s), "{}/edge_cases_db.db/complex_types".format(WH))
print("  R-14: edge_cases_db.complex_types (3 rows STRUCT/ARRAY/MAP)")

# -- R-15  wide_table (50 columns) ---------------------------------------------
wide_fields = [StructField("id",IntegerType(),False)] + \
              [StructField("col_{:02d}".format(i),StringType(),True) for i in range(1,50)]
wide_rows = [tuple([r] + ["val_{}_{}".format(r,c) if (r+c)%7!=0 else None
                           for c in range(1,50)]) for r in range(1,6)]
w(spark.createDataFrame(wide_rows, StructType(wide_fields)),
  "{}/edge_cases_db.db/wide_table".format(WH))
print("  R-15: edge_cases_db.wide_table (5 rows, 50 cols)")

# -- R-16/R-18  formats_db.parquet_table, orc_table ---------------------------
fmt_s = StructType([
    StructField("id",IntegerType(),False),
    StructField("name",StringType(),True),
    StructField("amount",DoubleType(),True),
])
fmt_d = [(1,"Alpha",100.0),(2,"Beta",200.0),(3,"Gamma",None),(4,"Delta",400.0),(5,"Epsilon",500.0)]
w(spark.createDataFrame(fmt_d, fmt_s), "{}/formats_db.db/parquet_table".format(WH), "parquet")
w(spark.createDataFrame(fmt_d, fmt_s), "{}/formats_db.db/orc_table".format(WH), "orc")
print("  R-16/R-18: formats_db.parquet_table, orc_table (5 rows each)")

# -- R-20  TZ tables (LA 17:00 values) ----------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW events_la AS SELECT * FROM VALUES
        ('EV001','0575890215',TIMESTAMP('2026-04-07 17:00:00'),TIMESTAMP('2026-04-07 17:00:00'),'2026-04-10','LOAN_APPLICATION'),
        ('EV002','0575890496',TIMESTAMP('2026-04-07 17:00:00'),TIMESTAMP('2026-04-07 17:00:00'),'2026-04-10','LOAN_APPLICATION'),
        ('EV003','0575954763',TIMESTAMP('2026-04-06 17:00:00'),TIMESTAMP('2026-04-06 17:00:00'),'2026-04-10','LOAN_INQUIRY'),
        ('EV004','0576014294',TIMESTAMP('2026-04-07 17:00:00'),TIMESTAMP('2026-04-07 17:00:00'),'2026-04-10','LOAN_APPLICATION'),
        ('EV005','0576136782',TIMESTAMP('2026-03-17 17:00:00'),TIMESTAMP('2026-03-17 17:00:00'),'2026-04-10','LOAN_REVIEW'),
        ('EV006','0576200001',TIMESTAMP('2026-04-08 09:30:00'),TIMESTAMP('2026-04-08 09:30:00'),'2026-04-11','LOAN_APPROVED'),
        ('EV007','0576200002',TIMESTAMP('2026-04-08 14:15:00'),TIMESTAMP('2026-04-08 14:15:00'),'2026-04-11','LOAN_DENIED'),
        ('EV008','0576200003',TIMESTAMP('2026-04-09 08:00:00'),TIMESTAMP('2026-04-09 08:00:00'),'2026-04-11','LOAN_APPLICATION')
    AS t(event_id,ln_nbr_10,application_dt,ln_registration_dt,bus_date,event_type)
""")
spark.table("events_la").write.mode("overwrite").parquet(
    "{}/tz_db.db/events_with_ts_la".format(WH))

spark.sql("""
    CREATE OR REPLACE TEMP VIEW orders_la AS SELECT * FROM VALUES
        ('ORD001','0575890215',TIMESTAMP('2026-04-07 17:00:00'),TIMESTAMP('2026-04-07 17:00:00'),'ACTIVE',250.00,'2026-04-10'),
        ('ORD002','0575890496',TIMESTAMP('2026-04-07 17:00:00'),TIMESTAMP('2026-04-07 17:00:00'),'PENDING',175.50,'2026-04-10'),
        ('ORD003','0575954763',TIMESTAMP('2026-04-06 17:00:00'),TIMESTAMP('2026-04-06 17:00:00'),'ACTIVE',320.00,'2026-04-10'),
        ('ORD004','0576014294',TIMESTAMP('2026-04-07 17:00:00'),TIMESTAMP('2026-04-07 17:00:00'),'CLOSED',89.99,'2026-04-10'),
        ('ORD005','0576136782',TIMESTAMP('2026-03-17 17:00:00'),TIMESTAMP('2026-03-17 17:00:00'),'ACTIVE',540.25,'2026-04-10'),
        ('ORD006','0576200001',TIMESTAMP('2026-04-08 09:30:00'),TIMESTAMP('2026-04-08 09:30:00'),'ACTIVE',110.00,'2026-04-11'),
        ('ORD007','0576200002',TIMESTAMP('2026-04-08 14:15:00'),TIMESTAMP('2026-04-08 14:15:00'),'PENDING',225.75,'2026-04-11'),
        ('ORD008','0576200003',TIMESTAMP('2026-04-09 08:00:00'),TIMESTAMP('2026-04-09 08:00:00'),'ACTIVE',399.00,'2026-04-11')
    AS t(order_id,ln_nbr_10,application_dt,ln_registration_dt,status,amount,dt)
""")
spark.table("orders_la").filter("dt='2026-04-10'").drop("dt").write.mode("overwrite").parquet(
    "{}/tz_db.db/orders_with_ts_la/dt=2026-04-10".format(WH))
spark.table("orders_la").filter("dt='2026-04-11'").drop("dt").write.mode("overwrite").parquet(
    "{}/tz_db.db/orders_with_ts_la/dt=2026-04-11".format(WH))

spark.sql("""
    CREATE OR REPLACE TEMP VIEW sessions_la AS SELECT * FROM VALUES
        ('SES001','USER_A',TIMESTAMP('2026-04-01 08:00:00'),TIMESTAMP('2026-04-01 08:45:00'),2700,2026,4),
        ('SES002','USER_B',TIMESTAMP('2026-04-02 09:30:00'),TIMESTAMP('2026-04-02 10:00:00'),1800,2026,4),
        ('SES003','USER_C',TIMESTAMP('2026-04-15 17:00:00'),TIMESTAMP('2026-04-15 17:30:00'),1800,2026,4),
        ('SES004','USER_D',TIMESTAMP('2026-04-20 17:00:00'),TIMESTAMP('2026-04-20 18:00:00'),3600,2026,4),
        ('SES005','USER_E',TIMESTAMP('2026-05-01 10:00:00'),TIMESTAMP('2026-05-01 10:30:00'),1800,2026,5),
        ('SES006','USER_F',TIMESTAMP('2026-05-10 17:00:00'),TIMESTAMP('2026-05-10 17:45:00'),2700,2026,5)
    AS t(session_id,user_id,start_ts,end_ts,duration_sec,year,month)
""")
spark.table("sessions_la").filter("year=2026 AND month=4").drop("year","month").write.mode("overwrite").parquet(
    "{}/tz_db.db/sessions_with_ts_la/year=2026/month=4".format(WH))
spark.table("sessions_la").filter("year=2026 AND month=5").drop("year","month").write.mode("overwrite").parquet(
    "{}/tz_db.db/sessions_with_ts_la/year=2026/month=5".format(WH))
print("  R-20: tz_db.*_la written (8/8/6 rows)")

# -- T-01  retry_test_db.yarn_oom_table (20 rows) ------------------------------
t_s = StructType([
    StructField("id",IntegerType(),False),
    StructField("payload",StringType(),True),
    StructField("value",DoubleType(),True),
])
w(spark.createDataFrame(
    [(i,"payload_{}".format(i),float(i)*1.5) for i in range(1,21)], t_s),
  "{}/retry_test_db.db/yarn_oom_table".format(WH))
print("  T-01: retry_test_db.yarn_oom_table (20 rows)")

# -- P-03/P-04  corrupt_test_db: 3 tables in one batch ------------------------
# table_a and table_c: healthy data
# corrupt_table: valid data here; corrupt AFTER DAG1 copies it to S3
ct_s = StructType([
    StructField("id",IntegerType(),False),
    StructField("name",StringType(),True),
    StructField("amount",DoubleType(),True),
])
for tbl, data in [
    ("table_a",       [(1,"Alpha",100.0),(2,"Beta",200.0),(3,"Gamma",None)]),
    ("corrupt_table", [(1,"Corrupt1",999.0),(2,"Corrupt2",888.0)]),
    ("table_c",       [(1,"Delta",400.0),(2,"Epsilon",500.0),(3,"Zeta",600.0),(4,"Eta",700.0)]),
]:
    w(spark.createDataFrame(data, ct_s),
      "{}/corrupt_test_db.db/{}".format(WH, tbl))
    print("  P-04: corrupt_test_db.{} written ({} rows)".format(tbl, len(data)))

spark.stop()
print("  All PySpark data written.")
PYEOF


# =============================================================================
# [10/11]  Verification
# =============================================================================
echo ""
echo "============================================================"
echo " [10/11] Verification"
echo "============================================================"

pyspark --master local[*] << 'PYEOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("verify_ebr") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

checks = [
    ("migration_db.customers",         "R-01",  10, None),
    ("migration_db.products",          "R-02",  10, None),
    ("migration_db.transactions",      "R-03",  15, None),
    ("metrics_db.daily_active_users",  "R-04a",  4, None),
    ("metrics_db.conversion_rates",    "R-04b",  4, None),
    ("metrics_db.revenue_summary",     "R-04c",  4, None),
    ("audit_db.access_log",            "R-05a",  5, None),
    ("audit_db.change_log",            "R-05b",  4, None),
    ("hr_db_tz.employees",                "R-06a",  6, None),
    ("hr_db_tz.departments",              "R-06b",  4, None),
    ("sales_db_tz.orders",                "R-07",   9, ("dt",3)),
    ("sales_db_tz.returns",               "R-08",   4, ("dt",3)),
    ("sales_db_tz.daily_summary",         "R-09",   6, ("year,month",3)),
    ("analytics_db_tz.events",            "R-10",   9, ("region,year,month",3)),
    ("analytics_db_tz.sessions",          "R-19",   6, ("year,month",2)),
    ("logs_db_tz.app_logs",               "R-11",  10, ("dt",3)),
    ("edge_cases_db.empty_table",      "R-12",   0, None),
    ("edge_cases_db.nulls_table",      "R-13",   5, None),
    ("edge_cases_db.complex_types",    "R-14",   3, None),
    ("edge_cases_db.wide_table",       "R-15",   5, None),
    ("formats_db.parquet_table",       "R-18",   5, None),
    ("formats_db.orc_table",           "R-16",   5, None),
    ("formats_db.text_table",          "R-17",   5, None),
    ("tz_db.events_with_ts_la",        "R-20a",  8, None),
    ("tz_db.orders_with_ts_la",        "R-20b",  8, ("dt",2)),
    ("tz_db.sessions_with_ts_la",      "R-20c",  6, ("year,month",2)),
    ("retry_test_db.yarn_oom_table",   "T-01",  20, None),
    ("corrupt_test_db.table_a",        "P-04a",  3, None),
    ("corrupt_test_db.corrupt_table",  "P-04b",  2, None),
    ("corrupt_test_db.table_c",        "P-04c",  4, None),
]

all_pass = True
print("  {:<42} {:<7} {:<9} {:<9} {}".format("TABLE","TC","EXPECTED","ACTUAL","STATUS"))
print("  " + "-"*72)
for table, tc, expected, part_check in checks:
    try:
        actual = spark.sql("SELECT COUNT(*) as c FROM {}".format(table)).collect()[0]["c"]
        ok = (actual == expected)
        note = ""
        if part_check:
            pcols, exp_parts = part_check
            try:
                got = spark.sql("SHOW PARTITIONS {}".format(table)).count()
                note = "  parts={}(exp {})".format(got, exp_parts)
                if got != exp_parts:
                    ok = False
            except Exception:
                note = "  (partition check skipped)"
        if not ok:
            all_pass = False
        print("  {:<42} {:<7} {:<9} {:<9} {}{}".format(
            table, tc, expected, actual, "PASS" if ok else "FAIL", note))
    except Exception as e:
        all_pass = False
        print("  {:<42} {:<7} ERROR: {}".format(table, tc, str(e)[:60]))

print("")

# Structural checks
try:
    r = spark.sql("SELECT tags, address FROM edge_cases_db.complex_types WHERE id=1").collect()[0]
    assert r.tags and "spark" in r.tags
    assert r.address and r.address.city == "San Francisco"
    print("  R-14 STRUCT/ARRAY content: PASS")
except Exception as e:
    print("  R-14 STRUCT/ARRAY content: FAIL --", e)
    all_pass = False

try:
    n = spark.sql(
        "SELECT COUNT(*) as c FROM edge_cases_db.nulls_table WHERE str_col IS NULL"
    ).collect()[0]["c"]
    assert n >= 2
    print("  R-13 NULL values:           PASS ({} rows with null str_col)".format(n))
except Exception as e:
    print("  R-13 NULL values:           FAIL --", e)
    all_pass = False

try:
    dts = [r["dt"] for r in spark.sql(
        "SELECT DISTINCT dt FROM logs_db_tz.app_logs WHERE dt >= '2025-01-15' ORDER BY dt"
    ).collect()]
    assert dts == ["2025-01-15","2025-02-01"]
    print("  R-11 partition filter range: PASS (2 of 3 parts >= 2025-01-15)")
except Exception as e:
    print("  R-11 partition filter range: FAIL --", e)
    all_pass = False

print("")
if all_pass:
    print("  ALL CHECKS PASSED -- data ready.")
else:
    print("  SOME CHECKS FAILED -- fix before running DAGs.")

spark.stop()
PYEOF


# =============================================================================
# [11/11]  Instructions for manual test cases
# =============================================================================
echo ""
echo "============================================================"
echo " [11/11] Test execution instructions"
echo "============================================================"
cat << 'INST_EOF'

SOURCE DATA READY
================================================================

REGRESSION BASELINE (run DAG 1 first, then DAG 2):
  All regression tables should produce VALIDATED after both DAGs.
  Exceptions expected:
    EMPTY_SOURCE      : edge_cases_db.empty_table
    TABLE_NOT_FOUND   : migration_db.nonexistent_tbl
    DATABASE_NOT_FOUND: does_not_exist_db.*
    SKIPPED           : formats_db_s3.text_table (INPLACE=TRUE in DAG2 Excel)
    SKIPPED           : migration_db_s3.customers (INPLACE on already-Iceberg)

TRANSIENT RETRY TESTS:
  T-01 YARN OOM during DistCp (retry_test_db.yarn_oom_table):
    Configure YARN with low container memory before running DAG1.
    Expected: run_distcp_ssh retries up to 3 times. Log does NOT contain
    "[PermanentFail]". Airflow attempt count = 3.

  T-03 YARN AM restart message (unit test -- no infra needed):
    python3 -c "
    from utils.migrations.shared import is_permanent_error
    msg = 'Application application_1234567890_0001 does not exist in the list'
    print('PASS' if not is_permanent_error('distcp', Exception(msg)) else 'FAIL')
    "
    Expected: PASS (transient -- must NOT suppress retry)

  T-05 Iceberg commit conflict (unit test):
    python3 -c "
    from utils.migrations.shared import is_permanent_error
    msg = 'CommitFailedException: Cannot commit due to conflicting transaction'
    print('PASS' if not is_permanent_error('iceberg_migrate', Exception(msg)) else 'FAIL')
    "
    Expected: PASS (handled by execute_with_iceberg_retry internally)

PERMANENT FAIL TESTS (run AFTER DAG 1 completes for these tables):

  P-01 Source path missing -> permanent_fail, 0 retries:
    hdfs dfs -rm -r hdfs://localhost:9000/user/hive/warehouse/migration_db.db/products
    Re-run DAG1 for migration_db/products.
    Expected: run_distcp_ssh FAILED on attempt 1. Log shows [PermanentFail].
    Error contains "Input path does not exist" or "File does not exist:".

  P-02 Permission denied -> permanent_fail, 0 retries:
    hdfs dfs -chmod 000 hdfs://localhost:9000/user/hive/warehouse/migration_db.db/transactions
    Re-run DAG1 for migration_db.transactions.
    Expected: permanent_fail fires on attempt 1. Log shows [PermanentFail].
    Restore after: hdfs dfs -chmod 755 .../migration_db.db/transactions

  P-03 Single corrupt Parquet -> permanent_fail after DAG2:
    After DAG1 copies migration_db.customers to S3:
      aws s3 cp /dev/urandom s3://BUCKET/migration_db_s3/customers/part-0000.parquet \
          --content-type application/octet-stream
    Run DAG2 for migration_db_s3.customers (SNAPSHOT).
    Expected: migrate_tables_to_iceberg FAILED on attempt 1.
    Error contains "could not read footer" or "invalid parquet file".

  P-04 KEY TEST -- Multi-table batch, one corrupt, others healthy:
    Step 1: DAG1 copies corrupt_test_db (table_a, corrupt_table, table_c) to S3.
    Step 2: Corrupt exactly one Parquet file in corrupt_table's S3 prefix:
      aws s3 cp /dev/urandom \
          s3://BUCKET/corrupt_test_db_s3/corrupt_table/part-0000.parquet
    Step 3: Run DAG2 for corrupt_test_db_s3 (all 3 tables as one batch, SNAPSHOT).
    Expected:
      - table_a:       VALIDATED  (processed BEFORE corrupt_table)
      - corrupt_table: FAILED     (permanent error)
      - table_c:       VALIDATED  (processed AFTER corrupt_table -- KEY assertion)
      - Task:          FAILED permanently (no retries)
      - Airflow attempt_number: 1 (no retries consumed)
    This validates the _permanent_failure flag fix: remaining tables NOT skipped.

  P-05 TEXT INPLACE -> SKIPPED (not a failure):
    DAG2 Excel already includes formats_db_s3.text_table with inplace=TRUE.
    Expected: status=SKIPPED in iceberg_migration_table_status, NOT FAILED.

  P-06 Corrupt Excel -> permanent_fail on parse_excel:
    Upload a binary garbage file to S3 as the Excel config path.
    Run DAG1. Expected: parse_excel FAILED on attempt 1 with [PermanentFail].

  P-07 Expired MapR ticket (compare with SSH timeout):
    Let MapR ticket expire, run DAG1.
    Expected: validate_prerequisites FAILED on attempt 1 (permanent).
    Compare: with SSH timeout, validate_prerequisites retries (transient).
    Verify:
      is_permanent_error("validate_prerequisites", Exception("SSH connection refused"))
      should return False (transient)

INST_EOF

echo ""
echo "Done. Source data is ready. Run DAG 1 first, then DAG 2."