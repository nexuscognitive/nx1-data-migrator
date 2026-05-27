#!/bin/bash
# Test the discover task by generating and running the EXACT PySpark script
# that discover_tables_via_spark_ssh builds and uploads to the edge node.
#
# This script is a local substitute for the DAG's SSH+SFTP steps:
#   1. Writes partition_utils.py to /tmp/discover_test/ (DAG does this via SFTP)
#   2. Generates discover_tables.py with your parameters substituted in
#      (DAG does this via Python string .format())
#   3. Runs: pyspark < discover_tables.py
#      (DAG runs this exact command over SSH)
#
# Any change you make to the pyspark_script block in migration_dag_mapr_to_s3.py
# should be copy-pasted into the PYSPARK_SCRIPT heredoc below, then re-run this
# script to verify it works inside the container before committing.
#
# Usage (from host machine):
#   docker exec -u root mapr-edge-node bash /test-discover.sh \
#     <src_db> <tables> <dest_db> <dest_bucket> [partition_filter]
#
# Arguments:
#   src_db           source Hive database name
#   tables           comma-separated table names — no quotes or JSON needed
#                    use * for all tables, orders,customers for specific ones
#   dest_db          destination database name
#   dest_bucket      destination S3 bucket (or any placeholder for local testing)
#   partition_filter optional — partition filter expression (e.g. dt>=2024-01-15)
#
# Examples (run from PowerShell or WSL — no special quoting needed):
#   # Single table
#   docker exec -u root mapr-edge-node bash /test-discover.sh sales_db orders sales_db s3a://my-bucket
#
#   # All tables in a database
#   docker exec -u root mapr-edge-node bash /test-discover.sh hr_db * hr_db s3a://my-bucket
#
#   # Multiple specific tables
#   docker exec -u root mapr-edge-node bash /test-discover.sh sales_db orders,customers sales_db s3a://my-bucket
#
#   # With partition filter
#   docker exec -u root mapr-edge-node bash /test-discover.sh logs_db app_logs logs_db s3a://my-bucket dt>=2024-01-15
#
#   # Unregistered partitions table (triggers MSCK REPAIR TABLE path in DAG)
#   docker exec -u root mapr-edge-node bash /test-discover.sh analytics_db sessions analytics_db s3a://my-bucket

set -e
source /etc/profile.d/hadoop.sh 2>/dev/null || true
source ~/.profile 2>/dev/null || true

# ---------------------------------------------------------------------------
# Args (mirror the DAG's discover_tables_via_spark_ssh variables exactly)
# ---------------------------------------------------------------------------
SRC_DB="${1:?Usage: $0 <src_db> <tables> <dest_db> <dest_bucket> [partition_filter]}"
TABLES_RAW="${2:?}"
TOKENS_JSON=$(python2.7 -c "
import sys, json
raw = sys.argv[1]
tokens = [t.strip() for t in raw.split(',') if t.strip()]
print(json.dumps(tokens))
" "$TABLES_RAW" 2>/dev/null || python3 -c "
import sys, json
raw = sys.argv[1]
tokens = [t.strip() for t in raw.split(',') if t.strip()]
print(json.dumps(tokens))
" "$TABLES_RAW")
DEST_DB="${3:?}"
DEST_BUCKET="${4:?}"
PARTITION_FILTER="${5:-}"

RUN_ID="local_test_$(date +%Y%m%d_%H%M%S)"
DEST_BUCKET_SLUG=$(echo "$DEST_BUCKET" | sed 's/[^a-zA-Z0-9_-]/_/g')

TEMP_DIR="/tmp/discovery_${RUN_ID}_${SRC_DB}_${DEST_DB}_${DEST_BUCKET_SLUG}"
mkdir -p "$TEMP_DIR"

echo "======================================================"
echo " DISCOVER TEST"
echo " src_db          : $SRC_DB"
echo " tables          : $TABLES_RAW  (json: $TOKENS_JSON)"
echo " dest_db         : $DEST_DB"
echo " dest_bucket     : $DEST_BUCKET"
echo " partition_filter: ${PARTITION_FILTER:-(none)}"
echo " temp_dir        : $TEMP_DIR"
echo "======================================================"

# ---------------------------------------------------------------------------
# Step 1: Write partition_utils.py to temp_dir
# (DAG does this via sftp.put of the local utils/migrations/partition_utils.py)
# ---------------------------------------------------------------------------
echo ""
echo ">>> [1] Writing partition_utils.py to $TEMP_DIR ..."

cat > "$TEMP_DIR/partition_utils.py" << 'PARTITION_UTILS_EOF'
"""
Pure-Python partition filtering and SQL clause helpers.
-- EXACT COPY of utils/migrations/partition_utils.py --
-- Keep in sync with any changes made to that file.  --
"""

import re
try:
    from urllib import unquote as url_unquote     
except ImportError:
    from urllib.parse import unquote as url_unquote


def apply_partition_filter(partitions, filter_expr):
    """Filter Hive partition strings against a filter expression."""

    if not filter_expr:
        return partitions

    def parse_partition(part_str):
        result = {}
        for segment in part_str.split('/'):
            if '=' in segment:
                k, _, v = segment.partition('=')
                result[k.strip()] = url_unquote(v.strip())
        return result

    def try_numeric(val):
        try:
            return int(val)
        except ValueError:
            return val

    terms = [t.strip() for t in filter_expr.split(',') if t.strip()]
    matched = set()

    for term in terms:
        m = re.match(r'^last_n_partitions=(\d+)$', term.strip())
        if m:
            for p in sorted(partitions, reverse=True)[:int(m.group(1))]:
                matched.add(p)
            continue

        if not any(op in term for op in ('>=', '<=', '>', '<')):
            if term.endswith('/*') or term.endswith('*'):
                prefix = term.rstrip('*').rstrip('/')
                for p in partitions:
                    if p.startswith(prefix):
                        matched.add(p)
            else:
                for p in partitions:
                    if p == term:
                        matched.add(p)
            continue

        op_match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)(>=|<=|>|<)(.+)$', term.strip())
        if op_match:
            key = op_match.group(1)
            op = op_match.group(2)
            threshold_raw = op_match.group(3).strip()
            threshold_is_prefix = threshold_raw.endswith('*')
            threshold = threshold_raw.rstrip('*')
            threshold_cmp = try_numeric(threshold)
            for p in partitions:
                pdict = parse_partition(p)
                if key not in pdict:
                    continue
                pval = pdict[key]
                pval_for_cmp = pval[:len(threshold)] if threshold_is_prefix else pval
                pval_cmp = try_numeric(pval_for_cmp)
                try:
                    if (
                        (op == '>=' and pval_cmp >= threshold_cmp)
                        or (op == '<=' and pval_cmp <= threshold_cmp)
                        or (op == '>'  and pval_cmp >  threshold_cmp)
                        or (op == '<'  and pval_cmp <  threshold_cmp)
                    ):
                        matched.add(p)
                except TypeError:
                    if (
                        (op == '>=' and pval_for_cmp >= threshold)
                        or (op == '<=' and pval_for_cmp <= threshold)
                        or (op == '>'  and pval_for_cmp >  threshold)
                        or (op == '<'  and pval_for_cmp <  threshold)
                    ):
                        matched.add(p)
            continue

    return [p for p in partitions if p in matched]


def partitions_to_where_clause(partitions):
    """Convert partition strings to a SQL WHERE clause. Values are single-quote escaped."""
    if not partitions:
        return "1=0"
    clauses = []
    for part_str in partitions:
        conditions = []
        for segment in part_str.split('/'):
            if '=' in segment:
                k, _, v = segment.partition('=')
                conditions.append("`%s`='%s'" % (k.strip(), v.replace("'", "''").strip()))
        if conditions:
            clauses.append("(" + " AND ".join(conditions) + ")")
    return " OR ".join(clauses) if clauses else "1=1"
PARTITION_UTILS_EOF

echo "    partition_utils.py written."

# ---------------------------------------------------------------------------
# Step 2: Generate discover_tables.py
# This is the EXACT pyspark_script from discover_tables_via_spark_ssh,
# with the same .format() substitutions the DAG applies.
#
# TO TEST DAG CHANGES: update the PYSPARK_SCRIPT heredoc below to match
# the updated pyspark_script block in migration_dag_mapr_to_s3.py, then
# re-run this script.
# ---------------------------------------------------------------------------
echo ""
echo ">>> [2] Generating discover_tables.py ..."

# Escape single quotes in tokens_json and partition_filter for shell embedding
# (DAG does: tokens_json.replace("'", "\\'") and partition_filter.replace("'","\'").replace('"','\"'))
TOKENS_JSON_ESCAPED=$(echo "$TOKENS_JSON" | sed "s/'/\\\\'/g")
FILTER_ESCAPED=$(echo "$PARTITION_FILTER" | sed "s/'/\\\\'/g" | sed 's/"/\\"/g')

SCRIPT_PATH="$TEMP_DIR/discover_tables.py"

# ============================================================
# PYSPARK_SCRIPT — exact copy of the pyspark_script heredoc
# in discover_tables_via_spark_ssh(), with shell variable
# substitution replacing the Python .format() placeholders.
#
# Python .format() key  →  shell variable
#   {temp_dir}          →  $TEMP_DIR
#   {run_id}            →  $RUN_ID
#   {src_db}            →  $SRC_DB
#   {dest_db}           →  $DEST_DB
#   {dest_bucket}       →  $DEST_BUCKET
#   {dest_bucket_slug}  →  $DEST_BUCKET_SLUG
#   {tokens_json_escaped} → $TOKENS_JSON_ESCAPED
#   {filter_expr_escaped} → $FILTER_ESCAPED
# ============================================================
cat > "$SCRIPT_PATH" << PYSPARK_SCRIPT_EOF
import json
import sys
import fnmatch
from pyspark.sql import SparkSession
sys.path.insert(0, "$TEMP_DIR")
from partition_utils import apply_partition_filter, partitions_to_where_clause

spark = SparkSession.builder \\
    .appName("table_discovery_${RUN_ID}_${SRC_DB}_${DEST_DB}_${DEST_BUCKET_SLUG}") \\
    .enableHiveSupport() \\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

src_db = "$SRC_DB"
dest_db = "$DEST_DB"
dest_bucket = "$DEST_BUCKET"

tokens = json.loads('$TOKENS_JSON_ESCAPED')

def resolve_tokens(spark, db, tokens):
    resolved = []
    seen = set()

    for tok in tokens:
        if tok == '*':
            rows = spark.sql("SHOW TABLES IN {0}".format(db)).collect()
            for r in rows:
                t = r.tableName
                if t not in seen:
                    seen.add(t)
                    resolved.append(t)
        elif '*' in tok:
            rows = spark.sql(
                "SHOW TABLES IN {0} LIKE '{1}'".format(db, tok)
            ).collect()
            for r in rows:
                t = r.tableName
                if t not in seen:
                    seen.add(t)
                    resolved.append(t)
        else:
            if tok not in seen:
                seen.add(tok)
                resolved.append(tok)

    return resolved

table_list = resolve_tokens(spark, src_db, tokens)

metadata = []

for tbl in table_list:
    loc = None
    table_type = "UNKNOWN"
    input_format = None
    serde_properties = {}
    in_serde_section = False
    # Pre-initialise all variables referenced in the except block so they are
    # always defined even if the exception fires before the try body reaches them.
    filter_expr = "$FILTER_ESCAPED"
    row_count = 0
    filtered_partitions = []
    partition_filter_active = False
    filtered_source_size = 0
    filtered_file_count = 0
    full_row_count = 0
    full_partition_count = 0
    try:
        desc_df = spark.sql(
            "DESCRIBE FORMATTED {0}.{1}".format(src_db, tbl)
        )
        desc_rows = desc_df.collect()

        for row in desc_rows:
            col_name = (row.col_name or "").strip().rstrip(":").lower()
            data_type = (row.data_type or "").strip()

            if col_name == "location":
                loc = data_type
            elif col_name in ("type", "table type"):
                table_type = data_type.replace("_TABLE", "")
            elif col_name == "inputformat":
                input_format = data_type
            elif col_name in (
                "storage properties",
                "storage desc params",
                "serde library",
                "serialization lib",
                "parameters",
            ):
                raw = data_type.strip("[]")
                for pair in raw.split(", "):
                    if "=" in pair:
                        k, _, v = pair.partition("=")
                        k = k.strip()
                        v = v.strip()
                        if k in ("field.delim", "escape.delim", "null.format", "field delimiter"):
                            serde_properties[k] = v

        source_total_size = 0
        source_file_count = 0
        if loc:
            try:
                from py4j.java_gateway import java_import
                java_import(spark._jvm, "org.apache.hadoop.fs.*")

                fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark._jvm.java.net.URI(loc),
                    spark._jsc.hadoopConfiguration()
                )
                path = spark._jvm.org.apache.hadoop.fs.Path(loc)

                if fs.exists(path):
                    content_summary = fs.getContentSummary(path)
                    source_total_size = int(content_summary.getLength())
                    source_file_count = int(content_summary.getFileCount())
            except Exception:
                pass

        file_format = "PARQUET"
        if input_format:
            if "parquet" in input_format.lower():
                file_format = "PARQUET"
            elif "orc" in input_format.lower():
                file_format = "ORC"
            elif "avro" in input_format.lower():
                file_format = "AVRO"
            elif "text" in input_format.lower():
                file_format = "TEXTFILE"

        row_count = 0
        try:
            row_count = spark.sql(
                "SELECT COUNT(*) as c FROM {0}.{1}".format(src_db, tbl)
            ).collect()[0].c
        except Exception:
            pass

        partition_cols_from_describe = []
        in_partition_section = False
        for row in desc_rows:
            col_name = (row.col_name or "").strip()
            if col_name == "# Partition Information":
                in_partition_section = True
                continue
            if in_partition_section and col_name == "# col_name":
                continue
            if in_partition_section and col_name.startswith("#"):
                break
            if in_partition_section and col_name:
                partition_cols_from_describe.append(col_name)

        partition_definition = len(partition_cols_from_describe) > 0
        partition_columns = ",".join(partition_cols_from_describe)

        partitions = []
        registered_partition_count = 0
        filtered_partitions = []
        partition_filter_active = False
        filtered_source_size = source_total_size
        filtered_file_count  = source_file_count
        full_row_count = row_count
        full_partition_count = 0
        try:
            parts_df = spark.sql(
                "SHOW PARTITIONS {0}.{1}".format(src_db, tbl)
            )
            partitions = [row.partition for row in parts_df.collect()]
            registered_partition_count = len(partitions)
        except Exception:
            pass

        # Apply partition filter
        filter_expr = "$FILTER_ESCAPED"
        full_partition_count = len(partitions)
        full_row_count = row_count  # already computed above
        filtered_partitions = apply_partition_filter(partitions, filter_expr)
        partition_filter_active = bool(filter_expr) and len(filtered_partitions) < len(partitions)

        # Warn edge cases
        if filter_expr and not partition_definition:
            import sys as _sys
            partition_filter_active = False
            filtered_partitions = partitions

        # Filtered size and file count
        filtered_source_size = source_total_size
        filtered_file_count  = source_file_count
        if partition_filter_active and filtered_partitions and loc:
            filtered_source_size = 0
            filtered_file_count  = 0
            for part_str in filtered_partitions:
                try:
                    part_path = spark._jvm.org.apache.hadoop.fs.Path(loc + "/" + part_str)
                    if fs.exists(part_path):
                        cs = fs.getContentSummary(part_path)
                        filtered_source_size += int(cs.getLength())
                        filtered_file_count  += int(cs.getFileCount())
                except Exception:
                    pass

        # Filtered row count
        if partition_filter_active and filtered_partitions:
            try:
                where_clause = partitions_to_where_clause(filtered_partitions)
                row_count = spark.sql(
                    "SELECT COUNT(*) as c FROM {0}.{1} WHERE ".format(src_db, tbl) + where_clause
                ).collect()[0].c
            except Exception:
                pass

        is_partitioned = partition_definition
        unregistered_partitions = partition_definition and registered_partition_count == 0

        schema_df = spark.sql(
            "DESCRIBE {0}.{1}".format(src_db, tbl)
        )
        schema = []
        for row in schema_df.collect():
            col_name = row.col_name.strip() if row.col_name else ""
            data_type = row.data_type.strip() if row.data_type else ""

            if col_name.startswith("#") or col_name == "" or col_name == "col_name":
                break

            schema.append({"name": col_name, "type": data_type})

        s3_location = "{0}/{1}/{2}".format(dest_bucket, dest_db, tbl)

        metadata.append({
            "source_database": src_db,
            "source_table": tbl,
            "dest_database": dest_db,
            "dest_bucket": dest_bucket,
            "source_location": loc or "",
            "s3_location": s3_location,
            "file_format": file_format,
            "schema": schema,
            "partitions": filtered_partitions,
            "partition_columns": partition_columns,
            "partition_count": len(partitions),
            "row_count": row_count,
            "is_partitioned": is_partitioned,
            "unregistered_partitions": unregistered_partitions,
            "table_type": table_type,
            "source_total_size_bytes": source_total_size,
            "serde_properties": serde_properties,
            "source_file_count": source_file_count,
            "partition_filter": filter_expr or None,
            "filtered_partitions": filtered_partitions,
            "partition_filter_active": partition_filter_active,
            "filtered_row_count": row_count,
            "filtered_source_size_bytes": filtered_source_size,
            "filtered_file_count": filtered_file_count,
            "full_table_row_count": full_row_count,
            "full_table_partition_count": full_partition_count,
        })

    except Exception as e:
        metadata.append({
            "source_database": src_db,
            "source_table": tbl,
            "dest_database": dest_db,
            "dest_bucket": dest_bucket,
            "source_location": "",
            "s3_location": dest_bucket + "/" + dest_db + "/" + tbl,
            "file_format": "PARQUET",
            "schema": [],
            "partitions": [],
            "partition_columns": "",
            "partition_count": 0,
            "row_count": 0,
            "is_partitioned": False,
            "unregistered_partitions": False,
            "table_type": "UNKNOWN",
            "source_total_size_bytes": 0,
            "source_file_count": 0,
            "serde_properties": serde_properties,
            "partition_filter": filter_expr or None,
            "filtered_partitions": filtered_partitions,
            "partition_filter_active": partition_filter_active,
            "filtered_row_count": row_count,
            "filtered_source_size_bytes": filtered_source_size,
            "filtered_file_count": filtered_file_count,
            "full_table_row_count": full_row_count,
            "full_table_partition_count": full_partition_count,
            "error": str(e)[:500]
        })

print ("===JSON_START===")
sys.stdout.flush()
print (json.dumps(metadata, indent=2))
sys.stdout.flush()
print ("===JSON_END===")
sys.stdout.flush()

spark.stop()
PYSPARK_SCRIPT_EOF

echo "    discover_tables.py written to $SCRIPT_PATH"

# ---------------------------------------------------------------------------
# Step 3: Run the exact command the DAG executes over SSH
# DAG: pyspark < {script_path} 2>&1 | tee discovery_<run_id>_...log
# ---------------------------------------------------------------------------
echo ""
echo ">>> [3] Running: pyspark < $SCRIPT_PATH"
echo "        (same command DAG runs over SSH)"
echo ""

cd "$TEMP_DIR"
pyspark < "$SCRIPT_PATH" 2>&1 | tee "discovery_${RUN_ID}_${SRC_DB}_${DEST_DB}_${DEST_BUCKET_SLUG}.log"
EXIT_CODE=${PIPESTATUS[0]}

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "======================================================"
    echo " DISCOVER TEST PASSED (exit 0)"
    echo " Full output + JSON saved to:"
    echo " $TEMP_DIR/discovery_${RUN_ID}_${SRC_DB}_${DEST_DB}_${DEST_BUCKET_SLUG}.log"
    echo "======================================================"
else
    echo "======================================================"
    echo " DISCOVER TEST FAILED (exit $EXIT_CODE)"
    echo " Check output above for PySpark errors."
    echo "======================================================"
    exit $EXIT_CODE
fi