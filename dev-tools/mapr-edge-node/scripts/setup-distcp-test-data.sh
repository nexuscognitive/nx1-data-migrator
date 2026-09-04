#!/bin/bash
# =============================================================================
# setup-distcp-sizing-test-data.sh
#
# Test data for the DistCp size-aware tuning feature (feat/distcp_tuning).
#
# Run INSIDE the container:
#   docker exec -u root mapr-edge-node bash /setup-distcp-sizing-test-data.sh
#
# WHY THIS EXISTS
# ---------------
# Every table in setup-test-data.sh is a few KB. With the shipped default of
# distcp_target_bytes_per_mapper = 2 GiB, all of them auto-size to -m 1, so
# they cannot tell correct sizing apart from broken sizing.
#
# This script creates tables in the low-MB range and is meant to be run with
# the sizing knobs scaled DOWN for the test environment:
#
#   migration_distcp_target_bytes_per_mapper = 1048576   (1 MiB)
#   migration_distcp_min_mappers             = 1
#   migration_distcp_max_mappers             = 8
#   migration_distcp_target_aggregate_mbps   = 800
#   migration_distcp_mappers                 = (unset/empty)
#   migration_distcp_bandwidth               = (unset/empty)
#
# At those settings each table below lands on a DIFFERENT branch of
# size_distcp_job(), so the emitted -m / -bandwidth is a real signal.
#
# The script ends by printing the mapper/bandwidth values the DAG SHOULD
# derive, computed from the actual on-disk sizes using the same arithmetic
# as size_distcp_job(). Compare that table against the "[DistCp] Sized ..."
# lines in the Airflow task log.
#
# Safe to re-run — tables are DROP + CREATE, HDFS paths are overwritten.
# =============================================================================

set -euo pipefail

BEELINE="/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true"
WH="hdfs://localhost:9000/user/hive/warehouse"
DB="distcp_sizing_db"
DBDIR="${WH}/${DB}.db"
FOLDER_SRC="hdfs://localhost:9000/user/testdata/distcp_sizing_folder"
STAGE="/tmp/distcp_sizing_stage"

# Sizing knobs — MUST match the Airflow Variables you set for the test run.
TARGET_BYTES_PER_MAPPER=${TARGET_BYTES_PER_MAPPER:-1048576}
MIN_MAPPERS=${MIN_MAPPERS:-1}
MAX_MAPPERS=${MAX_MAPPERS:-8}
TARGET_AGGREGATE_MBPS=${TARGET_AGGREGATE_MBPS:-800}

echo "============================================================"
echo " [1/5] Generating local staging files"
echo "============================================================"

rm -rf "$STAGE"
mkdir -p "$STAGE"

# One 1 MiB block: 16384 lines of 63 'x' + newline = 1048576 bytes exactly.
LINE=$(printf 'x%.0s' $(seq 1 63))
set +o pipefail
yes "$LINE" 2>/dev/null | head -n 16384 > "${STAGE}/block_1m.txt"
set -o pipefail

BLOCK_SIZE=$(stat -c%s "${STAGE}/block_1m.txt")
if [ "$BLOCK_SIZE" -ne 1048576 ]; then
  echo "FATAL: 1 MiB block is ${BLOCK_SIZE} bytes, expected 1048576" >&2
  exit 1
fi
echo "  block_1m.txt = ${BLOCK_SIZE} bytes"

head -c 4096 "${STAGE}/block_1m.txt" > "${STAGE}/tiny_4k.txt"
cat "${STAGE}/block_1m.txt" "${STAGE}/block_1m.txt" "${STAGE}/block_1m.txt" \
    "${STAGE}/block_1m.txt" "${STAGE}/block_1m.txt" > "${STAGE}/block_5m.txt"

echo "============================================================"
echo " [2/5] Creating HDFS layout"
echo "============================================================"

hdfs dfs -rm -r -f -skipTrash "${DBDIR}" >/dev/null 2>&1 || true
hdfs dfs -rm -r -f -skipTrash "${FOLDER_SRC}" >/dev/null 2>&1 || true

hdfs dfs -mkdir -p "${DBDIR}/t_tiny"
hdfs dfs -mkdir -p "${DBDIR}/t_size_bound"
hdfs dfs -mkdir -p "${DBDIR}/t_max_clamp"
hdfs dfs -mkdir -p "${DBDIR}/t_file_clamp"
hdfs dfs -mkdir -p "${DBDIR}/t_partitioned/dt=2024-01-01"
hdfs dfs -mkdir -p "${DBDIR}/t_partitioned/dt=2024-01-02"
hdfs dfs -mkdir -p "${DBDIR}/t_empty"
hdfs dfs -mkdir -p "${FOLDER_SRC}"

echo "============================================================"
echo " [3/5] Loading data"
echo "============================================================"

# t_tiny         — 1 file, 4 KB.  Exercises: min_mappers floor.
hdfs dfs -put -f "${STAGE}/tiny_4k.txt" "${DBDIR}/t_tiny/data_00.txt"

# t_size_bound   — 6 files x 1 MiB. Exercises: pure size-driven count,
#                  neither the max clamp nor the file clamp binds.
for i in 0 1 2 3 4 5; do
  hdfs dfs -put -f "${STAGE}/block_1m.txt" "${DBDIR}/t_size_bound/data_0${i}.txt"
done

# t_max_clamp    — 12 files x 1 MiB. Exercises: distcp_max_mappers ceiling.
for i in $(seq -w 0 11); do
  hdfs dfs -put -f "${STAGE}/block_1m.txt" "${DBDIR}/t_max_clamp/data_${i}.txt"
done

# t_file_clamp   — 2 files x 5 MiB. Exercises: the Hadoop 2.7 no-blocksperchunk
#                  clamp — size wants more mappers than there are files.
hdfs dfs -put -f "${STAGE}/block_5m.txt" "${DBDIR}/t_file_clamp/data_00.txt"
hdfs dfs -put -f "${STAGE}/block_5m.txt" "${DBDIR}/t_file_clamp/data_01.txt"

# t_partitioned  — skewed: 1 file in one partition, 6 in the other.
#                  Exercises: per-partition sizing in the preserve_delete loop.
hdfs dfs -put -f "${STAGE}/block_1m.txt" "${DBDIR}/t_partitioned/dt=2024-01-01/data_00.txt"
for i in 0 1 2 3 4 5; do
  hdfs dfs -put -f "${STAGE}/block_1m.txt" "${DBDIR}/t_partitioned/dt=2024-01-02/data_0${i}.txt"
done

# t_empty        — directory only. Exercises: EMPTY_SOURCE short-circuit,
#                  which must never reach size_distcp_job's command path.

# DAG 3 folder    — 5 files x 1 MiB. Exercises: the hadoop fs -count probe.
for i in 0 1 2 3 4; do
  hdfs dfs -put -f "${STAGE}/block_1m.txt" "${FOLDER_SRC}/part_0${i}.txt"
done

echo "============================================================"
echo " [4/5] Registering Hive tables"
echo "============================================================"

$BEELINE -e "
CREATE DATABASE IF NOT EXISTS ${DB}
  LOCATION 'hdfs://localhost:9000/user/hive/warehouse/${DB}.db';

DROP TABLE IF EXISTS ${DB}.t_tiny;
CREATE TABLE ${DB}.t_tiny (line STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION 'hdfs://localhost:9000/user/hive/warehouse/${DB}.db/t_tiny';

DROP TABLE IF EXISTS ${DB}.t_size_bound;
CREATE TABLE ${DB}.t_size_bound (line STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION 'hdfs://localhost:9000/user/hive/warehouse/${DB}.db/t_size_bound';

DROP TABLE IF EXISTS ${DB}.t_max_clamp;
CREATE TABLE ${DB}.t_max_clamp (line STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION 'hdfs://localhost:9000/user/hive/warehouse/${DB}.db/t_max_clamp';

DROP TABLE IF EXISTS ${DB}.t_file_clamp;
CREATE TABLE ${DB}.t_file_clamp (line STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION 'hdfs://localhost:9000/user/hive/warehouse/${DB}.db/t_file_clamp';

DROP TABLE IF EXISTS ${DB}.t_partitioned;
CREATE TABLE ${DB}.t_partitioned (line STRING)
  PARTITIONED BY (dt STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION 'hdfs://localhost:9000/user/hive/warehouse/${DB}.db/t_partitioned';
ALTER TABLE ${DB}.t_partitioned ADD IF NOT EXISTS PARTITION (dt='2024-01-01');
ALTER TABLE ${DB}.t_partitioned ADD IF NOT EXISTS PARTITION (dt='2024-01-02');

DROP TABLE IF EXISTS ${DB}.t_empty;
CREATE TABLE ${DB}.t_empty (line STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION 'hdfs://localhost:9000/user/hive/warehouse/${DB}.db/t_empty';
"

echo "============================================================"
echo " [5/5] Expected sizing"
echo "============================================================"

ceil_div() { echo $(( ($1 + $2 - 1) / $2 )); }

# Mirrors size_distcp_job() in migrator_utils/migrations/shared.py.
size_job() {
  local size=$1 files=$2 m bw
  m=$(ceil_div "$size" "$TARGET_BYTES_PER_MAPPER")
  (( m > MAX_MAPPERS )) && m=$MAX_MAPPERS
  (( m < MIN_MAPPERS )) && m=$MIN_MAPPERS
  if (( files > 0 )) && (( m > files )); then m=$files; fi
  (( m < MIN_MAPPERS )) && m=$MIN_MAPPERS
  bw=$(( TARGET_AGGREGATE_MBPS / m ))
  (( bw < 1 )) && bw=1
  echo "$m $bw"
}

# hadoop fs -count emits: DIR_COUNT FILE_COUNT CONTENT_SIZE PATH
probe() {
  hdfs dfs -count "$1" 2>/dev/null | awk '{print $2, $3}'
}

printf "\nknobs: target=%s bytes/mapper  min=%s  max=%s  aggregate=%s MB/s\n\n" \
  "$TARGET_BYTES_PER_MAPPER" "$MIN_MAPPERS" "$MAX_MAPPERS" "$TARGET_AGGREGATE_MBPS"

printf "%-34s %10s %7s %8s %11s   %s\n" \
  "PATH" "BYTES" "FILES" "EXP -m" "EXP -bw" "BRANCH EXERCISED"
printf '%.0s-' {1..110}; echo

report() {
  local path=$1 label=$2 note=$3
  read -r files bytes <<< "$(probe "$path")"
  files=${files:-0}; bytes=${bytes:-0}
  if [ "$files" -eq 0 ]; then
    printf "%-34s %10s %7s %8s %11s   %s\n" "$label" "$bytes" "$files" "-" "-" "$note"
    return
  fi
  read -r m bw <<< "$(size_job "$bytes" "$files")"
  printf "%-34s %10s %7s %8s %11s   %s\n" "$label" "$bytes" "$files" "$m" "$bw" "$note"
}

report "${DBDIR}/t_tiny"        "${DB}.t_tiny"        "min_mappers floor"
report "${DBDIR}/t_size_bound"  "${DB}.t_size_bound"  "size-driven, no clamp"
report "${DBDIR}/t_max_clamp"   "${DB}.t_max_clamp"   "max_mappers ceiling"
report "${DBDIR}/t_file_clamp"  "${DB}.t_file_clamp"  "file-count clamp (no -blocksperchunk)"
report "${DBDIR}/t_partitioned" "${DB}.t_partitioned" "table-level value"
report "${DBDIR}/t_empty"       "${DB}.t_empty"       "EMPTY_SOURCE — no distcp expected"
report "${FOLDER_SRC}"          "DAG3 folder"         "hadoop fs -count probe"

echo
echo "Per-partition expectation for ${DB}.t_partitioned"
echo "(preserve_delete=true emits one distcp per partition; the DAG apportions"
echo " table bytes by each partition's file share)"
echo

read -r tfiles tbytes <<< "$(probe "${DBDIR}/t_partitioned")"
printf "%-34s %10s %7s %8s %11s\n" "PARTITION" "APPORT.B" "FILES" "EXP -m" "EXP -bw"
printf '%.0s-' {1..80}; echo
for p in dt=2024-01-01 dt=2024-01-02; do
  read -r pfiles _ <<< "$(probe "${DBDIR}/t_partitioned/${p}")"
  psize=$(( tbytes * pfiles / tfiles ))
  read -r m bw <<< "$(size_job "$psize" "$pfiles")"
  printf "%-34s %10s %7s %8s %11s\n" "$p" "$psize" "$pfiles" "$m" "$bw"
done

echo
echo "Done. Compare these against the '[DistCp] Sized' lines and the emitted"
echo "hadoop distcp commands in the Airflow task logs."