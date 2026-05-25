#!/bin/bash
# Test the distcp task by running the EXACT shell command that
# run_distcp_ssh builds and executes over SSH.
#
# The DAG constructs one of two cmd strings depending on whether
# partition_filter_active is true or false, then runs it via:
#   client.exec_command(cmd, timeout=SSH_COMMAND_TIMEOUT)
#
# This script builds the same cmd strings with your parameters substituted in
# and runs them directly — no SSH layer needed.
#
# Any change you make to the cmd variable in run_distcp_ssh() in
# migration_dag_mapr_to_s3.py should be reflected here, then re-run this
# script to verify it works inside the container before committing.
#
# Usage (from host machine):
#   docker exec -u root \
#     [-e AWS_ACCESS_KEY_ID=...] \
#     [-e AWS_SECRET_ACCESS_KEY=...] \
#     [-e S3_ENDPOINT=https://...] \
#     hadoop-edge-node bash /test-distcp.sh \
#     <source_location> <s3_location> [mappers] [bandwidth] [partition_filter_active] [filtered_partitions_json]
#
# Arguments:
#   source_location          HDFS source path (e.g. hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders)
#   s3_location              Destination path — S3 (s3a://bucket/db/table) or HDFS for local testing
#   mappers                  Number of DistCp mappers (default: 1)
#   bandwidth                Bandwidth limit in MB/s (default: 100)
#   partition_filter_active  "true" or "false" (default: false)
#   filtered_partitions_json JSON array of partition strings when partition_filter_active=true
#                            e.g. '["dt=2024-01-15","dt=2024-02-01"]'
#
# Examples:
#   # Full table copy (no partition filter) — HDFS to HDFS for local testing
#   docker exec -u root hadoop-edge-node bash /test-distcp.sh \
#     hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders \
#     hdfs://localhost:9000/tmp/distcp-test/orders
#
#   # With partition filter active — copies only matching partitions
#   docker exec -u root hadoop-edge-node bash /test-distcp.sh \
#     hdfs://localhost:9000/user/hive/warehouse/logs_db.db/app_logs \
#     hdfs://localhost:9000/tmp/distcp-test/app_logs \
#     1 100 true '["dt=2024-01-15","dt=2024-02-01"]'
#
#   # Real S3 destination (pass AWS creds via -e)
#   docker exec -u root \
#     -e AWS_ACCESS_KEY_ID=AKIA... \
#     -e AWS_SECRET_ACCESS_KEY=... \
#     hadoop-edge-node bash /test-distcp.sh \
#     hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders \
#     s3a://my-bucket/dest-db/orders
#
#   # Run twice on same dest to test incremental (-update -delete) path
#   docker exec -u root hadoop-edge-node bash /test-distcp.sh \
#     hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders \
#     hdfs://localhost:9000/tmp/distcp-test/orders

set -e
source /etc/profile.d/hadoop.sh 2>/dev/null || true
source ~/.profile 2>/dev/null || true

# ---------------------------------------------------------------------------
# Args (mirror the DAG's run_distcp_ssh variables exactly)
# ---------------------------------------------------------------------------
SOURCE_LOC="${1:?Usage: $0 <source_location> <s3_location> [mappers] [bandwidth] [partition_filter_active] [filtered_partitions_json]}"
S3_LOC="${2:?}"
MAPPERS="${3:-1}"
BANDWIDTH="${4:-100}"
PARTITION_FILTER_ACTIVE="${5:-false}"
FILTERED_PARTITIONS_JSON="${6:-[]}"

RUN_ID="local_test_$(date +%Y%m%d_%H%M%S)"
TBL=$(basename "$S3_LOC")
TEMP_DIR="/tmp/distcp_${RUN_ID}_${TBL}"
mkdir -p "$TEMP_DIR"

# ---------------------------------------------------------------------------
# Build s3_opts — mirrors build_s3_opts() in shared.py
# Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT in the environment
# to replicate what the DAG injects from Airflow connections/variables.
# ---------------------------------------------------------------------------
S3_OPTS=""
if [ -n "${AWS_ACCESS_KEY_ID:-}" ]; then
    S3_OPTS=" -Dfs.s3a.access.key=${AWS_ACCESS_KEY_ID} -Dfs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}"
    if [ -n "${S3_ENDPOINT:-}" ]; then
        S3_OPTS="${S3_OPTS} -Dfs.s3a.endpoint=${S3_ENDPOINT} -Dfs.s3a.path.style.access=true"
    fi
fi

echo "======================================================"
echo " DISTCP TEST"
echo " source_location        : $SOURCE_LOC"
echo " s3_location            : $S3_LOC"
echo " mappers                : $MAPPERS"
echo " bandwidth              : $BANDWIDTH MB/s"
echo " partition_filter_active: $PARTITION_FILTER_ACTIVE"
echo " filtered_partitions    : $FILTERED_PARTITIONS_JSON"
echo " s3_opts                : ${S3_OPTS:-(none — local HDFS or no AWS creds)}"
echo " temp_dir               : $TEMP_DIR"
echo "======================================================"

# ---------------------------------------------------------------------------
# Build and run the exact cmd from run_distcp_ssh()
#
# The DAG has two branches:
#   Branch A — partition_filter_active=true  → per-partition distcp loop
#   Branch B — partition_filter_active=false → single distcp for whole table
#
# TO TEST DAG CHANGES: update the CMD heredoc in the relevant branch below
# to match the updated cmd string in migration_dag_mapr_to_s3.py, then
# re-run this script.
#
# Python f-string key      →  shell variable
#   {source_profile}       →  "source ~/.profile 2>/dev/null || true\n"  (already sourced above)
#   {s3_opts}              →  $S3_OPTS
#   {s3_loc}               →  $S3_LOC
#   {source_loc}           →  $SOURCE_LOC
#   {mappers}              →  $MAPPERS
#   {bandwidth}            →  $BANDWIDTH
#   {tbl}                  →  $TBL
#   {temp_dir}             →  $TEMP_DIR
#   {len(filtered_partitions)} → derived from $FILTERED_PARTITIONS_JSON
# ---------------------------------------------------------------------------

if [ "$PARTITION_FILTER_ACTIVE" = "true" ]; then

    # -----------------------------------------------------------------------
    # Branch A: partition_filter_active=true
    # DAG iterates filtered_partitions and emits one distcp call per partition.
    # We replicate this with a Python one-liner to parse the JSON array and
    # emit the same per-partition shell block the DAG's f-string loop builds.
    # -----------------------------------------------------------------------
    echo ""
    echo ">>> Mode: PARTITION FILTER ACTIVE — running per-partition distcp"

    # Parse the JSON array into a bash array using Python (available in the image)
    PARTITIONS=$(python3 -c "
import json, sys
parts = json.loads(sys.argv[1])
for p in parts:
    print(p)
" "$FILTERED_PARTITIONS_JSON" 2>/dev/null || python3 -c "
import json, sys
parts = json.loads(sys.argv[1])
for p in parts:
    print(p)
" "$FILTERED_PARTITIONS_JSON")

    NUM_PARTITIONS=$(echo "$PARTITIONS" | grep -c . || echo 0)

    # Build the distcp_calls block — same as the DAG's loop:
    #   for part_idx, (src_part, dst_part) in enumerate(partition_copy_pairs):
    #       distcp_calls += f"""
    #       echo "=== Copying partition: {src_part} -> {dst_part} ==="
    #       hadoop distcp{s3_opts} -update -delete -m {mappers} -bandwidth {bandwidth} -strategy dynamic \
    #           -log {temp_dir}/distcp_{tbl}_part{part_idx}.log \
    #           "{src_part}" "{dst_part}"
    #       """
    DISTCP_CALLS=""
    PART_IDX=0
    while IFS= read -r PART_STR; do
        [ -z "$PART_STR" ] && continue
        SRC_PART="${SOURCE_LOC}/${PART_STR}"
        DST_PART="${S3_LOC}/${PART_STR}"
        DISTCP_CALLS="${DISTCP_CALLS}
echo \"=== Copying partition: ${SRC_PART} -> ${DST_PART} ===\"
hadoop distcp -Dmapreduce.framework.name=local${S3_OPTS} -update -delete -m ${MAPPERS} -bandwidth ${BANDWIDTH} -strategy dynamic \\
    -log ${TEMP_DIR}/distcp_${TBL}_part${PART_IDX}.log \\
    \"${SRC_PART}\" \"${DST_PART}\"
"
        PART_IDX=$((PART_IDX + 1))
    done <<< "$PARTITIONS"

    # -----------------------------------------------------------------------
    # THE CMD — exact replica of the partition_filter_active=true branch in
    # run_distcp_ssh(). Double braces {{ }} in the DAG become single { } here.
    # -----------------------------------------------------------------------
    CMD=$(cat << CMD_EOF
set -e

calculate_s3_metrics_hadoop() {
    local location=\$1
    if ! hadoop fs${S3_OPTS} -test -d "\$location" 2>/dev/null; then
        echo "S3_FILE_COUNT=0"
        echo "S3_TOTAL_SIZE=0"
        return
    fi
    FILE_COUNT=\$(hadoop fs${S3_OPTS} -ls -R "\$location" 2>/dev/null | grep '^-' | wc -l)
    TOTAL_SIZE=\$(hadoop fs${S3_OPTS} -du -s "\$location" 2>/dev/null | awk '{print \$1}')
    [ -z "\$FILE_COUNT" ] && FILE_COUNT=0
    [ -z "\$TOTAL_SIZE" ] && TOTAL_SIZE=0
    echo "S3_FILE_COUNT=\$FILE_COUNT"
    echo "S3_TOTAL_SIZE=\$TOTAL_SIZE"
}

INCR=false
hadoop fs${S3_OPTS} -test -d ${S3_LOC} 2>/dev/null && INCR=true
echo "INCREMENTAL=\$INCR"

echo "=== Calculating S3 metrics BEFORE distcp ==="
S3_BEFORE=\$(calculate_s3_metrics_hadoop "${S3_LOC}")
S3_FILE_COUNT_BEFORE=\$(echo "\$S3_BEFORE" | grep "S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_BEFORE=\$(echo "\$S3_BEFORE" | grep "S3_TOTAL_SIZE=" | cut -d'=' -f2)
echo "S3_FILE_COUNT_BEFORE=\$S3_FILE_COUNT_BEFORE"
echo "S3_TOTAL_SIZE_BEFORE=\$S3_TOTAL_SIZE_BEFORE"

echo "=== Running distcp per-partition ==="
${DISTCP_CALLS}
echo "DISTCP_EXIT_CODE=0"

echo "=== Calculating S3 metrics AFTER distcp ==="
S3_AFTER=\$(calculate_s3_metrics_hadoop "${S3_LOC}")
S3_FILE_COUNT_AFTER=\$(echo "\$S3_AFTER" | grep "S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_AFTER=\$(echo "\$S3_AFTER" | grep "S3_TOTAL_SIZE=" | cut -d'=' -f2)
echo "S3_FILE_COUNT_AFTER=\$S3_FILE_COUNT_AFTER"
echo "S3_TOTAL_SIZE_AFTER=\$S3_TOTAL_SIZE_AFTER"

S3_FILES_TRANSFERRED=\$((S3_FILE_COUNT_AFTER - S3_FILE_COUNT_BEFORE))
S3_BYTES_TRANSFERRED=\$((S3_TOTAL_SIZE_AFTER - S3_TOTAL_SIZE_BEFORE))
echo "S3_FILES_TRANSFERRED=\$S3_FILES_TRANSFERRED"
echo "S3_BYTES_TRANSFERRED=\$S3_BYTES_TRANSFERRED"
echo "PARTITIONS_REQUESTED=${NUM_PARTITIONS}"
exit 0
CMD_EOF
)

else

    # -----------------------------------------------------------------------
    # Branch B: partition_filter_active=false
    # DAG runs a single distcp for the whole table.
    # -----------------------------------------------------------------------
    echo ""
    echo ">>> Mode: FULL TABLE — running single distcp"

    # -----------------------------------------------------------------------
    # THE CMD — exact replica of the partition_filter_active=false branch in
    # run_distcp_ssh(). Double braces {{ }} in the DAG become single { } here.
    # -----------------------------------------------------------------------
    CMD=$(cat << CMD_EOF
set -e

calculate_s3_metrics_hadoop() {
    local location=\$1

    if ! hadoop fs${S3_OPTS} -test -d "\$location" 2>/dev/null; then
        echo "S3_FILE_COUNT=0"
        echo "S3_TOTAL_SIZE=0"
        return
    fi

    FILE_COUNT=\$(hadoop fs${S3_OPTS} -ls -R "\$location" 2>/dev/null | grep '^-' | wc -l)
    TOTAL_SIZE=\$(hadoop fs${S3_OPTS} -du -s "\$location" 2>/dev/null | awk '{print \$1}')
    [ -z "\$FILE_COUNT" ] && FILE_COUNT=0
    [ -z "\$TOTAL_SIZE" ] && TOTAL_SIZE=0

    echo "S3_FILE_COUNT=\$FILE_COUNT"
    echo "S3_TOTAL_SIZE=\$TOTAL_SIZE"
}

INCR=false
hadoop fs${S3_OPTS} -test -d ${S3_LOC} 2>/dev/null && INCR=true
echo "INCREMENTAL=\$INCR"

echo "=== Calculating S3 metrics BEFORE distcp ==="
S3_BEFORE=\$(calculate_s3_metrics_hadoop "${S3_LOC}")

S3_FILE_COUNT_BEFORE=\$(echo "\$S3_BEFORE" | grep "S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_BEFORE=\$(echo "\$S3_BEFORE" | grep "S3_TOTAL_SIZE=" | cut -d'=' -f2)

echo "S3_FILE_COUNT_BEFORE=\$S3_FILE_COUNT_BEFORE"
echo "S3_TOTAL_SIZE_BEFORE=\$S3_TOTAL_SIZE_BEFORE"

echo "=== Running distcp ==="
DISTCP_LOG_FILE="${TEMP_DIR}/distcp_${TBL}.log"
set +e
hadoop distcp -Dmapreduce.framework.name=local${S3_OPTS} -update -delete -m ${MAPPERS} -bandwidth ${BANDWIDTH} -strategy dynamic -log ${TEMP_DIR}/distcp_${TBL}_joblog "${SOURCE_LOC}" "${S3_LOC}" 2>&1 | tee "\$DISTCP_LOG_FILE"
DISTCP_EXIT=\${PIPESTATUS[0]}
set -e
echo "DISTCP_EXIT_CODE=\$DISTCP_EXIT"

BYTES_COPIED=\$(grep "Bytes Copied=" "\$DISTCP_LOG_FILE" 2>/dev/null | sed 's/.*Bytes Copied=\([0-9]*\).*/\1/' | tail -1)
FILES_COPIED=\$(grep "Files Copied=" "\$DISTCP_LOG_FILE" 2>/dev/null | sed 's/.*Files Copied=\([0-9]*\).*/\1/' | tail -1)

[ -z "\$BYTES_COPIED" ] && BYTES_COPIED=0
[ -z "\$FILES_COPIED" ] && FILES_COPIED=0

echo "BYTES_COPIED=\$BYTES_COPIED"
echo "FILES_COPIED=\$FILES_COPIED"

echo "=== Calculating S3 metrics AFTER distcp ==="
S3_AFTER=\$(calculate_s3_metrics_hadoop "${S3_LOC}")

S3_FILE_COUNT_AFTER=\$(echo "\$S3_AFTER" | grep "S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_AFTER=\$(echo "\$S3_AFTER" | grep "S3_TOTAL_SIZE=" | cut -d'=' -f2)

echo "S3_FILE_COUNT_AFTER=\$S3_FILE_COUNT_AFTER"
echo "S3_TOTAL_SIZE_AFTER=\$S3_TOTAL_SIZE_AFTER"

S3_FILES_TRANSFERRED=\$((S3_FILE_COUNT_AFTER - S3_FILE_COUNT_BEFORE))
S3_BYTES_TRANSFERRED=\$((S3_TOTAL_SIZE_AFTER - S3_TOTAL_SIZE_BEFORE))

echo "S3_FILES_TRANSFERRED=\$S3_FILES_TRANSFERRED"
echo "S3_BYTES_TRANSFERRED=\$S3_BYTES_TRANSFERRED"

[ "\$DISTCP_EXIT" -ne 0 ] && exit \$DISTCP_EXIT
exit 0
CMD_EOF
)

fi

# ---------------------------------------------------------------------------
# Execute the cmd — same as: client.exec_command(cmd, timeout=SSH_COMMAND_TIMEOUT)
# ---------------------------------------------------------------------------
echo ""
echo ">>> Executing cmd (same as DAG's client.exec_command over SSH)..."
echo "---"
eval "$CMD"
EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "======================================================"
    echo " DISTCP TEST PASSED (exit 0)"
    echo " DistCp logs saved to: $TEMP_DIR/"
    echo "======================================================"
else
    echo "======================================================"
    echo " DISTCP TEST FAILED (exit $EXIT_CODE)"
    echo " Check output above. DistCp logs: $TEMP_DIR/"
    echo "======================================================"
    exit $EXIT_CODE
fi