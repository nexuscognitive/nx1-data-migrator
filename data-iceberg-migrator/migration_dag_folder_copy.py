"""
DAG 3: Folder-Only Data Copy

Copies folders from MapR/HDFS to S3 via DistCp — no Hive metadata.
For migrating raw data directories that don't have associated Hive tables.

Excel columns: source_path | target_bucket | dest_folder | endpoint
"""

import contextlib
import logging
import os
import re
from datetime import datetime, timedelta
from html import escape as _html_escape
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.ssh.hooks.ssh import SSHHook
from dotenv import load_dotenv
from migrator_utils.migrations.shared import (
    SSH_COMMAND_TIMEOUT,
    _login_shell,
    build_s3_opts,
    cluster_login,
    execute_with_iceberg_retry,
    get_config,
    is_permanent_error,
    normalize_s3,
    permanent_fail,
)

_dag_stem = Path(__file__).stem
logger = logging.getLogger(__name__)

_dag_dir = Path(__file__).resolve().parent
_config_dir = str(_dag_dir / 'migrator_utils' / 'migration_configs')
if os.path.isdir(_config_dir):
    load_dotenv(os.path.join(_config_dir, 'env.shared'))
    load_dotenv(os.path.join(_config_dir, f'env.{_dag_stem}'), override=True)
else:
    logger.warning(f"Config directory {_config_dir} not found — env files not loaded, using Airflow Variables / defaults")

def _resolve_dag_owner() -> str:
    """Owner shown in the Airflow UI, fixed at DAG parse time.

    Not read from an Airflow Variable: parse time has no run, so it cannot tell
    a portal-triggered run from a hand-launched one, and reading the portal's
    value here pinned every manual run to the last portal user. deploy.py
    rewrites this literal via --owner. The per-run owner comes from
    dag_run.conf and is resolved in get_config().
    """
    return 'data-migration'

default_args = {
    "owner": _resolve_dag_owner(),
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def _folder_copy_dag_failure_callback(context) -> None:
    try:
        dag_run = None
        task_instance = None
        try:
            dag_run = context.get('dag_run')
            task_instance = context.get('task_instance')
        except Exception:
            pass
        run_ref = getattr(dag_run, 'run_id', None)
        task_ref = getattr(task_instance, 'task_id', None)
        logger.error(
            f"[FolderCopy] DAG failure callback — dag_run_id={run_ref} task_id={task_ref}"
        )

        tracking_run_id = None
        try:
            creator_ti = dag_run.get_task_instance('create_data_copy_run')
            if creator_ti is not None:
                tracking_run_id = creator_ti.xcom_pull(
                    task_ids='create_data_copy_run', key='return_value'
                )
        except Exception as e:
            logger.warning(
                f"[FolderCopy] Failure callback could not read the tracking run_id XCom: {e}"
            )
        if not tracking_run_id:
            logger.warning(
                "[FolderCopy] Failure callback found no tracking run_id — "
                "the run failed before create_data_copy_run wrote a record."
            )
            return

        logger.warning(
            f"[FolderCopy] Run {tracking_run_id} ended without a report. "
            f"data_copy_runs still shows status RUNNING for {tracking_run_id} — "
            f"re-run the DAG or reconcile the tracking row manually."
        )
    except Exception:
        with contextlib.suppress(Exception):
            logger.error("[FolderCopy] DAG failure callback raised and was suppressed")


@task
def cluster_login_setup(run_id: str) -> dict:
    """SSH to edge, perform cluster login (MapR or Kerberos), create temp dir."""
    return cluster_login(run_id)

# =============================================================================
# DAG 3: FOLDER-ONLY DATA COPY TASKS
# =============================================================================

@task
def validate_prerequisites_folder_copy() -> dict:
    """Validate SSH connectivity and Hadoop DistCp availability before starting the folder copy."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])

    checks = {
        'ssh_connectivity': False,
        'hadoop_distcp_available': False,
        'hadoop_fs_available': False,
    }
    errors = []

    logger.info("=" * 60)
    logger.info("[FolderCopy] STARTING PRE-DAG VALIDATION")
    logger.info("=" * 60)

    try:
        with ssh.get_conn() as client:

            # 1. SSH connectivity
            logger.info("[1/3] Testing SSH connectivity...")
            _, stdout, stderr = client.exec_command(_login_shell('echo "SSH_TEST_OK"', config.get('cluster_type', 'MapR')), timeout=30)
            output = stdout.read().decode()
            stderr.read()
            exit_code = stdout.channel.recv_exit_status()
            if exit_code == 0 and "SSH_TEST_OK" in output:
                checks['ssh_connectivity'] = True
                logger.info("SSH connectivity: PASSED")
            else:
                msg = f"SSH command failed with exit code {exit_code}"
                errors.append(f"SSH: {msg}")
                logger.error(f"SSH connectivity: FAILED - {msg}")

            # 2. Hadoop DistCp
            logger.info("[2/3] Testing hadoop distcp availability...")
            test_cmd = "hadoop distcp 2>&1 | grep -qi 'usage\\|distcp\\|options' && echo DISTCP_OK || echo DISTCP_FAIL"
            _, stdout, stderr = client.exec_command(_login_shell(test_cmd, config.get('cluster_type', 'MapR')), timeout=60)
            output = stdout.read().decode()
            stderr.read()
            exit_code = stdout.channel.recv_exit_status()
            if "DISTCP_OK" in output:
                checks['hadoop_distcp_available'] = True
                logger.info("Hadoop DistCp: PASSED")
            else:
                msg = "hadoop distcp not found or not executable"
                errors.append(f"DistCp: {msg}")
                logger.error(f"Hadoop DistCp: FAILED - {msg}")

            # 3. Hadoop FS
            logger.info("[3/3] Testing Hadoop FS commands...")
            hdfs_nameservice = config.get('hdfs_nameservice', '')
            fs_root = f"hdfs://{hdfs_nameservice}/" if hdfs_nameservice else "/"
            test_cmd = f"if hadoop fs -ls {fs_root} > /dev/null 2>&1; then echo HADOOP_FS_OK; else echo HADOOP_FS_FAIL; fi"
            _, stdout, stderr = client.exec_command(_login_shell(test_cmd, config.get('cluster_type', 'MapR')), timeout=60)
            output = stdout.read().decode()
            stderr.read()
            exit_code = stdout.channel.recv_exit_status()
            if "HADOOP_FS_OK" in output:
                checks['hadoop_fs_available'] = True
                logger.info("Hadoop FS: PASSED")
            else:
                msg = "hadoop fs -ls / failed"
                errors.append(f"Hadoop FS: {msg}")
                logger.error(f"Hadoop FS: FAILED - {msg}")

    except Exception as e:
        msg = f"SSH connection failed: {str(e)}"
        errors.append(f"SSH: {msg}")
        errors.append("DistCp: Skipped due to SSH failure")
        errors.append("Hadoop FS: Skipped due to SSH failure")
        logger.error(f"SSH connectivity: FAILED - {msg}")

    logger.info("=" * 60)
    logger.info("[FolderCopy] VALIDATION SUMMARY")
    logger.info("=" * 60)

    if errors:
        logger.error("SOME PRE-DAG CHECKS FAILED:")
        for e in errors:
            logger.warning(f"  - {e}")
        raise Exception(
            f"Pre-DAG validation failed — {len(errors)} check(s) failed:\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    logger.info("ALL PRE-DAG CHECKS PASSED")
    return checks


@task.pyspark(conn_id='spark_default')
def init_folder_copy_tracking_tables(spark) -> dict:
    """Create tracking tables for folder-only data copy if they don't exist."""
    config = get_config()
    tracking_db = config['tracking_database']
    tracking_loc = config['tracking_location']
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'")
    # Run-level table: one row per DAG run
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.data_copy_runs (
            run_id          STRING,
            excel_file_path STRING,
            started_at      TIMESTAMP,
            completed_at    TIMESTAMP,
            status          STRING,
            total_folders   INT,
            successful_folders INT,
            failed_folders  INT,
            error_message   STRING,
            created_at      TIMESTAMP,
            dag_run_id      STRING,
            service_account_user_id STRING,
            service_account_source  STRING,
            skipped_folders INT
        )
        USING iceberg
        LOCATION '{tracking_loc}/data_copy_runs'
    """)
    # Folder-level table: one row per source/dest folder pair
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.data_copy_status (
            run_id            STRING,
            source_path       STRING,
            dest_bucket       STRING,
            dest_path         STRING,
            status            STRING,
            started_at        TIMESTAMP,
            completed_at      TIMESTAMP,
            source_file_count BIGINT,
            source_size_bytes BIGINT,
            dest_file_count   BIGINT,
            dest_size_bytes   BIGINT,
            files_copied      BIGINT,
            bytes_copied      BIGINT,
            is_incremental    BOOLEAN,
            file_count_match  BOOLEAN,
            size_match        BOOLEAN,
            error_message     STRING,
            updated_at        TIMESTAMP,
            yarn_application_id STRING,
            distcp_started_at   TIMESTAMP,
            distcp_completed_at TIMESTAMP,
            distcp_duration_seconds DOUBLE,
            distcp_bytes_copied BIGINT,
            distcp_files_copied BIGINT,
            throughput_mbps     DOUBLE
        )
        USING iceberg
        LOCATION '{tracking_loc}/data_copy_status'
    """)

    for _col_name, _col_type in (
        ("yarn_application_id", "STRING"),
        ("distcp_started_at", "TIMESTAMP"),
        ("distcp_completed_at", "TIMESTAMP"),
        ("distcp_duration_seconds", "DOUBLE"),
        ("distcp_bytes_copied", "BIGINT"),
        ("distcp_files_copied", "BIGINT"),
        ("throughput_mbps", "DOUBLE"),
    ):
        try:
            spark.sql(
                f"ALTER TABLE {tracking_db}.data_copy_status "
                f"ADD COLUMN {_col_name} {_col_type}"
            )
            logger.info(f"[FolderCopy] Added column {_col_name} to data_copy_status")
        except Exception as _alter_exc:
            logger.debug(
                f"[FolderCopy] ALTER TABLE {tracking_db}.data_copy_status "
                f"ADD COLUMN {_col_name} {_col_type} did not apply: {_alter_exc}"
            )

    for _col_name, _col_type in (
        ("dag_run_id", "STRING"),
        ("service_account_user_id", "STRING"),
        ("service_account_source", "STRING"),
        ("skipped_folders", "INT"),
    ):
        try:
            spark.sql(
                f"ALTER TABLE {tracking_db}.data_copy_runs "
                f"ADD COLUMN {_col_name} {_col_type}"
            )
            logger.info(f"[FolderCopy] Added column {_col_name} to data_copy_runs")
        except Exception as _alter_exc:
            logger.debug(
                f"[FolderCopy] ALTER TABLE {tracking_db}.data_copy_runs "
                f"ADD COLUMN {_col_name} {_col_type} did not apply: {_alter_exc}"
            )

    for _table_name, _expected in (
        ('data_copy_status', (
            'yarn_application_id',
            'distcp_started_at',
            'distcp_completed_at',
            'distcp_duration_seconds',
            'distcp_bytes_copied',
            'distcp_files_copied',
            'throughput_mbps',
        )),
        ('data_copy_runs', (
            'dag_run_id',
            'service_account_user_id',
            'service_account_source',
            'skipped_folders',
        )),
    ):
        try:
            described = spark.sql(f"DESCRIBE {tracking_db}.{_table_name}").collect()
            present = set()
            for _row in described:
                try:
                    present.add(str(_row['col_name']).strip().lower())
                except Exception:
                    continue
            missing = sorted(c for c in _expected if c.lower() not in present)
        except Exception as _desc_exc:
            logger.debug(
                f"[FolderCopy] Could not DESCRIBE {tracking_db}.{_table_name}: {_desc_exc}"
            )
            missing = []
        if missing:
            logger.warning(
                f"[FolderCopy] Tracking table {tracking_db}.{_table_name} is missing "
                f"expected column(s) after schema evolution: {', '.join(missing)}. "
                f"Subsequent INSERT/UPDATE statements referencing them will fail — "
                f"check ALTER TABLE privileges on {tracking_db}."
            )

    logger.info("[FolderCopy] Tracking tables initialized: data_copy_runs, data_copy_status")
    return {'status': 'initialized', 'database': tracking_db}


@task.pyspark(conn_id='spark_default')
def create_data_copy_run(excel_file_path: str, spark, dag_run_id: str = '') -> str:
    """Create a run record in data_copy_runs at the start of a folder-only copy DAG run."""
    import uuid
    from datetime import datetime
    config = get_config()
    tracking_db = config['tracking_database']
    run_id = f"folder_run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    sa_user = str(config.get('service_account_user_id') or '').strip()
    sa_source = 'config:service_account_user_id' if sa_user else 'pending (.profile fallback)'
    esc_excel = str(excel_file_path or '').replace("'", "''")
    esc_dag_run_id = str(dag_run_id or '').replace("'", "''")
    esc_sa_user = sa_user.replace("'", "''")
    esc_sa_source = sa_source.replace("'", "''")

    spark.sql(f"""
        INSERT INTO {tracking_db}.data_copy_runs (
            run_id, excel_file_path, started_at, completed_at,
            status, total_folders, successful_folders, failed_folders,
            error_message, created_at,
            dag_run_id, service_account_user_id, service_account_source,
            skipped_folders
        ) VALUES (
            '{run_id}',
            '{esc_excel}',
            current_timestamp(),
            NULL,
            'RUNNING',
            NULL, NULL, NULL, NULL,
            current_timestamp(),
            '{esc_dag_run_id}',
            '{esc_sa_user}',
            '{esc_sa_source}',
            NULL
        )
    """)
    logger.info(f"[FolderCopy] Created run record: {run_id}")
    return run_id


@task.pyspark(conn_id='spark_default')
def parse_folder_copy_excel(excel_file_path: str, run_id: str, spark) -> list:
    """Read folder copy Excel config from S3.

    Expected columns:
      - source_path   (required) : Full source cluster path (MapR-FS or HDFS)
      - target_bucket (required) : S3 bucket, normalised to s3a://
      - dest_folder   (optional) : Destination folder inside the bucket;
                                   defaults to the basename of source_path
      - endpoint      (optional) : S3 endpoint URL for non-default tenants;
                                   credentials looked up via <hostname>_access_key/secret_key Variables
    Returns a list of dicts, one per valid row.
    """
    import os
    from io import BytesIO

    import pandas as ps

    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    excel_bytes = bytes(row.content)
    df = ps.read_excel(BytesIO(excel_bytes), engine='openpyxl')

    # Normalise column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    configs = []
    skipped = 0
    for _, row in df.iterrows():
        source_path = str(row.get('source_path', '') or '').strip()
        if not source_path:
            skipped += 1
            continue

        raw_bucket = str(row.get('target_bucket', '') or '').strip()
        if not raw_bucket or raw_bucket.lower() in ('nan', 'none'):
            logger.warning(f"[FolderCopy] Skipping row — missing target_bucket for source_path={source_path!r}")
            skipped += 1
            continue

        if re.match(r'(?i)^s3:(?!//)', raw_bucket):
            raw_bucket = raw_bucket[3:].strip()

        dest_bucket = normalize_s3(raw_bucket)
        scheme, _, bucket_body = dest_bucket.partition('://')
        bucket_body = re.sub(r'/{2,}', '/', bucket_body).strip('/')
        dest_bucket = f"{scheme}://{bucket_body}" if bucket_body else dest_bucket.rstrip('/')

        raw_val = row.get('dest_folder', '')
        dest_folder = '' if ps.isna(raw_val) else str(raw_val).strip().strip('/')
        dest_folder = re.sub(r'/{2,}', '/', dest_folder).strip('/')
        dest_folder = dest_folder or os.path.basename(source_path.rstrip('/'))

        if dest_folder and dest_bucket.endswith(f"/{dest_folder}"):
            dest_bucket = dest_bucket[: -(len(dest_folder) + 1)].rstrip('/')

        raw_endpoint = row.get('endpoint', '')
        dest_endpoint = '' if (raw_endpoint is None or str(raw_endpoint).strip().lower() in ('', 'nan', 'none')) else str(raw_endpoint).strip()

        config_entry = {
            'run_id': run_id,
            'source_path': source_path,
            'dest_bucket': dest_bucket,
            'dest_folder': dest_folder,
            'dest_endpoint': dest_endpoint,
        }
        logger.info(
            f"[FolderCopy] Parsed: {source_path} -> {dest_bucket}/{dest_folder}"
        )
        configs.append(config_entry)

    logger.info(
        f"[FolderCopy] parse_folder_copy_excel: {len(configs)} folders to copy, {skipped} rows skipped"
    )
    if not configs:
        raise ValueError("[FolderCopy] No valid rows found in Excel — check source_path and target_bucket columns")

    return configs


@task
def run_folder_distcp_ssh(folder_config: dict, cluster_setup: dict = None, **context) -> dict:
    """Copy a single source folder to S3 via SSH DistCp with -update for incremental runs."""
    from datetime import datetime as _dt

    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])

    run_id = folder_config['run_id']
    source_path = folder_config['source_path']
    dest_bucket = folder_config['dest_bucket']
    dest_folder = folder_config['dest_folder']
    dest_endpoint = folder_config.get('dest_endpoint', '')
    s3_dest = f"{dest_bucket}/{dest_folder}"

    mappers = config['distcp_mappers']
    bandwidth = config['distcp_bandwidth']

    cluster_setup = cluster_setup if isinstance(cluster_setup, dict) else {}
    distcp_log_dir = (
        cluster_setup.get('distcp_log_dir')
        or cluster_setup.get('temp_dir')
        or '/tmp'
    )
    log_name = re.sub(r'[^A-Za-z0-9_.-]', '_', str(dest_folder or 'folder')).strip('_') or 'folder'

    allow_delete = str(config.get('folder_copy_allow_delete')).strip().lower() in (
        'true', '1', 'yes', 'on'
    )

    delete_flag = ''
    if config.get('distcp_preserve_delete') is True and allow_delete:
        delete_flag = ' -delete'

    s3_opts = build_s3_opts(dest_bucket, config, dest_endpoint)

    cmd = f'''set +e

calculate_s3_metrics() {{
    local location=$1
    if ! hadoop fs{s3_opts} -test -d "$location" 2>/dev/null; then
        echo "S3_FILE_COUNT=0"
        echo "S3_TOTAL_SIZE=0"
        return
    fi
    FILE_COUNT=$(hadoop fs{s3_opts} -ls -R "$location" 2>/dev/null | grep '^-' | wc -l)
    TOTAL_SIZE=$(hadoop fs{s3_opts} -du -s "$location" 2>/dev/null | awk '{{print $1}}')
    [ -z "$FILE_COUNT" ] && FILE_COUNT=0
    [ -z "$TOTAL_SIZE" ] && TOTAL_SIZE=0
    echo "S3_FILE_COUNT=$FILE_COUNT"
    echo "S3_TOTAL_SIZE=$TOTAL_SIZE"
}}

emit_metrics() {{
    echo "===DISTCP_METRICS_START==="
    echo "INCREMENTAL=$1"
    echo "SRC_FILE_COUNT=$2"
    echo "SRC_TOTAL_SIZE=$3"
    echo "S3_FILE_COUNT_BEFORE=$4"
    echo "S3_TOTAL_SIZE_BEFORE=$5"
    echo "S3_FILE_COUNT_AFTER=$6"
    echo "S3_TOTAL_SIZE_AFTER=$7"
    echo "BYTES_COPIED=$8"
    echo "FILES_COPIED=$9"
    echo "===DISTCP_METRICS_END==="
}}

echo "=== Source existence check ==="
SRC_TEST_ERR=$(hadoop fs -test -d "{source_path}" 2>&1)
SRC_TEST_RC=$?
echo "SOURCE_TEST_RC=$SRC_TEST_RC"
if [ "$SRC_TEST_RC" -eq 1 ]; then
    echo "SOURCE_NOT_FOUND=true"
    emit_metrics false 0 0 0 0 0 0 0 0
    exit 0
elif [ "$SRC_TEST_RC" -ne 0 ]; then
    echo "SOURCE_TEST_ERROR=$SRC_TEST_ERR"
    exit $SRC_TEST_RC
fi

INCR=false
hadoop fs{s3_opts} -test -d "{s3_dest}" 2>/dev/null && INCR=true
echo "INCREMENTAL=$INCR"

echo "=== S3 metrics BEFORE distcp ==="
S3_BEFORE=$(calculate_s3_metrics "{s3_dest}")
S3_FILE_COUNT_BEFORE=$(echo "$S3_BEFORE" | grep "S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_BEFORE=$(echo "$S3_BEFORE" | grep "S3_TOTAL_SIZE=" | cut -d'=' -f2)
[ -z "$S3_FILE_COUNT_BEFORE" ] && S3_FILE_COUNT_BEFORE=0
[ -z "$S3_TOTAL_SIZE_BEFORE" ] && S3_TOTAL_SIZE_BEFORE=0
echo "S3_FILE_COUNT_BEFORE=$S3_FILE_COUNT_BEFORE"
echo "S3_TOTAL_SIZE_BEFORE=$S3_TOTAL_SIZE_BEFORE"

echo "=== Source metrics ==="
SRC_FILE_COUNT=$(hadoop fs -ls -R "{source_path}" 2>/dev/null | grep '^-' | wc -l)
SRC_TOTAL_SIZE=$(hadoop fs -du -s "{source_path}" 2>/dev/null | awk '{{print $1}}')
[ -z "$SRC_FILE_COUNT" ] && SRC_FILE_COUNT=0
[ -z "$SRC_TOTAL_SIZE" ] && SRC_TOTAL_SIZE=0
echo "SRC_FILE_COUNT=$SRC_FILE_COUNT"
echo "SRC_TOTAL_SIZE=$SRC_TOTAL_SIZE"

if [ "$SRC_FILE_COUNT" -eq 0 ]; then
    echo "EMPTY_SOURCE=true"
    hadoop fs{s3_opts} -mkdir -p "{s3_dest}" 2>/dev/null || true
    emit_metrics "$INCR" 0 "$SRC_TOTAL_SIZE" "$S3_FILE_COUNT_BEFORE" "$S3_TOTAL_SIZE_BEFORE" "$S3_FILE_COUNT_BEFORE" "$S3_TOTAL_SIZE_BEFORE" 0 0
    exit 0
fi

echo "=== Running distcp ==="
DISTCP_OUTPUT=$(hadoop distcp{s3_opts} -update{delete_flag} -m {mappers} -bandwidth {bandwidth} -strategy dynamic \\
    -log {distcp_log_dir}/distcp_{log_name}.log "{source_path}" "{s3_dest}" 2>&1)
DISTCP_EXIT=$?
echo "$DISTCP_OUTPUT"
echo "DISTCP_EXIT_CODE=$DISTCP_EXIT"

BYTES_COPIED=$(echo "$DISTCP_OUTPUT" | grep -i "Bytes Copied" | grep -oE '[0-9,]+' | tail -1 | tr -d ',')
FILES_COPIED=$(echo "$DISTCP_OUTPUT" | grep -iE "Files Copied|Number of files copied" | grep -oE '[0-9,]+' | tail -1 | tr -d ',')
[ -z "$BYTES_COPIED" ] && BYTES_COPIED=0
[ -z "$FILES_COPIED" ] && FILES_COPIED=0

echo "=== S3 metrics AFTER distcp ==="
S3_AFTER=$(calculate_s3_metrics "{s3_dest}")
S3_FILE_COUNT_AFTER=$(echo "$S3_AFTER" | grep "S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_AFTER=$(echo "$S3_AFTER" | grep "S3_TOTAL_SIZE=" | cut -d'=' -f2)
[ -z "$S3_FILE_COUNT_AFTER" ] && S3_FILE_COUNT_AFTER=0
[ -z "$S3_TOTAL_SIZE_AFTER" ] && S3_TOTAL_SIZE_AFTER=0
echo "S3_FILE_COUNT_AFTER=$S3_FILE_COUNT_AFTER"
echo "S3_TOTAL_SIZE_AFTER=$S3_TOTAL_SIZE_AFTER"

emit_metrics "$INCR" "$SRC_FILE_COUNT" "$SRC_TOTAL_SIZE" "$S3_FILE_COUNT_BEFORE" "$S3_TOTAL_SIZE_BEFORE" "$S3_FILE_COUNT_AFTER" "$S3_TOTAL_SIZE_AFTER" "$BYTES_COPIED" "$FILES_COPIED"

[ "$DISTCP_EXIT" -ne 0 ] && exit $DISTCP_EXIT
exit 0
'''

    started_at = _dt.utcnow()
    started_at_str = started_at.strftime('%Y-%m-%d %H:%M:%S')
    distcp_reported = False

    try:
        with ssh.get_conn() as client:
            _, stdout, stderr = client.exec_command(
                _login_shell(cmd, config.get('cluster_type', 'MapR')),
                timeout=SSH_COMMAND_TIMEOUT,
                get_pty=True,
            )
            output = stdout.read().decode()
            error_output = stderr.read().decode()
            exit_code = stdout.channel.recv_exit_status()

            logger.info(f"=== [FolderDistCp] {source_path} -> {s3_dest} (FULL stdout) ===")
            logger.info(output)
            if error_output.strip():
                logger.info(f"=== [FolderDistCp] {source_path} -> {s3_dest} (FULL stderr) ===")
                logger.info(error_output)

            combined_output = output + "\n" + error_output
            yarn_application_ids = re.findall(r"application_\d+_\d+", combined_output)
            yarn_application_ids = list(dict.fromkeys(yarn_application_ids))
            yarn_application_id = yarn_application_ids[-1] if yarn_application_ids else None
            if yarn_application_ids:
                logger.info(
                    f"[FolderDistCp] YARN Application IDs for {source_path}: {yarn_application_ids}"
                )
                logger.info(
                    f"[FolderDistCp] Last YARN Application ID for {source_path}: {yarn_application_id}"
                )
            else:
                logger.warning(
                    f"[FolderDistCp] No YARN Application ID found for {source_path}"
                )

            metrics = {
                'INCREMENTAL': 'false',
                'SRC_FILE_COUNT': 0,
                'SRC_TOTAL_SIZE': 0,
                'S3_FILE_COUNT_BEFORE': 0,
                'S3_TOTAL_SIZE_BEFORE': 0,
                'S3_FILE_COUNT_AFTER': 0,
                'S3_TOTAL_SIZE_AFTER': 0,
                'BYTES_COPIED': 0,
                'FILES_COPIED': 0,
            }
            m_start = output.find("===DISTCP_METRICS_START===")
            m_end = output.find("===DISTCP_METRICS_END===")
            if m_start != -1 and m_end != -1:
                metrics_block = output[m_start + len("===DISTCP_METRICS_START==="):m_end]
            else:
                logger.warning(
                    f"[FolderDistCp] Metrics block not found for {source_path} — "
                    f"falling back to scanning full output"
                )
                metrics_block = output
            for line in metrics_block.splitlines():
                line = line.strip()
                if '=' not in line:
                    continue
                key, _, val = line.partition('=')
                key = key.strip()
                if key not in metrics:
                    continue
                val = val.strip()
                if key == 'INCREMENTAL':
                    metrics[key] = val
                else:
                    try:
                        metrics[key] = int(val or 0)
                    except ValueError:
                        logger.warning(
                            f"[FolderDistCp] Could not parse metric '{key}={val}' for {source_path}"
                        )

            completed_at = _dt.utcnow()
            duration = max((completed_at - started_at).total_seconds(), 0.0)
            completed_at_str = completed_at.strftime('%Y-%m-%d %H:%M:%S')

            source_test_rc = None
            source_test_error = ''
            for line in output.splitlines():
                line = line.strip()
                if line.startswith('SOURCE_TEST_RC='):
                    try:
                        source_test_rc = int(line.split('=', 1)[1] or 0)
                    except ValueError:
                        source_test_rc = None
                elif line.startswith('SOURCE_TEST_ERROR='):
                    source_test_error = line.split('=', 1)[1].strip()

            if source_test_rc == 1:
                logger.error(f"[FolderDistCp] SOURCE_NOT_FOUND: {source_path}")
                return {
                    'run_id': run_id,
                    'source_path': source_path,
                    'dest_bucket': dest_bucket,
                    'dest_path': dest_folder,
                    'dest_endpoint': dest_endpoint,
                    'status': 'SOURCE_NOT_FOUND',
                    'started_at': started_at_str,
                    'completed_at': completed_at_str,
                    'source_file_count': 0,
                    'source_size_bytes': 0,
                    'dest_file_count': 0,
                    'dest_size_bytes': 0,
                    'files_copied': 0,
                    'bytes_copied': 0,
                    'is_incremental': False,
                    'file_count_match': False,
                    'size_match': False,
                    'error': f"Source path does not exist: {source_path}",
                    'yarn_application_id': yarn_application_id,
                    'yarn_application_ids': yarn_application_ids,
                    'distcp_duration_seconds': duration,
                    'distcp_bytes_copied': 0,
                    'distcp_files_copied': 0,
                    'throughput_mbps': 0.0,
                }

            is_incr = str(metrics['INCREMENTAL']).lower() == 'true'
            src_files = metrics['SRC_FILE_COUNT']
            src_bytes = metrics['SRC_TOTAL_SIZE']
            dest_files = metrics['S3_FILE_COUNT_AFTER']
            dest_bytes = metrics['S3_TOTAL_SIZE_AFTER']
            transferred_files = max(dest_files - metrics['S3_FILE_COUNT_BEFORE'], 0)
            transferred_bytes = max(dest_bytes - metrics['S3_TOTAL_SIZE_BEFORE'], 0)
            distcp_bytes = metrics['BYTES_COPIED']
            distcp_files = metrics['FILES_COPIED']
            throughput = (distcp_bytes / 1048576.0 / duration) if duration > 0 else 0.0

            if 'EMPTY_SOURCE=true' in output:
                logger.warning(
                    f"[FolderDistCp] EMPTY_SOURCE: {source_path} has 0 files — "
                    f"S3 prefix created at {s3_dest}"
                )
                return {
                    'run_id': run_id,
                    'source_path': source_path,
                    'dest_bucket': dest_bucket,
                    'dest_path': dest_folder,
                    'dest_endpoint': dest_endpoint,
                    'status': 'EMPTY_SOURCE',
                    'started_at': started_at_str,
                    'completed_at': completed_at_str,
                    'source_file_count': src_files,
                    'source_size_bytes': src_bytes,
                    'dest_file_count': dest_files,
                    'dest_size_bytes': dest_bytes,
                    'files_copied': transferred_files,
                    'bytes_copied': transferred_bytes,
                    'is_incremental': is_incr,
                    'file_count_match': False,
                    'size_match': False,
                    'error': None,
                    'yarn_application_id': yarn_application_id,
                    'yarn_application_ids': yarn_application_ids,
                    'distcp_duration_seconds': duration,
                    'distcp_bytes_copied': distcp_bytes,
                    'distcp_files_copied': distcp_files,
                    'throughput_mbps': throughput,
                }

            if exit_code != 0:
                if source_test_rc is not None and source_test_rc not in (0, 1):
                    error_msg = (
                        f"Source existence check failed (exit {source_test_rc}): "
                        f"{source_test_error or error_output[:1000] or output[-1000:]}"
                    )[:2000]
                else:
                    error_msg = (
                        f"DistCp failed for {source_path} -> {s3_dest} "
                        f"(exit {exit_code}): {error_output[:1000] or output[-1000:]}"
                    )[:2000]
                logger.error(f"[FolderDistCp] FAILED: {error_msg}")
                result = {
                    'run_id': run_id,
                    'source_path': source_path,
                    'dest_bucket': dest_bucket,
                    'dest_path': dest_folder,
                    'dest_endpoint': dest_endpoint,
                    'status': 'FAILED',
                    'started_at': started_at_str,
                    'completed_at': completed_at_str,
                    'source_file_count': src_files,
                    'source_size_bytes': src_bytes,
                    'dest_file_count': dest_files,
                    'dest_size_bytes': dest_bytes,
                    'files_copied': transferred_files,
                    'bytes_copied': transferred_bytes,
                    'is_incremental': is_incr,
                    'file_count_match': False,
                    'size_match': False,
                    'error': error_msg,
                    'yarn_application_id': yarn_application_id,
                    'yarn_application_ids': yarn_application_ids,
                    'distcp_duration_seconds': duration,
                    'distcp_bytes_copied': distcp_bytes,
                    'distcp_files_copied': distcp_files,
                    'throughput_mbps': throughput,
                }
                context['ti'].xcom_push(key='return_value', value=result)
                distcp_reported = True
                raise Exception(
                    f"DistCp failed for {source_path} -> {s3_dest}: {error_msg}"
                )

            logger.info(
                f"[FolderDistCp] COMPLETED: {source_path} -> {s3_dest} | "
                f"incremental={is_incr} | "
                f"files_copied={transferred_files} | bytes_copied={transferred_bytes} | "
                f"duration={duration:.1f}s | "
                f"throughput={throughput:.2f} MB/s"
            )
            return {
                'run_id': run_id,
                'source_path': source_path,
                'dest_bucket': dest_bucket,
                'dest_path': dest_folder,
                'dest_endpoint': dest_endpoint,
                'status': 'COMPLETED',
                'started_at': started_at_str,
                'completed_at': completed_at_str,
                'source_file_count': src_files,
                'source_size_bytes': src_bytes,
                'dest_file_count': dest_files,
                'dest_size_bytes': dest_bytes,
                'files_copied': transferred_files,
                'bytes_copied': transferred_bytes,
                'is_incremental': is_incr,
                'file_count_match': src_files == dest_files,
                'size_match': abs(src_bytes - dest_bytes) <= max(1, int(src_bytes * 0.01)),
                'error': None,
                'yarn_application_id': yarn_application_id,
                'yarn_application_ids': yarn_application_ids,
                'distcp_duration_seconds': duration,
                'distcp_bytes_copied': distcp_bytes,
                'distcp_files_copied': distcp_files,
                'throughput_mbps': throughput,
            }

    except Exception as e:
        if distcp_reported:
            raise
        completed_at = _dt.utcnow()
        error_msg = str(e)[:2000]
        logger.error(f"[FolderDistCp] ERROR: {source_path} -> {s3_dest}: {error_msg}")
        result = {
            'run_id': run_id,
            'source_path': source_path,
            'dest_bucket': dest_bucket,
            'dest_path': dest_folder,
            'dest_endpoint': dest_endpoint,
            'status': 'FAILED',
            'started_at': started_at_str,
            'completed_at': completed_at.strftime('%Y-%m-%d %H:%M:%S'),
            'source_file_count': 0,
            'source_size_bytes': 0,
            'dest_file_count': 0,
            'dest_size_bytes': 0,
            'files_copied': 0,
            'bytes_copied': 0,
            'is_incremental': False,
            'file_count_match': False,
            'size_match': False,
            'error': error_msg,
            'yarn_application_id': None,
            'yarn_application_ids': [],
            'distcp_duration_seconds': max((completed_at - started_at).total_seconds(), 0.0),
            'distcp_bytes_copied': 0,
            'distcp_files_copied': 0,
            'throughput_mbps': 0.0,
        }
        context['ti'].xcom_push(key='return_value', value=result)
        raise Exception(
            f"DistCp failed for {source_path} -> {s3_dest}: {error_msg}"
        ) from e


@task.pyspark(conn_id='spark_default')
def record_data_copy_status(distcp_result: dict, spark) -> dict:
    """Insert a row into data_copy_status for the completed/failed folder copy."""
    if not isinstance(distcp_result, dict) or 'run_id' not in distcp_result:
        logger.warning(
            f"[record_data_copy_status] Skipping invalid input: {type(distcp_result)}"
        )
        return {}

    config = get_config()
    tracking_db = config['tracking_database']

    run_id        = distcp_result['run_id']
    source_path   = distcp_result['source_path'].replace("'", "''")
    dest_bucket   = distcp_result['dest_bucket'].replace("'", "''")
    dest_path     = distcp_result['dest_path'].replace("'", "''")
    status        = str(distcp_result['status']).replace("'", "''")
    started_at    = str(distcp_result.get('started_at', '') or '').replace("'", "''")
    completed_at  = str(distcp_result.get('completed_at', '') or '').replace("'", "''")
    error_msg     = (distcp_result.get('error') or '').replace("'", "''")[:2000]

    src_file_count  = distcp_result.get('source_file_count', 0) or 0
    src_size_bytes  = distcp_result.get('source_size_bytes', 0) or 0
    dest_file_count = distcp_result.get('dest_file_count', 0) or 0
    dest_size_bytes = distcp_result.get('dest_size_bytes', 0) or 0
    files_copied    = distcp_result.get('files_copied', 0) or 0
    bytes_copied    = distcp_result.get('bytes_copied', 0) or 0
    is_incremental  = str(distcp_result.get('is_incremental', False)).lower()
    file_count_match = str(distcp_result.get('file_count_match', False)).lower()
    size_match       = str(distcp_result.get('size_match', False)).lower()

    yarn_ids = distcp_result.get('yarn_application_ids') or (
        [distcp_result['yarn_application_id']] if distcp_result.get('yarn_application_id') else []
    )
    yarn_app_id = ','.join(str(x) for x in yarn_ids).replace("'", "''")
    distcp_duration = float(distcp_result.get('distcp_duration_seconds', 0.0) or 0.0)
    distcp_bytes = int(distcp_result.get('distcp_bytes_copied', 0) or 0)
    distcp_files = int(distcp_result.get('distcp_files_copied', 0) or 0)
    throughput_mbps = float(distcp_result.get('throughput_mbps', 0.0) or 0.0)

    execute_with_iceberg_retry(spark, f"""
        INSERT INTO {tracking_db}.data_copy_status (
            run_id, source_path, dest_bucket, dest_path,
            status, started_at, completed_at,
            source_file_count, source_size_bytes,
            dest_file_count, dest_size_bytes,
            files_copied, bytes_copied,
            is_incremental, file_count_match, size_match,
            error_message, updated_at,
            yarn_application_id, distcp_started_at, distcp_completed_at,
            distcp_duration_seconds, distcp_bytes_copied, distcp_files_copied,
            throughput_mbps
        ) VALUES (
            '{run_id}',
            '{source_path}',
            '{dest_bucket}',
            '{dest_path}',
            '{status}',
            CAST('{started_at}' AS TIMESTAMP),
            CAST('{completed_at}' AS TIMESTAMP),
            {src_file_count},
            {src_size_bytes},
            {dest_file_count},
            {dest_size_bytes},
            {files_copied},
            {bytes_copied},
            {is_incremental},
            {file_count_match},
            {size_match},
            '{error_msg}',
            current_timestamp(),
            '{yarn_app_id}',
            CAST('{started_at}' AS TIMESTAMP),
            CAST('{completed_at}' AS TIMESTAMP),
            {distcp_duration},
            {distcp_bytes},
            {distcp_files},
            {throughput_mbps}
        )
    """, task_label=f"record_data_copy_status:{source_path}")

    logger.info(
        f"[FolderCopy] Recorded status={status} for {source_path} -> {dest_bucket}/{dest_path}"
    )
    return distcp_result


@task
def validate_data_copy(copy_status: dict, **context) -> dict:
    """Re-verify the S3 destination after copy: recount files/bytes and update data_copy_status."""
    if not isinstance(copy_status, dict) or 'run_id' not in copy_status:
        logger.warning(
            f"[validate_data_copy] Skipping invalid input: {type(copy_status)}"
        )
        return {}

    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])

    source_path = copy_status['source_path']
    dest_bucket = copy_status['dest_bucket']
    dest_path   = copy_status['dest_path']
    dest_endpoint = copy_status.get('dest_endpoint', '')
    s3_dest     = f"{dest_bucket}/{dest_path}"

    s3_opts = build_s3_opts(dest_bucket, config, dest_endpoint)

    # If the copy itself failed, skip SSH validation and mark as VALIDATION_SKIPPED
    upstream_status = copy_status.get('status')
    if upstream_status == 'FAILED':
        logger.warning(f"[FolderValidate] Skipping validation — copy FAILED for {source_path}")
        result = {
            **copy_status,
            'validation_status': 'VALIDATION_SKIPPED',
            'validation_error': copy_status.get('error'),
        }
        context['ti'].xcom_push(key='return_value', value=result)
        raise Exception(f"Validation skipped — upstream copy FAILED for {source_path}")

    if upstream_status in ('SOURCE_NOT_FOUND', 'EMPTY_SOURCE', 'SKIPPED'):
        logger.warning(
            f"[FolderValidate] Passing through status={upstream_status} for {source_path} "
            f"— no SSH validation performed"
        )
        result = {
            **copy_status,
            'validation_status': upstream_status,
            'validation_error': copy_status.get('error'),
        }
        ti = context.get('ti')
        if ti is not None:
            ti.xcom_push(key='return_value', value=result)
        return result

    cmd = f"""
if ! hadoop fs{s3_opts} -test -d "{s3_dest}" 2>/dev/null; then
    echo "DEST_EXISTS=false"
    echo "DEST_FILE_COUNT=0"
    echo "DEST_TOTAL_SIZE=0"
else
    echo "DEST_EXISTS=true"
    DEST_FILE_COUNT=$(hadoop fs{s3_opts} -ls -R "{s3_dest}" 2>/dev/null | grep '^-' | wc -l)
    DEST_TOTAL_SIZE=$(hadoop fs{s3_opts} -du -s "{s3_dest}" 2>/dev/null | awk '{{print $1}}')
    [ -z "$DEST_FILE_COUNT" ] && DEST_FILE_COUNT=0
    [ -z "$DEST_TOTAL_SIZE" ] && DEST_TOTAL_SIZE=0
    echo "DEST_FILE_COUNT=$DEST_FILE_COUNT"
    echo "DEST_TOTAL_SIZE=$DEST_TOTAL_SIZE"
fi
"""

    dest_exists = False
    dest_file_count = 0
    dest_size_bytes = 0
    validation_error = None

    try:
        with ssh.get_conn() as client:
            _, stdout, stderr = client.exec_command(_login_shell(cmd, config.get('cluster_type', 'MapR')), timeout=SSH_COMMAND_TIMEOUT)
            output = stdout.read().decode()
            stderr.read()
            stdout.channel.recv_exit_status()

            for line in output.split('\n'):
                line = line.strip()
                if line == 'DEST_EXISTS=true':
                    dest_exists = True
                elif line.startswith('DEST_FILE_COUNT='):
                    dest_file_count = int(line.split('=', 1)[1] or 0)
                elif line.startswith('DEST_TOTAL_SIZE='):
                    dest_size_bytes = int(line.split('=', 1)[1] or 0)

            if not dest_exists:
                validation_error = f"S3 destination does not exist after copy: {s3_dest}"
                logger.error(f"[FolderValidate] {validation_error}")

    except Exception as e:
        validation_error = f"Validation SSH error: {str(e)[:1000]}"
        logger.error(f"[FolderValidate] {validation_error}")

    src_file_count = copy_status.get('source_file_count', 0) or 0
    src_size_bytes = copy_status.get('source_size_bytes', 0) or 0

    file_count_match = dest_file_count == src_file_count
    size_match = abs(src_size_bytes - dest_size_bytes) <= max(1, int(src_size_bytes * 0.01))

    validation_status = 'VALIDATED' if (dest_exists and file_count_match and size_match and not validation_error) else 'VALIDATION_FAILED'

    logger.info(
        f"[FolderValidate] {source_path} -> {s3_dest} | "
        f"dest_exists={dest_exists} | file_count_match={file_count_match} | "
        f"size_match={size_match} | status={validation_status}"
    )

    result = {
        **copy_status,
        'dest_file_count': dest_file_count,
        'dest_size_bytes': dest_size_bytes,
        'file_count_match': file_count_match,
        'size_match': size_match,
        'validation_status': validation_status,
        'validation_error': validation_error,
    }
    if validation_status != 'VALIDATED':
        result['validation_error'] = (
            validation_error or 'file count or size mismatch'
        )
        context['ti'].xcom_push(key='return_value', value=result)
        raise Exception(
            f"Validation {validation_status} for {source_path} -> {s3_dest}: "
            f"{validation_error or 'file count or size mismatch'}"
        )
    return result


@task.pyspark(conn_id='spark_default')
def update_data_copy_validation(validation_result: dict, spark) -> dict:
    """Update data_copy_status with final validation metrics and status."""
    if not isinstance(validation_result, dict) or 'run_id' not in validation_result:
        logger.warning(
            f"[update_data_copy_validation] Skipping invalid input: {type(validation_result)}"
        )
        return {}

    config = get_config()
    tracking_db = config['tracking_database']

    run_id      = validation_result['run_id']
    source_path = validation_result['source_path'].replace("'", "''")
    dest_bucket = validation_result['dest_bucket'].replace("'", "''")
    dest_path   = validation_result['dest_path'].replace("'", "''")

    validation_status = str(
        validation_result.get('validation_status', 'VALIDATION_FAILED')
    ).replace("'", "''")
    dest_file_count   = validation_result.get('dest_file_count', 0) or 0
    dest_size_bytes   = validation_result.get('dest_size_bytes', 0) or 0
    file_count_match  = str(validation_result.get('file_count_match', False)).lower()
    size_match        = str(validation_result.get('size_match', False)).lower()
    val_error         = str(validation_result.get('validation_error') or '').replace("'", "''")[:2000]

    yarn_ids = validation_result.get('yarn_application_ids') or (
        [validation_result['yarn_application_id']]
        if validation_result.get('yarn_application_id') else []
    )
    yarn_app_id = ','.join(str(x) for x in yarn_ids).replace("'", "''")
    distcp_duration = float(validation_result.get('distcp_duration_seconds', 0.0) or 0.0)
    distcp_bytes = int(validation_result.get('distcp_bytes_copied', 0) or 0)
    distcp_files = int(validation_result.get('distcp_files_copied', 0) or 0)
    throughput_mbps = float(validation_result.get('throughput_mbps', 0.0) or 0.0)

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.data_copy_status
        SET status           = '{validation_status}',
            dest_file_count  = {dest_file_count},
            dest_size_bytes  = {dest_size_bytes},
            file_count_match = {file_count_match},
            size_match       = {size_match},
            yarn_application_id = CASE
                                 WHEN '{yarn_app_id}' != '' THEN '{yarn_app_id}'
                                 ELSE yarn_application_id
                               END,
            distcp_duration_seconds = {distcp_duration},
            distcp_bytes_copied     = {distcp_bytes},
            distcp_files_copied     = {distcp_files},
            throughput_mbps         = {throughput_mbps},
            error_message    = CASE
                                 WHEN '{val_error}' != '' THEN '{val_error}'
                                 ELSE error_message
                               END,
            updated_at       = current_timestamp()
        WHERE run_id     = '{run_id}'
          AND source_path = '{source_path}'
          AND dest_bucket = '{dest_bucket}'
          AND dest_path   = '{dest_path}'
    """, task_label=f"update_data_copy_validation:{source_path}")

    logger.info(f"[FolderValidate] Updated tracking: {source_path} -> {validation_status}")
    return validation_result


@task.pyspark(conn_id='spark_default')
def finalize_data_copy_run(run_id: str, spark, cluster_setup: dict = None) -> dict:
    """Aggregate folder-level counts and mark the data_copy_runs record as COMPLETED."""
    config = get_config()
    tracking_db = config['tracking_database']

    total = successful = failed = skipped = not_found = 0
    overall_status = 'FAILED'

    try:
        stats = spark.sql(f"""
            SELECT
                COUNT(*)                                                              AS total_folders,
                SUM(CASE WHEN status IN ('VALIDATED', 'EMPTY_SOURCE', 'SOURCE_NOT_FOUND')  THEN 1 ELSE 0 END) AS successful_folders,
                SUM(CASE WHEN status IN ('FAILED', 'VALIDATION_FAILED', 'VALIDATION_SKIPPED') THEN 1 ELSE 0 END) AS failed_folders,
                SUM(CASE WHEN status IN ('SOURCE_NOT_FOUND')                               THEN 1 ELSE 0 END) AS not_found_folders,
                SUM(CASE WHEN status IN ('SKIPPED')                                        THEN 1 ELSE 0 END) AS skipped_folders
            FROM {tracking_db}.data_copy_status
            WHERE run_id = '{run_id}'
        """).collect()

        if not stats or int(stats[0]['total_folders'] or 0) == 0:
            logger.warning(
                f"[FolderCopy] No folder records found for run_id '{run_id}'. "
                f"Upstream tasks (excel parse / distcp / record) likely failed before "
                f"writing any records."
            )
            overall_status = 'FAILED'
        else:
            total      = int(stats[0]['total_folders'] or 0)
            successful = int(stats[0]['successful_folders'] or 0)
            failed     = int(stats[0]['failed_folders'] or 0)
            not_found  = int(stats[0]['not_found_folders'] or 0)
            skipped    = int(stats[0]['skipped_folders'] or 0)
            overall_status = 'COMPLETED' if failed == 0 else 'COMPLETED_WITH_ERRORS'
    except Exception as e:
        logger.error(
            f"[FolderCopy] Failed to query data_copy_status for run_id '{run_id}': {str(e)}"
        )
        total = successful = failed = skipped = not_found = 0
        overall_status = 'FAILED'

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.data_copy_runs
        SET status             = '{overall_status}',
            completed_at       = current_timestamp(),
            total_folders      = {total},
            successful_folders = {successful},
            failed_folders     = {failed},
            skipped_folders    = {skipped}
        WHERE run_id = '{run_id}'
    """, task_label="finalize_data_copy_run")

    if isinstance(cluster_setup, dict) and cluster_setup.get('service_account_user_id'):
        _sa = str(cluster_setup['service_account_user_id']).replace("'", "''")
        _sa_src = str(cluster_setup.get('service_account_source') or 'unknown').replace("'", "''")
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.data_copy_runs
            SET service_account_user_id = '{_sa}',
                service_account_source  = '{_sa_src}'
            WHERE run_id = '{run_id}'
        """, task_label="finalize_data_copy_run:update_service_account")
        logger.info(f"[FolderCopy] Service account recorded: {_sa} ({_sa_src})")

    logger.info(
        f"[FolderCopy] Run {run_id} finalized: status={overall_status} | "
        f"total={total} | successful={successful} | failed={failed} | "
        f"not_found={not_found} | skipped={skipped}"
    )
    return {
        'run_id': run_id,
        'status': overall_status,
        'total_folders': total,
        'successful_folders': successful,
        'failed_folders': failed,
        'not_found_folders': not_found,
        'skipped_folders': skipped,
    }


@task.pyspark(conn_id='spark_default')
def generate_data_copy_html_report(finalize_result: dict, run_id: str, spark,
                                   cluster_setup: dict = None, **context) -> dict:
    """Generate HTML report for folder-only data copy run and write to S3."""
    from datetime import datetime

    config = get_config()
    tracking_db = config['tracking_database']
    report_location = config['report_output_location']

    run_info = spark.sql(f"""
        SELECT * FROM {tracking_db}.data_copy_runs
        WHERE run_id = '{run_id}'
    """).collect()
    run_row = run_info[0] if run_info else None

    folders = spark.sql(f"""
        SELECT * FROM {tracking_db}.data_copy_status
        WHERE run_id = '{run_id}'
        ORDER BY source_path
    """).collect()

    non_distcp_statuses = ('EMPTY_SOURCE', 'SOURCE_NOT_FOUND', 'SKIPPED')
    copied_rows = [f for f in folders if (f.status or '') not in non_distcp_statuses]

    badge_classes = {
        'VALIDATED': 'status-completed',
        'COMPLETED': 'status-completed',
        'FAILED': 'status-failed',
        'VALIDATION_FAILED': 'status-failed',
        'VALIDATION_SKIPPED': 'status-failed',
        'EMPTY_SOURCE': 'status-empty',
        'SOURCE_NOT_FOUND': 'status-not-found',
        'SKIPPED': 'status-skipped',
    }

    total_folders     = len(folders)
    validated         = sum(1 for f in folders if f.status == 'VALIDATED')
    failed            = sum(1 for f in folders if (f.status or '') in ('FAILED', 'VALIDATION_FAILED', 'VALIDATION_SKIPPED'))
    source_not_found  = sum(1 for f in folders if (f.status or '') == 'SOURCE_NOT_FOUND')
    empty_source      = sum(1 for f in folders if (f.status or '') == 'EMPTY_SOURCE')
    skipped           = sum(1 for f in folders if (f.status or '') == 'SKIPPED')
    successful        = validated + empty_source + source_not_found
    incremental       = sum(1 for f in folders if f.is_incremental)
    total_bytes       = sum(f.dest_size_bytes or 0 for f in folders)
    total_files       = sum(f.dest_file_count or 0 for f in folders)
    total_gb          = total_bytes / (1024 ** 3)

    size_match_count      = sum(1 for f in copied_rows if f.size_match)
    size_mismatch_count   = len(copied_rows) - size_match_count
    fcount_match_count    = sum(1 for f in copied_rows if f.file_count_match)
    fcount_mismatch_count = len(copied_rows) - fcount_match_count
    total_src_gb  = sum(f.source_size_bytes or 0 for f in copied_rows) / (1024 ** 3)
    total_dest_gb = sum(f.dest_size_bytes or 0 for f in copied_rows) / (1024 ** 3)
    size_diff_pct = (
        abs(total_src_gb - total_dest_gb) / total_src_gb * 100 if total_src_gb > 0 else 0.0
    )

    _throughputs = [
        float(getattr(f, 'throughput_mbps', 0) or 0)
        for f in folders
        if float(getattr(f, 'throughput_mbps', 0) or 0) > 0
    ]
    avg_throughput = (sum(_throughputs) / len(_throughputs)) if _throughputs else 0.0

    run_status = (run_row.status if run_row else 'UNKNOWN')
    dag_run_id = str(getattr(run_row, 'dag_run_id', '') or '') if run_row else ''

    if isinstance(cluster_setup, dict):
        sa_user = str(cluster_setup.get('service_account_user_id') or '').strip()
        sa_source = str(cluster_setup.get('service_account_source') or '').strip()
    else:
        sa_user, sa_source = '', ''
    if not sa_user and run_row is not None:
        sa_user = str(getattr(run_row, 'service_account_user_id', '') or '').strip()
        sa_source = str(getattr(run_row, 'service_account_source', '') or '').strip()
    if not sa_user:
        sa_user = str(config.get('service_account_user_id') or '').strip()
        sa_source = 'config:service_account_user_id' if sa_user else 'unknown'
    sa_display = sa_user or 'not configured'
    sa_source = sa_source or 'unknown'

    def _status_row(f, colspan):
        status_val = f.status or ''
        source_cell = _html_escape(str(f.source_path or ''))
        if status_val == 'EMPTY_SOURCE':
            return f"""
                <tr>
                    <td>{source_cell}</td>
                    <td colspan="{colspan}">
                        <span class="status-badge status-empty">EMPTY_SOURCE</span>
                    </td>
                </tr>
"""
        return f"""
                <tr>
                    <td>{source_cell}</td>
                    <td colspan="{colspan}">
                        <span class="status-badge status-not-found">SOURCE_NOT_FOUND</span>
                    </td>
                </tr>
"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Folder Data Copy Report - {run_id}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 30px;
            border-bottom: 2px solid #ecf0f1;
            padding-bottom: 8px;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .summary-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .summary-card.success {{
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }}
        .summary-card.warning {{
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        }}
        .summary-card.info {{
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        }}
        .summary-card h3 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .summary-card .value {{
            font-size: 32px;
            font-weight: bold;
            margin: 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 14px;
        }}
        th {{
            background-color: #34495e;
            color: white;
            padding: 12px;
            text-align: left;
            position: sticky;
            top: 0;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #ecf0f1;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        .status-badge {{
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: bold;
            display: inline-block;
        }}
        .status-completed {{
            background-color: #d4edda;
            color: #155724;
        }}
        .status-failed {{
            background-color: #f8d7da;
            color: #721c24;
        }}
        .status-skipped {{
            background-color: #fff3cd;
            color: #856404;
        }}
        .status-empty {{
            background-color: #e8f4fd;
            color: #1a6fa3;
        }}
        .status-not-found {{
            background-color: #e2e3e5;
            color: #6c757d;
        }}
        .status-warning {{
            background-color: #fff3cd;
            color: #856404;
        }}
        .metric {{
            font-weight: bold;
            color: #2980b9;
        }}
        .duration {{
            color: #7f8c8d;
            font-size: 12px;
        }}
        .validation-pass {{
            color: #27ae60;
            font-weight: bold;
        }}
        .validation-fail {{
            color: #e74c3c;
            font-weight: bold;
        }}
        .validation-warn {{
            color: #856404;
            background-color: #fff3cd;
            font-weight: bold;
            padding: 2px 6px;
            border-radius: 4px;
        }}
        .timestamp {{
            color: #95a5a6;
            font-size: 12px;
        }}
        td.status-cell {{
            max-width: 320px;
        }}
        .status-reason {{
            margin-top: 6px;
            color: #4d5656;
            font-size: 12px;
            line-height: 1.4;
            white-space: normal;
            word-break: break-word;
        }}
        .section-divider {{
            margin: 40px 0;
            border-top: 2px dashed #ecf0f1;
        }}
    </style>
</head>
<body>
<div class="container">
    <h1>Folder Data Copy Report</h1>
    <div class="timestamp">
        Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
        Run ID: <strong>{run_id}</strong><br>
        DAG Run: <strong>{_html_escape(dag_run_id)}</strong><br>
        Service Account: <strong>{_html_escape(sa_display)}</strong> <span style="opacity:0.7">({_html_escape(sa_source)})</span>
    </div>

    <h2>Copy Summary</h2>
    <div class="summary-grid">
        <div class="summary-card">
            <h3>TOTAL FOLDERS</h3>
            <p class="value">{total_folders}</p>
        </div>
        <div class="summary-card success">
            <h3>SUCCESSFUL</h3>
            <p class="value">{successful}</p>
        </div>
        <div class="summary-card warning">
            <h3>FAILED</h3>
            <p class="value">{failed}</p>
        </div>
        <div class="summary-card">
            <h3>SOURCE NOT FOUND</h3>
            <p class="value">{source_not_found}</p>
        </div>
        <div class="summary-card info">
            <h3>EMPTY SOURCE</h3>
            <p class="value">{empty_source}</p>
        </div>
        <div class="summary-card">
            <h3>SKIPPED</h3>
            <p class="value">{skipped}</p>
        </div>
        <div class="summary-card info">
            <h3>TOTAL DATA</h3>
            <p class="value">{total_gb:.6f} GB</p>
        </div>
        <div class="summary-card info">
            <h3>TOTAL FILES</h3>
            <p class="value">{total_files:,}</p>
        </div>
        <div class="summary-card">
            <h3>INCREMENTAL RUNS</h3>
            <p class="value">{incremental}</p>
        </div>
        <div class="summary-card info">
            <h3>AVG THROUGHPUT (MB/s)</h3>
            <p class="value">{avg_throughput:.2f}</p>
        </div>
    </div>

    <div class="section-divider"></div>

    <h2>Data Validation Summary</h2>
    <div class="summary-grid">
        <div class="summary-card {'success' if size_mismatch_count == 0 else 'warning'}">
            <h3>SIZE MATCH</h3>
            <p class="value">{size_match_count} / {size_match_count + size_mismatch_count}</p>
        </div>
        <div class="summary-card {'success' if fcount_mismatch_count == 0 else 'warning'}">
            <h3>FILE COUNT MATCH</h3>
            <p class="value">{fcount_match_count} / {fcount_match_count + fcount_mismatch_count}</p>
        </div>
        <div class="summary-card info">
            <h3>SOURCE SIZE</h3>
            <p class="value">{total_src_gb:.6f} GB</p>
        </div>
        <div class="summary-card info">
            <h3>DEST SIZE</h3>
            <p class="value">{total_dest_gb:.6f} GB</p>
        </div>
        <div class="summary-card {'success' if size_diff_pct < 1.0 else 'warning'}">
            <h3>SIZE DELTA</h3>
            <p class="value">{size_diff_pct:.2f}%</p>
        </div>
    </div>

    <div class="section-divider"></div>

    <h2>Folder Copy Details</h2>
    <table>
        <thead>
            <tr>
                <th>Source Path</th>
                <th>Destination</th>
                <th>Status</th>
                <th>Incremental</th>
                <th>DistCp Duration</th>
                <th>YARN App ID</th>
                <th>Total Duration</th>
            </tr>
        </thead>
        <tbody>
"""

    for f in folders:
        status = f.status or ''
        if status in ('EMPTY_SOURCE', 'SOURCE_NOT_FOUND'):
            html += _status_row(f, 6)
            continue

        badge_cls = badge_classes.get(status, 'status-warning')
        _reason = str(f.error_message or '')
        badge_title_attr = f' title="{_html_escape(_reason)}"' if _reason else ''
        status_reason_html = (
            f'\n                        <div class="status-reason">{_html_escape(_reason[:200])}</div>'
            if _reason else ''
        )
        _dur = float(getattr(f, 'distcp_duration_seconds', 0) or 0)
        _started = getattr(f, 'started_at', None)
        _completed = getattr(f, 'completed_at', None)
        _total_dur = _dur
        if _started is not None and _completed is not None:
            try:
                _total_dur = max((_completed - _started).total_seconds(), 0.0)
            except Exception:
                _total_dur = _dur
        dur_cell = f"{_dur:.1f}s" if _dur else 'N/A'
        if f.is_incremental:
            dur_cell += " <span style='background-color: #fff3cd; padding: 2px 6px; border-radius: 4px; font-size: 10px;'>INCREMENTAL</span>"

        _yarn_raw = str(getattr(f, 'yarn_application_id', '') or '')
        _yarn_ids = [x for x in _yarn_raw.split(',') if x]
        if len(_yarn_ids) > 1:
            _yarn_list = "".join(
                f"<div style='font-family:monospace;color:#666;font-size:11px;'>{_html_escape(i)}</div>"
                for i in _yarn_ids
            )
            yarn_cell = (
                f"<details style='font-size:11px;color:#666;'>"
                f"<summary style='cursor:pointer;color:#2980b9;'>{len(_yarn_ids)} YARN app IDs</summary>"
                f"{_yarn_list}</details>"
            )
        elif _yarn_ids:
            yarn_cell = f"<small style='font-family:monospace;color:#666;'>{_html_escape(_yarn_ids[0])}</small>"
        else:
            yarn_cell = 'N/A'

        html += f"""
                <tr>
                    <td>{_html_escape(str(f.source_path or ''))}</td>
                    <td>{_html_escape(str(f.dest_bucket or ''))}/{_html_escape(str(f.dest_path or ''))}</td>
                    <td class="status-cell">
                        <span class="status-badge {badge_cls}"{badge_title_attr}>{status}</span>{status_reason_html}
                    </td>
                    <td style="text-align:center">{'✓' if f.is_incremental else ''}</td>
                    <td class="duration">{dur_cell}</td>
                    <td>{yarn_cell}</td>
                    <td class="metric">{_total_dur:.1f}s</td>
                </tr>
"""

    html += """
        </tbody>
    </table>

    <div class="section-divider"></div>

    <h2>Data Validation Results</h2>
    <table>
        <thead>
            <tr>
                <th>Source Path</th>
                <th>Source Size (GB)</th>
                <th>S3 Size Before (GB)</th>
                <th>S3 Size After (GB)</th>
                <th>S3 Size Transferred (GB)</th>
                <th>Size Match</th>
                <th>Source Files</th>
                <th>S3 Files Before</th>
                <th>S3 Files After</th>
                <th>S3 Files Transferred</th>
                <th>File Count Match</th>
            </tr>
        </thead>
        <tbody>
"""

    for f in folders:
        status = f.status or ''
        if status in ('EMPTY_SOURCE', 'SOURCE_NOT_FOUND'):
            html += _status_row(f, 10)
            continue

        src_gb = (f.source_size_bytes or 0) / (1024 ** 3)
        dest_gb = (f.dest_size_bytes or 0) / (1024 ** 3)
        transferred_gb = (f.bytes_copied or 0) / (1024 ** 3)
        before_gb = max(dest_gb - transferred_gb, 0.0)
        files_after = f.dest_file_count or 0
        files_transferred = f.files_copied or 0
        files_before = max(files_after - files_transferred, 0)

        size_match_class = 'validation-pass' if f.size_match else 'validation-fail'
        size_match_icon = '✓ PASS' if f.size_match else '✗ FAIL'
        count_match_class = 'validation-pass' if f.file_count_match else 'validation-fail'
        count_match_icon = '✓ PASS' if f.file_count_match else '✗ FAIL'

        html += f"""
                <tr>
                    <td>{_html_escape(str(f.source_path or ''))}</td>
                    <td class="metric">{src_gb:.6f}</td>
                    <td class="metric">{before_gb:.6f}</td>
                    <td class="metric">{dest_gb:.6f}</td>
                    <td class="metric">{transferred_gb:.6f}</td>
                    <td class="{size_match_class}">{size_match_icon}</td>
                    <td class="metric">{(f.source_file_count or 0):,}</td>
                    <td class="metric">{files_before:,}</td>
                    <td class="metric">{files_after:,}</td>
                    <td class="metric">{files_transferred:,}</td>
                    <td class="{count_match_class}">{count_match_icon}</td>
                </tr>
"""

    html += """
        </tbody>
    </table>

    <div class="section-divider"></div>

    <h2>Performance Metrics</h2>
    <table>
        <thead>
            <tr>
                <th>Source Path</th>
                <th>Data Volume</th>
                <th>DistCp Speed</th>
                <th>Files/Second</th>
                <th>End-to-End Duration</th>
            </tr>
        </thead>
        <tbody>
"""

    for f in folders:
        if (f.status or '') == 'SOURCE_NOT_FOUND':
            continue
        _dur = float(getattr(f, 'distcp_duration_seconds', 0) or 0)
        _started = getattr(f, 'started_at', None)
        _completed = getattr(f, 'completed_at', None)
        _total_dur = _dur
        if _started is not None and _completed is not None:
            try:
                _total_dur = max((_completed - _started).total_seconds(), 0.0)
            except Exception:
                _total_dur = _dur
        data_gb = (f.dest_size_bytes or 0) / (1024 ** 3)
        speed_mbps = float(getattr(f, 'throughput_mbps', 0) or 0)
        files_per_sec = (f.dest_file_count or 0) / (_dur or 1)
        html += f"""
                <tr>
                    <td>{_html_escape(str(f.source_path or ''))}</td>
                    <td class="metric">{data_gb:.6f} GB</td>
                    <td class="metric">{speed_mbps:.6f} MB/s</td>
                    <td class="metric">{files_per_sec:,.0f}</td>
                    <td class="metric">{_total_dur:.1f}s ({_total_dur / 60:.1f}m)</td>
                </tr>
"""

    html += """
        </tbody>
    </table>

    <div style="margin-top:50px; padding-top:20px; border-top:1px solid #ecf0f1; color:#95a5a6; font-size:12px;">
        <p>This report was automatically generated by the Folder Data Copy DAG.</p>
    </div>
</div>
</body>
</html>
"""

    portal_run_id = context.get('params', {}).get('run_id') or run_id
    report_filename = f"{portal_run_id}_data_copy_report.html"
    report_path = f"{report_location}/{report_filename}"
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(report_path),
        hadoop_conf
    )
    out = spark._jvm.org.apache.hadoop.fs.Path(report_path)
    stream = fs.create(out, True)
    stream.write(html.encode('utf-8'))
    stream.close()

    logger.info(f"[FolderCopy] HTML report written to {report_path}")
    return {
        'report_path': report_path,
        'overall_status': run_status,
        'total_folders': total_folders,
        'successful_folders': successful,
    }


@task.pyspark(conn_id='spark_default')
def send_data_copy_report_email(report_result: dict, run_id: str, spark) -> dict:
    """Send the folder data copy HTML report via SMTP."""
    import os
    import tempfile

    from airflow.utils.email import send_email

    config = get_config()
    smtp_conn_id   = config.get('smtp_conn_id', 'smtp_default')
    recipients_raw = config.get('email_recipients', '')

    if not recipients_raw or not recipients_raw.strip():
        logger.warning("[FolderCopy] No recipients configured in 'migration_email_recipients' variable. Skipping email.")
        return {'sent': False, 'reason': 'no_recipients'}

    recipients  = [r.strip() for r in recipients_raw.split(',') if r.strip()]
    report_path = report_result.get('report_path', '')
    overall_status = report_result.get('overall_status', 'UNKNOWN')
    total_folders = report_result.get('total_folders', 0)
    successful_folders = report_result.get('successful_folders', 0)

    try:
        logger.info(f"[FolderCopy] Reading HTML report from S3: {report_path}")
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(report_path),
            hadoop_conf
        )
        s3_path_obj = spark._jvm.org.apache.hadoop.fs.Path(report_path)
        reader = spark._jvm.java.io.BufferedReader(
            spark._jvm.java.io.InputStreamReader(fs.open(s3_path_obj), "UTF-8")
        )
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        html_content = "\n".join(lines)

        tmp = tempfile.NamedTemporaryFile(
            mode='w', suffix='.html',
            prefix=f'{run_id}_data_copy_report_',
            delete=False
        )
        tmp.write(html_content)
        tmp.close()

        send_email(
            to=recipients,
            subject=(
                f"Folder Data Copy Report - {run_id} - {overall_status} "
                f"({successful_folders}/{total_folders})"
            ),
            html_content=f"<p>Please find the Folder Data Copy report for run <strong>{run_id}</strong> attached.</p>",
            files=[tmp.name],
            conn_id=smtp_conn_id,
        )
        os.unlink(tmp.name)
        logger.info(f"[FolderCopy] Report email sent to: {recipients}")
        return {'sent': True, 'recipients': recipients, 'report_path': report_path}
    except Exception as e:
        logger.error(f"[FolderCopy] Failed to send report email: {str(e)}")
        if is_permanent_error("send_email", e):
            permanent_fail("send_data_copy_report_email", e)
        raise Exception(f"Failed to send Folder Data Copy report email: {str(e)}") from e


# =============================================================================
# DAG 3 DEFINITION: FOLDER-ONLY DATA COPY
# =============================================================================

with DAG(
    dag_id='folder_only_data_copy',
    default_args=default_args,
    description='Copy folders from source cluster (MapR/HDP) to S3 via DistCp — no Hive metadata',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=3,
    tags=['migration', 'source-cluster', 's3', 'folder-copy'],
    params={
        'excel_file_path': Param(
            default='s3a://config-bucket/folder_copy.xlsx',
            type='string',
            description='S3 path to Excel config file (columns: source_path, target_bucket, dest_folder)'
        ),
        'tenant': Param(
            default=None,
            type=['null', 'string'],
            description=(
                'Tenant/environment key from the migration_tenant_profiles Variable '
                '(e.g. tenant_1, tenant_2). Blank = use the global cluster_* Variables.'
            )
        ),
    },
    render_template_as_native_obj=True,
    on_failure_callback=_folder_copy_dag_failure_callback,
) as dag_folder_copy:

    # Pre-flight checks
    t_fc_prereq  = validate_prerequisites_folder_copy()

    # Initialize tracking tables and create a run record
    t_fc_init   = init_folder_copy_tracking_tables()
    t_fc_run_id = create_data_copy_run(
        excel_file_path="{{ params.excel_file_path }}",
        dag_run_id="{{ run_id }}"
    )

    # Cluster authentication — receives tracking run_id
    t_fc_cluster = cluster_login_setup(run_id=t_fc_run_id)

    # Parse Excel — one dict per folder row
    t_fc_excel = parse_folder_copy_excel(
        excel_file_path="{{ params.excel_file_path }}",
        run_id=t_fc_run_id
    )

    # Per-folder: copy → record → validate → update validation (dynamically mapped)
    # max_active_tis_per_dagrun=3 caps concurrent DistCp YARN jobs
    t_fc_distcp = run_folder_distcp_ssh.override(
        trigger_rule='all_done', max_active_tis_per_dagrun=3
    ).partial(cluster_setup=t_fc_cluster).expand(folder_config=t_fc_excel)

    t_fc_record = record_data_copy_status.override(
        trigger_rule='all_done'
    ).expand(distcp_result=t_fc_distcp)

    t_fc_copy_validate = validate_data_copy.override(
        trigger_rule='all_done'
    ).expand(copy_status=t_fc_record)

    t_fc_val_status = update_data_copy_validation.override(
        trigger_rule='all_done'
    ).expand(validation_result=t_fc_copy_validate)

    # Finalize run — waits for all per-folder validation to finish (via dependency chain)
    t_fc_final = finalize_data_copy_run.override(trigger_rule='all_done')(
        run_id=t_fc_run_id,
        cluster_setup=t_fc_cluster
    )

    # Report and email
    t_fc_report = generate_data_copy_html_report.override(trigger_rule='all_done')(
        run_id=t_fc_run_id,
        finalize_result=t_fc_final,
        cluster_setup=t_fc_cluster,
        params="{{ params }}"
    )

    t_fc_email = send_data_copy_report_email.override(trigger_rule='all_done')(
        report_result=t_fc_report,
        run_id=t_fc_run_id
    )

    # Dependency chain — prereqs → init → run_id → excel → cluster → work
    # Per-folder chain is linear: distcp → record_status → validate → update_validation
    t_fc_prereq >> t_fc_init >> t_fc_run_id >> t_fc_cluster >> t_fc_excel
    t_fc_excel >> t_fc_distcp >> t_fc_record >> t_fc_copy_validate >> t_fc_val_status
    t_fc_val_status >> t_fc_final >> t_fc_report >> t_fc_email
