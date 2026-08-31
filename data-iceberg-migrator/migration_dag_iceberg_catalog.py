"""
DAG 4: Iceberg Catalog Migration

Iceberg-to-Iceberg migration using the rewrite_table_path procedure.
Use this DAG when data AND metadata have already been copied to the destination
S3 bucket and snapshot history / partition transform fidelity must be preserved.

Strategy: iceberg_rewrite_table_path
  - Reads existing Iceberg metadata at the destination (which still references
    old source S3 paths) and rewrites every path prefix via the Iceberg
    rewrite_table_path stored procedure.
  - Registers the rewritten table in HMS via register_table.
  - Preserves full snapshot history, time-travel, all partition transforms
    (year, month, bucket, truncate), exact schema types, and table properties.

Requirements:
  - Both data AND metadata files must be present at the destination S3 path.
  - Destination Spark/Iceberg environment must support rewrite_table_path
    (Apache Iceberg 1.4+).

Pipeline stages:
  1. Init tracking tables & create run record
  2. Parse Excel config (database, table tokens, source_s3_prefix, dest_s3_prefix)
  3. Validate data presence at destination S3 paths
  4. Discover source table metadata (schema, partitions, row counts from metadata.json)
  5. Create destination tables (rewrite_table_path + register_table)
  6. Validate destination tables (row count, partition, schema comparison)
  7. Generate HTML report & send email

Excel columns: database | table | source_s3_prefix | dest_s3_prefix
"""

import contextlib
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from dotenv import load_dotenv
from migrator_utils.migrations.shared import (
    execute_with_iceberg_retry,
    get_config,
    track_duration,
)

_dag_stem = Path(__file__).stem
logger = logging.getLogger(__name__)

_dag_dir = Path(__file__).resolve().parent
_config_dir = str(_dag_dir / 'migrator_utils' / 'migration_configs')
if os.path.isdir(_config_dir):
    load_dotenv(os.path.join(_config_dir, 'env.shared'))
    load_dotenv(os.path.join(_config_dir, f'env.{_dag_stem}'), override=True)
else:
    logger.warning(
        f"Config directory {_config_dir} not found — "
        "env files not loaded, using Airflow Variables / defaults"
    )


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
    'owner': _resolve_dag_owner(),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# =============================================================================
# DAG 4: ICEBERG REWRITE TABLE PATH MIGRATION TASKS
# =============================================================================

@task.pyspark(conn_id='spark_default')
def init_tracking_tables(spark) -> dict:
    """Create Iceberg tracking tables for the rewrite migration if they don't exist."""
    config = get_config()
    tracking_db = config['tracking_database']
    tracking_loc = config['tracking_location']

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.rewrite_migration_runs (
            run_id              STRING,
            dag_run_id          STRING,
            excel_file_path     STRING,
            started_at          TIMESTAMP,
            completed_at        TIMESTAMP,
            status              STRING,
            total_tables        INT,
            successful_tables   INT,
            failed_tables       INT,
            missing_tables      INT,
            config_json         STRING
        )
        USING iceberg
        LOCATION '{tracking_loc}/rewrite_migration_runs'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.rewrite_migration_table_status (
            run_id                      STRING,
            source_database             STRING,
            source_table                STRING,
            dest_database               STRING,
            dest_bucket                 STRING,
            source_s3_location          STRING,
            dest_s3_location            STRING,
            source_s3_prefix            STRING,
            dest_s3_prefix              STRING,
            file_format                 STRING,
            is_partitioned              BOOLEAN,
            partition_columns           STRING,
            partition_count             INT,
            schema_json                 STRING,
            partitions_json             STRING,
            table_type                  STRING,
            source_row_count            BIGINT,
            data_presence_status        STRING,
            data_presence_checked_at    TIMESTAMP,
            data_presence_file_count    BIGINT,
            data_presence_size_bytes    BIGINT,
            discovery_status            STRING,
            discovery_completed_at      TIMESTAMP,
            discovery_duration_seconds  DOUBLE,
            table_create_status             STRING,
            table_create_completed_at       TIMESTAMP,
            table_create_duration_seconds   DOUBLE,
            table_already_existed           BOOLEAN,
            validation_status               STRING,
            validation_completed_at         TIMESTAMP,
            validation_duration_seconds     DOUBLE,
            dest_hive_row_count             BIGINT,
            dest_partition_count            INT,
            source_partition_count          INT,
            row_count_match                 BOOLEAN,
            partition_count_match           BOOLEAN,
            schema_match                    BOOLEAN,
            path_rewrite_verified           BOOLEAN,
            snapshot_count_match            BOOLEAN,
            source_snapshot_count           BIGINT,
            dest_snapshot_count             BIGINT,
            schema_differences              STRING,
            overall_status  STRING,
            error_message   STRING,
            updated_at      TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (source_database)
        LOCATION '{tracking_loc}/rewrite_migration_table_status'
    """)

    logger.info(f"[init_tracking_tables] Tracking tables ready in '{tracking_db}'")
    return {'status': 'initialized', 'database': tracking_db}


@task.pyspark(conn_id='spark_default')
def create_migration_run(excel_file_path: str, dag_run_id: str, spark) -> str:
    """Create run record and return run_id."""
    import uuid
    config = get_config()
    tracking_db = config['tracking_database']
    run_id = f"rewrite_run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    spark.sql(f"""
        INSERT INTO {tracking_db}.rewrite_migration_runs
        VALUES (
            '{run_id}', '{dag_run_id}', '{excel_file_path}',
            current_timestamp(), NULL, 'RUNNING',
            0, 0, 0, 0,
            '{json.dumps(config).replace("'", "''")}'
        )
    """)
    logger.info(f"[create_migration_run] Run created: {run_id}")
    return run_id


@task.pyspark(conn_id='spark_default')
def parse_excel(excel_file_path: str, run_id: str, spark) -> list:
    """Read Excel config and parse rows for the rewrite_table_path migration.

    Required columns: database, table, source_s3_prefix, dest_s3_prefix.
    Rows are grouped by (database, source_s3_prefix, dest_s3_prefix).

    A blank source_s3_prefix is treated as a "register-only" row: the cell is
    mirrored to dest_s3_prefix so source_prefix == dest_prefix downstream, which
    causes rewrite_and_register_tables to skip rewrite_table_path and register
    straight from the destination metadata.
    """
    from io import BytesIO

    import pandas as ps
    from migrator_utils.migrations.shared import cell_str, normalize_s3

    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    excel_bytes = bytes(row.content)
    df = ps.read_excel(BytesIO(excel_bytes), engine='openpyxl')
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    grouped = {}
    for _, row in df.iterrows():
        database = cell_str(row.get('database'))
        if not database:
            continue

        raw_table = cell_str(row.get('table'), '*')
        source_s3_prefix = normalize_s3(cell_str(row.get('source_s3_prefix')))
        dest_s3_prefix = normalize_s3(cell_str(row.get('dest_s3_prefix')))

        if not dest_s3_prefix:
            logger.warning(
                f"[parse_excel] Skipping '{database}' — "
                "missing 'dest_s3_prefix'"
            )
            continue

        if not source_s3_prefix:
            # Blank source_s3_prefix means data/metadata already live at the
            # destination path with correct path references — rewrite_table_path
            # is unnecessary. Mirror dest_s3_prefix so downstream code can detect
            # the no-rewrite case via source_prefix == dest_prefix.
            source_s3_prefix = dest_s3_prefix
            logger.info(
                f"[parse_excel] '{database}' has blank source_s3_prefix — "
                f"treating as same-as-dest ({dest_s3_prefix}); rewrite will be skipped"
            )

        key = (database, source_s3_prefix.rstrip('/'), dest_s3_prefix.rstrip('/'))
        if key not in grouped:
            grouped[key] = {'tokens': []}

        for tok in raw_table.split(','):
            tok = tok.strip()
            if tok:
                grouped[key]['tokens'].append(tok)

    configs = []
    for (database, source_s3_prefix, dest_s3_prefix), group in grouped.items():
        unique_tokens = list(dict.fromkeys(group['tokens']))
        if '*' in unique_tokens:
            unique_tokens = ['*']

        configs.append({
            'source_database': database,
            'dest_database': database,
            'source_s3_prefix': source_s3_prefix,
            'dest_s3_prefix': dest_s3_prefix,
            'table_tokens': unique_tokens,
            'run_id': run_id,
        })
        logger.info(
            f"[parse_excel] {database} | "
            f"src={source_s3_prefix} | dest={dest_s3_prefix} | "
            f"tokens={unique_tokens[:5]}"
        )

    if not configs:
        logger.error("[parse_excel] No valid rows found in Excel config")
        return []

    logger.info(f"[parse_excel] Emitting {len(configs)} database config(s)")
    return configs


@task.pyspark(conn_id='spark_default')
def validate_data_presence(db_config: dict, spark, **context) -> dict:
    """Check that destination S3 paths contain both data and metadata files.

    Takes db_config (parse_excel output) and enumerates table dirs under
    dest_s3_prefix using _list_iceberg_tables and _match_tokens.
    """
    from migrator_utils.migrations.shared import (
        _list_iceberg_tables,
        _match_tokens,
    )

    if not isinstance(db_config, dict) or 'run_id' not in db_config:
        logger.warning(f"[validate_data_presence] Skipping invalid input: {type(db_config)}")
        return {}

    dest_prefix = db_config['dest_s3_prefix'].rstrip('/')
    source_prefix = db_config['source_s3_prefix'].rstrip('/')
    tokens = db_config.get('table_tokens', ['*'])
    run_id = db_config['run_id']
    src_db = db_config['source_database']

    available_tables = _list_iceberg_tables(spark, dest_prefix)
    matched = _match_tokens(available_tables, tokens)

    logger.info(
        f"[validate_data_presence] '{src_db}': {len(available_tables)} table(s) found, "
        f"{len(matched)} matched tokens {tokens[:5]}"
    )

    results = []
    for tbl_name in matched:
        dest_path = f"{dest_prefix}/{tbl_name}"

        try:
            from py4j.java_gateway import java_import
            java_import(spark._jvm, 'org.apache.hadoop.fs.*')

            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI(dest_path),
                spark._jsc.hadoopConfiguration()
            )
            path_obj = spark._jvm.org.apache.hadoop.fs.Path(dest_path)

            if not fs.exists(path_obj):
                results.append({
                    'source_table': tbl_name, 'dest_path': dest_path,
                    'status': 'MISSING', 'file_count': 0, 'size_bytes': 0,
                    'error': f'Destination path does not exist: {dest_path}',
                })
                continue

            # Verify metadata directory is present (required for rewrite_table_path)
            metadata_path = spark._jvm.org.apache.hadoop.fs.Path(f"{dest_path}/metadata")
            if not fs.exists(metadata_path):
                results.append({
                    'source_table': tbl_name, 'dest_path': dest_path,
                    'status': 'MISSING', 'file_count': 0, 'size_bytes': 0,
                    'error': (
                        f'Metadata directory missing at {dest_path}/metadata — '
                        'rewrite_table_path requires pre-copied metadata files'
                    ),
                })
                continue

            summary = fs.getContentSummary(path_obj)
            file_count = int(summary.getFileCount())
            size_bytes = int(summary.getLength())

            if file_count == 0:
                results.append({
                    'source_table': tbl_name, 'dest_path': dest_path,
                    'status': 'MISSING', 'file_count': 0, 'size_bytes': 0,
                    'error': f'Destination path exists but contains 0 files: {dest_path}',
                })
            else:
                logger.info(
                    f"[validate_data_presence] CONFIRMED: {src_db}.{tbl_name} | "
                    f"files={file_count} | size={size_bytes / (1024 ** 2):.1f}MB"
                )
                results.append({
                    'source_table': tbl_name, 'dest_path': dest_path,
                    'status': 'CONFIRMED', 'file_count': file_count,
                    'size_bytes': size_bytes, 'error': None,
                })

        except Exception as e:
            logger.error(f"[validate_data_presence] FAILED for {src_db}.{tbl_name}: {e}")
            results.append({
                'source_table': tbl_name, 'dest_path': dest_path,
                'status': 'FAILED', 'file_count': 0, 'size_bytes': 0,
                'error': str(e)[:500],
            })

    failed = [r for r in results if r['status'] == 'FAILED']
    result_dict = {
        'run_id': run_id,
        'source_database': src_db,
        'dest_database': db_config['dest_database'],
        'dest_bucket': db_config.get('dest_bucket', ''),
        'dest_s3_prefix': dest_prefix,
        'source_s3_prefix': source_prefix,
        'presence_results': results,
    }

    if failed:
        context['ti'].xcom_push(key='return_value', value=result_dict)
        raise Exception(
            f"[validate_data_presence] Data presence check FAILED for "
            f"{len(failed)}/{len(results)} table(s) in '{src_db}'"
        )

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_data_presence_in_tracking(presence_result: dict, spark) -> dict:
    """Insert initial tracking records with data presence check results.

    Since this runs before discover_tables, no records exist yet — this task
    INSERTs the initial rows. On reruns the existing INSERT/UPDATE check pattern
    keeps the function idempotent.
    """
    if not isinstance(presence_result, dict) or 'run_id' not in presence_result:
        logger.warning("[update_data_presence_in_tracking] Skipping invalid input")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = presence_result['run_id']
    src_db = presence_result.get('source_database', '')
    dest_db = presence_result.get('dest_database', '')
    dest_bucket = presence_result.get('dest_bucket', '')
    source_s3_prefix = presence_result.get('source_s3_prefix', '')
    dest_s3_prefix = presence_result.get('dest_s3_prefix', '')

    for r in presence_result.get('presence_results', []):
        overall = {
            'CONFIRMED': 'DATA_CONFIRMED',
            'MISSING': 'DATA_MISSING',
            'FAILED': 'FAILED',
        }.get(r['status'], 'FAILED')
        error_msg = (r.get('error') or '').replace("'", "''")[:2000]
        source_table = r['source_table']
        dest_path = r.get('dest_path', '')
        source_s3_location = f"{source_s3_prefix}/{source_table}"

        existing = spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM {tracking_db}.rewrite_migration_table_status
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND source_table = '{source_table}'
        """).collect()[0]['cnt']

        if existing > 0:
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.rewrite_migration_table_status
                SET data_presence_status = '{r['status']}',
                    data_presence_checked_at = current_timestamp(),
                    data_presence_file_count = {r['file_count']},
                    data_presence_size_bytes = {r['size_bytes']},
                    overall_status = '{overall}',
                    error_message = CASE WHEN '{r['status']}' != 'CONFIRMED'
                                         THEN '{error_msg}'
                                         ELSE error_message END,
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{source_table}'
            """, task_label=f"update_data_presence_in_tracking:update:{source_table}")
        else:
            execute_with_iceberg_retry(spark, f"""
                INSERT INTO {tracking_db}.rewrite_migration_table_status (
                    run_id, source_database, source_table, dest_database, dest_bucket,
                    source_s3_location, dest_s3_location,
                    source_s3_prefix, dest_s3_prefix,
                    file_format, is_partitioned, partition_columns, partition_count,
                    schema_json, partitions_json, table_type,
                    source_row_count,
                    data_presence_status, data_presence_checked_at,
                    data_presence_file_count, data_presence_size_bytes,
                    discovery_status, discovery_completed_at, discovery_duration_seconds,
                    table_create_status, table_create_completed_at, table_create_duration_seconds,
                    table_already_existed,
                    validation_status, validation_completed_at, validation_duration_seconds,
                    dest_hive_row_count, dest_partition_count, source_partition_count,
                    row_count_match, partition_count_match, schema_match, path_rewrite_verified,
                    snapshot_count_match, source_snapshot_count, dest_snapshot_count,
                    schema_differences,
                    overall_status, error_message, updated_at
                ) VALUES (
                    '{run_id}', '{src_db}', '{source_table}',
                    '{dest_db}', '{dest_bucket}',
                    '{source_s3_location}', '{dest_path}',
                    '{source_s3_prefix}', '{dest_s3_prefix}',
                    NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL,
                    '{r['status']}', current_timestamp(),
                    {r['file_count']}, {r['size_bytes']},
                    NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL,
                    '{overall}',
                    {'NULL' if r['status'] == 'CONFIRMED' else f"'{error_msg}'"},
                    current_timestamp()
                )
            """, task_label=f"update_data_presence_in_tracking:insert:{source_table}")

    return presence_result


@task.pyspark(conn_id='spark_default')
@track_duration
def discover_tables(presence_result: dict, spark, **context) -> dict:
    """Discover Iceberg tables at the destination and read their metadata.

    Takes presence_result (update_data_presence_in_tracking output) and reads
    metadata only for CONFIRMED tables — iterates over presence_results directly
    instead of listing the filesystem again.

    Always reads schema, partition spec, row count, and snapshot count from
    metadata.json rather than HMS because HMS may not have the table registered
    yet and would not preserve partition transform details (year, month, bucket,
    etc.). For register-only tables (source_prefix == dest_prefix), this same
    metadata read provides the source-side metrics that validate_dest_tables
    later compares against the registered Hive table.
    """
    from migrator_utils.migrations.shared import (
        _extract_partition_spec,
        _extract_row_count,
        _extract_schema,
        _read_iceberg_metadata,
    )

    database = presence_result['dest_database']
    dest_prefix = presence_result['dest_s3_prefix']
    source_prefix = presence_result['source_s3_prefix']

    # Initialise early so the except block can always push partial results to xcom.
    metadata_list = []
    result_dict = {
        **presence_result,
        'tables': metadata_list,
    }

    try:
        confirmed = [
            r for r in presence_result.get('presence_results', [])
            if r.get('status') == 'CONFIRMED'
        ]

        logger.info(
            f"[discover_tables] '{database}': reading metadata for "
            f"{len(confirmed)} CONFIRMED table(s)"
        )

        for r in confirmed:
            tbl_name = r['source_table']
            dest_path = r['dest_path']
            full_name = f"{database}.{tbl_name}"

            try:
                iceberg_meta = _read_iceberg_metadata(spark, dest_path)
                schema = _extract_schema(iceberg_meta)
                partition_spec, is_partitioned = _extract_partition_spec(iceberg_meta)
                row_count = _extract_row_count(iceberg_meta)
                file_format = iceberg_meta.get('properties', {}).get(
                    'write.format.default', 'parquet'
                ).upper()
                format_version = str(iceberg_meta.get('format-version', 2))
                source_snapshot_count = len(iceberg_meta.get('snapshots', []))

                logger.info(
                    f"[discover_tables] {full_name} | fmt={file_format} | "
                    f"rows={row_count} | "
                    f"partitioned={is_partitioned} | snapshots={source_snapshot_count}"
                )

                metadata_list.append({
                    'source_database': database,
                    'source_table': tbl_name,
                    'dest_database': database,
                    'source_location': f"{source_prefix.rstrip('/')}/{tbl_name}",
                    'dest_location': dest_path,
                    'source_s3_prefix': source_prefix,
                    'dest_s3_prefix': dest_prefix,
                    'file_format': file_format,
                    'table_type': 'ICEBERG',
                    'schema': schema,
                    'partition_columns': ','.join(
                        p['source_column'] for p in partition_spec
                    ),
                    'partition_spec_detail': partition_spec,
                    'partitions': [],
                    'partition_count': 0,
                    'is_partitioned': is_partitioned,
                    'source_row_count': row_count,
                    'format_version': format_version,
                    'source_snapshot_count': source_snapshot_count,
                })

            except Exception as e:
                logger.error(f"[discover_tables] FAILED for {full_name}: {e}")
                metadata_list.append({
                    'source_database': database,
                    'source_table': tbl_name,
                    'dest_database': database,
                    'source_location': f"{source_prefix.rstrip('/')}/{tbl_name}",
                    'dest_location': '',
                    'source_s3_prefix': source_prefix,
                    'dest_s3_prefix': dest_prefix,
                    'file_format': 'UNKNOWN',
                    'table_type': 'UNKNOWN',
                    'schema': [],
                    'partition_columns': '',
                    'partition_spec_detail': [],
                    'partitions': [],
                    'partition_count': 0,
                    'is_partitioned': False,
                    'source_row_count': 0,
                    'format_version': '2',
                    'source_snapshot_count': 0,
                    'error': str(e)[:500],
                })

        failed = [t for t in metadata_list if 'error' in t]
        context['ti'].xcom_push(key='return_value', value=result_dict)
        if failed:
            raise Exception(
                f"Discovery failed for {len(failed)}/{len(metadata_list)} table(s) in "
                f"'{database}': "
                + ', '.join(t['source_table'] for t in failed[:3])
            )
        return result_dict

    except Exception:
        # Outer failures (bad credentials, unreachable bucket, etc.) would otherwise
        # leave the downstream record task with no xcom to read. Always push here so
        # update_discovered_tables_in_tracking gets whatever was collected before the failure.
        context['ti'].xcom_push(key='return_value', value=result_dict)
        raise


@task.pyspark(conn_id='spark_default')
def update_discovered_tables_in_tracking(discovery: dict, spark) -> dict:
    """Update existing tracking records with discovered table metadata.

    Records were already inserted by update_data_presence_in_tracking, so this
    task only UPDATEs — no INSERT branch needed.
    """
    if not isinstance(discovery, dict) or 'tables' not in discovery:
        logger.warning(f"[update_discovered_tables_in_tracking] Skipping invalid input: {type(discovery)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = discovery['run_id']
    duration = discovery.get('_task_duration', 0.0)

    for t in discovery['tables']:
        schema_json = json.dumps(t.get('schema', [])).replace("'", "''")
        parts_json = json.dumps(t.get('partitions', [])).replace("'", "''")
        has_error = 'error' in t
        disc_status = 'FAILED' if has_error else 'COMPLETED'
        disc_error_sql = f"'{t['error'][:2000].replace(chr(39), chr(39)*2)}'" if has_error else 'NULL'

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.rewrite_migration_table_status
            SET discovery_status = '{disc_status}',
                discovery_completed_at = current_timestamp(),
                discovery_duration_seconds = {duration},
                source_s3_location = '{t['source_location']}',
                dest_s3_location = '{t['dest_location']}',
                file_format = '{t['file_format']}',
                table_type = '{t['table_type']}',
                source_row_count = {t['source_row_count']},
                partition_count = {t['partition_count']},
                source_partition_count = {t['partition_count']},
                source_snapshot_count = {t.get('source_snapshot_count', 0)},
                schema_json = '{schema_json}',
                partitions_json = '{parts_json}',
                is_partitioned = {str(t['is_partitioned']).lower()},
                partition_columns = '{t['partition_columns']}',
                overall_status = CASE
                    WHEN '{disc_status}' = 'FAILED' THEN 'FAILED'
                    ELSE 'DISCOVERED'
                END,
                error_message = CASE
                    WHEN '{disc_status}' = 'FAILED' THEN {disc_error_sql}
                    ELSE error_message
                END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{t['source_database']}'
              AND source_table = '{t['source_table']}'
        """, task_label=f"update_discovered_tables_in_tracking:{t['source_table']}")

    return discovery


@task.pyspark(conn_id='spark_default')
@track_duration
def rewrite_and_register_tables(presence_result: dict, spark, **context) -> dict:
    """Register destination tables in HMS.

    Two branches based on the (source_prefix, dest_prefix) relationship:

    Register-only (source_prefix == dest_prefix, or blank source mirrored to
    dest by parse_excel) — metadata at dest already references dest paths, no
    rewrite needed:
      1. Drop from HMS if already registered.
      2. Resolve the latest metadata.json at dest_path via Hadoop FS.
      3. Call register_table with that metadata file.

    Rewrite (source_prefix != dest_prefix) — metadata still references source
    paths, must be rewritten before HMS sees them:
      1. Drop from HMS if already registered.
      2. Temporarily register using the source metadata file so
         rewrite_table_path can locate the original source paths.
      3. Call rewrite_table_path — rewrites every path reference (all snapshots)
         source/→dest/ and writes new metadata into dest_path/metadata.
         Use latest_version from the result (not _resolve_metadata_file) because
         version-hint.text is stale after the rewrite.
      4. Drop temporary registration.
      5. Permanently register in HMS via register_table using the rewritten
         metadata now present at dest_path/metadata.
    """
    from migrator_utils.migrations.shared import _rebase_table_path, _resolve_metadata_file

    if not isinstance(presence_result, dict) or 'tables' not in presence_result:
        logger.warning("[rewrite_and_register_tables] Skipping invalid input")
        return {}

    dest_db = presence_result['dest_database']
    dest_prefix = presence_result.get('dest_s3_prefix', '').rstrip('/')
    source_prefix = presence_result.get('source_s3_prefix', '').rstrip('/')
    tables = presence_result['tables']

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_db}")

    presence_map = {
        r['source_table']: r
        for r in presence_result.get('presence_results', [])
    }

    results = []
    for t in tables:
        tbl = t['source_table']
        p_status = presence_map.get(tbl, {}).get('status', 'UNKNOWN')

        if p_status != 'CONFIRMED':
            logger.info(
                f"[rewrite_and_register_tables] Skipping {dest_db}.{tbl}, "
                f"data_presence={p_status}"
            )
            results.append({
                'source_table': tbl,
                'status': 'SKIPPED',
                'existed': False,
                'error': f'Data not present at destination (status={p_status})',
            })
            continue

        dest_path = t['dest_location']
        full_name = f"{dest_db}.{tbl}"

        try:
            # Step 1: Drop from HMS if already registered
            exists = False
            try:
                for _ in spark.sql(f"DESCRIBE FORMATTED {full_name}").collect():
                    pass
                exists = True
            except Exception:
                pass

            if exists:
                spark.sql(f"DROP TABLE {full_name}")
                logger.info(f"[rewrite_and_register_tables] Dropped {full_name} from HMS")

            # When source_prefix == dest_prefix (or source was left blank in
            # Excel and mirrored to dest in parse_excel), the metadata at
            # dest_path already references dest paths — no rewrite needed.
            # Resolve the latest metadata file via Hadoop FS and register
            # directly. discover_tables already populated source_* metrics by
            # reading this same metadata.json, so validation has source-side
            # numbers to compare against the registered Hive table.
            skip_rewrite = (not source_prefix) or (source_prefix == dest_prefix)

            if skip_rewrite:
                metadata_file = _resolve_metadata_file(spark, dest_path)
                spark.sql(f"""
                    CALL spark_catalog.system.register_table(
                        table => '{full_name}',
                        metadata_file => '{metadata_file}'
                    )
                """)
                logger.info(
                    f"[rewrite_and_register_tables] Registered {full_name} "
                    f"via {metadata_file} (no rewrite — source==dest)"
                )
            else:
                # Step 2: Temporary HMS registration using the SOURCE metadata file.
                # rewrite_table_path rewrites source/ → dest/ paths and writes the
                # new metadata directly to dest_path/metadata, so the source metadata
                # must be registered here for it to find the original path references.
                source_table_path = _rebase_table_path(dest_path, dest_prefix, source_prefix)
                source_metadata_file = _resolve_metadata_file(spark, source_table_path)
                spark.sql(f"""
                    CALL spark_catalog.system.register_table(
                        table => '{full_name}',
                        metadata_file => '{source_metadata_file}'
                    )
                """)
                logger.info(
                    f"[rewrite_and_register_tables] Temporarily registered "
                    f"{full_name} via source metadata at {source_metadata_file}"
                )

                # Step 3: Rewrite metadata — rewrites all source/ path references to
                # dest/ across every snapshot (full history preserved). New manifest
                # and metadata files are written to dest_path/metadata so they land
                # alongside the existing metadata files, not in the table root.
                # version-hint.text is NOT updated by the procedure, so we use
                # latest_version from the result rather than re-resolving via
                # _resolve_metadata_file, which would return the pre-rewrite file.
                rewrite_result = spark.sql(f"""
                    CALL spark_catalog.system.rewrite_table_path(
                        table            => '{full_name}',
                        source_prefix    => '{source_prefix}',
                        target_prefix    => '{dest_prefix}',
                        staging_location => '{dest_prefix}'
                    )
                """)
                latest_version = rewrite_result.collect()[0]['latest_version']
                new_metadata_file = f"{dest_path}/metadata/{latest_version}"
                logger.info(
                    f"[rewrite_and_register_tables] rewrite_table_path completed for {full_name}; "
                    f"new metadata: {new_metadata_file}"
                )

                # Step 4: Drop temporary registration
                spark.sql(f"DROP TABLE {full_name}")

                # Step 5: Permanent HMS registration using the rewritten metadata file.
                spark.sql(f"""
                    CALL spark_catalog.system.register_table(
                        table => '{full_name}',
                        metadata_file => '{new_metadata_file}'
                    )
                """)
                logger.info(
                    f"[rewrite_and_register_tables] Registered {full_name} "
                    f"via {new_metadata_file}"
                )

            imported_row_count = spark.sql(
                f"SELECT COUNT(*) as c FROM {full_name}"
            ).collect()[0]['c']

            imported_partition_count = 0
            if t.get('partition_spec_detail'):
                with contextlib.suppress(Exception):
                    imported_partition_count = spark.sql(
                        f"SELECT * FROM {full_name}.partitions"
                    ).count()

            logger.info(
                f"[rewrite_and_register_tables] {full_name} registered: "
                f"{imported_row_count} rows, {imported_partition_count} partitions"
            )

            results.append({
                'source_table': tbl,
                'status': 'COMPLETED',
                'existed': exists,
                'imported_row_count': imported_row_count,
                'imported_partition_count': imported_partition_count,
                'error': None,
            })

        except Exception as e:
            error_msg = str(e)[:2000]
            logger.error(f"[rewrite_and_register_tables] FAILED for {full_name}: {error_msg}")
            results.append({
                'source_table': tbl,
                'status': 'FAILED',
                'existed': False,
                'error': error_msg,
            })

    failed = [r for r in results if r['status'] == 'FAILED']
    result_dict = {**presence_result, 'table_results': results, '_has_failures': bool(failed)}
    context['ti'].xcom_push(key='return_value', value=result_dict)

    if failed:
        raise Exception(
            f"Table creation failed for {len(failed)}/{len(results)} "
            f"table(s) in '{dest_db}'"
        )
    return result_dict


@task.pyspark(conn_id='spark_default')
def update_rewrite_and_register_in_tracking(table_result: dict, spark) -> dict:
    """Update tracking table with table creation results."""
    if not isinstance(table_result, dict) or 'run_id' not in table_result:
        logger.warning("[update_rewrite_and_register_in_tracking] Skipping invalid input")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = table_result['run_id']
    dest_db = table_result['dest_database']
    src_db = table_result['source_database']
    src_prefix = (table_result.get('source_s3_prefix') or '').rstrip('/')
    dest_prefix = (table_result.get('dest_s3_prefix') or '').rstrip('/')
    table_dur = table_result.get('_task_duration', 0.0)

    for r in table_result.get('table_results', []):
        overall = {
            'COMPLETED': 'TABLE_CREATED',
            'SKIPPED': 'DATA_MISSING',
            'FAILED': 'FAILED',
        }.get(r['status'], 'FAILED')
        error_msg = (r.get('error') or '').replace("'", "''")[:2000]

        imported_rc = r.get('imported_row_count')
        imported_pc = r.get('imported_partition_count')
        metric_updates = []
        if imported_rc is not None:
            metric_updates.append(f"source_row_count = {int(imported_rc)}")
        if imported_pc is not None:
            metric_updates.append(f"source_partition_count = {int(imported_pc)}")
        metric_sql = ",".join(metric_updates) + "," if metric_updates else ""

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.rewrite_migration_table_status
            SET table_create_status = '{r['status']}',
                table_create_completed_at = current_timestamp(),
                table_create_duration_seconds = {table_dur},
                table_already_existed = {str(r.get('existed', False)).lower()},
                {metric_sql}
                overall_status = CASE
                    WHEN overall_status = 'FAILED' THEN overall_status
                    WHEN overall_status = 'DATA_MISSING' THEN overall_status
                    ELSE '{overall}'
                END,
                error_message = CASE
                    WHEN '{r['status']}' = 'FAILED' THEN '{error_msg}'
                    ELSE error_message
                END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND dest_database = '{dest_db}'
              AND source_table = '{r['source_table']}'
        """, task_label=f"update_rewrite_and_register_in_tracking:{r['source_table']}")

    # Scope catch-all to this config's dest_s3_prefix so parallel mapped-task
    # instances sharing the same source_database don't race on each other's rows.
    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.rewrite_migration_table_status
        SET table_create_status = 'FAILED',
            overall_status = 'FAILED',
            error_message = COALESCE(error_message, 'Table creation task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND source_s3_prefix = '{src_prefix}'
          AND dest_s3_prefix = '{dest_prefix}'
          AND table_create_status IS NULL
          AND data_presence_status = 'CONFIRMED'
    """, task_label="update_rewrite_and_register_in_tracking:catchall")

    return table_result


@task.pyspark(conn_id='spark_default')
@track_duration
def validate_dest_tables(table_result: dict, spark, **context) -> dict:
    """Validate destination tables — row counts, partition counts, schema comparison."""
    if not isinstance(table_result, dict) or 'tables' not in table_result:
        logger.warning("[validate_dest_tables] Skipping invalid input")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = table_result['run_id']
    src_db = table_result['source_database']
    dest_db = table_result['dest_database']

    validation_results = []

    for t in table_result['tables']:
        tbl = t['source_table']
        dest_tbl = f"{dest_db}.{tbl}"

        upstream = spark.sql(f"""
            SELECT table_create_status, data_presence_status, overall_status
            FROM {tracking_db}.rewrite_migration_table_status
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND source_table = '{tbl}'
        """).collect()

        if upstream:
            row = upstream[0]
            if row['table_create_status'] in ('FAILED', 'SKIPPED') or row['data_presence_status'] != 'CONFIRMED':
                validation_results.append({
                    'source_table': tbl, 'status': 'SKIPPED',
                    'error': f"Skipped — upstream status: {row['overall_status']}",
                })
                continue

        logger.info(f"[validate_dest_tables] Validating {dest_tbl}")

        try:
            src_metrics = spark.sql(f"""
                SELECT source_row_count, source_partition_count, source_snapshot_count
                FROM {tracking_db}.rewrite_migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
            """).collect()

            if not src_metrics:
                validation_results.append({
                    'source_table': tbl, 'status': 'SKIPPED',
                    'error': 'Source metrics not found in tracking table',
                })
                continue

            src_row_count = src_metrics[0]['source_row_count'] or 0
            src_partition_count = (
                src_metrics[0]['source_partition_count'] or t.get('partition_count', 0)
            )
            src_snapshot_count = src_metrics[0]['source_snapshot_count'] or 0

            dest_row_count = spark.sql(
                f"SELECT COUNT(*) as c FROM {dest_tbl}"
            ).collect()[0]['c']

            dest_partition_count = 0
            if t.get('is_partitioned'):
                with contextlib.suppress(Exception):
                    dest_partition_count = spark.sql(
                        f"SELECT * FROM {dest_tbl}.partitions"
                    ).count()

            src_schema = {c['name'].lower(): c['type'].lower() for c in t.get('schema', [])}
            dest_schema = {
                r.col_name.lower(): (r.data_type or '').lower()
                for r in spark.sql(f"DESCRIBE {dest_tbl}").collect()
                if r.col_name
                and not r.col_name.startswith('#')
                and not r.col_name.strip().lower().startswith('part ')
                and (r.data_type or '').strip() != ''
            }

            schema_match = True
            schema_diffs = []
            for cn, ct in src_schema.items():
                if cn not in dest_schema:
                    schema_match = False
                    schema_diffs.append(f"Missing column: {cn}")
                elif dest_schema[cn] != ct:
                    schema_match = False
                    schema_diffs.append(
                        f"Type mismatch {cn}: source={ct} dest={dest_schema[cn]}"
                    )
            for cn in dest_schema:
                if cn not in src_schema:
                    schema_match = False
                    schema_diffs.append(f"Extra column in dest: {cn}")

            row_count_match = src_row_count == dest_row_count
            partition_count_match = src_partition_count == dest_partition_count

            # Path rewrite check — query {dest_tbl}.snapshots via SQL and verify
            # that NO snapshot's manifest_list references the source prefix.
            # This checks ALL snapshots (full history), not just the current one.
            # When source_prefix == dest_prefix the table was registered without
            # rewrite_table_path, so this check is N/A (manifests legitimately
            # live under the shared prefix).
            path_rewrite_verified = True
            src_prefix = t.get('source_s3_prefix', '').rstrip('/')
            dest_prefix = t.get('dest_s3_prefix', '').rstrip('/')
            rewrite_skipped = (not src_prefix) or (src_prefix == dest_prefix)
            dest_snapshot_count = 0
            try:
                snaps_df = spark.sql(f"SELECT manifest_list FROM {dest_tbl}.snapshots")
                dest_snapshot_count = snaps_df.count()
                if src_prefix and not rewrite_skipped:
                    stale = snaps_df.filter(
                        snaps_df.manifest_list.startswith(src_prefix)
                    ).count()
                    if stale > 0:
                        path_rewrite_verified = False
                        logger.warning(
                            f"[validate_dest_tables] {dest_tbl}: {stale} snapshot(s) "
                            f"still reference source prefix"
                        )
            except Exception as path_e:
                logger.warning(
                    f"[validate_dest_tables] Could not verify path rewrite "
                    f"for {dest_tbl}: {path_e}"
                )
                path_rewrite_verified = False

            snapshot_count_match = (
                src_snapshot_count == dest_snapshot_count
                if src_snapshot_count > 0 else True
            )

            match_str = (
                f"rows={'✓' if row_count_match else '✗'} "
                f"parts={'✓' if partition_count_match else '⚠'} "
                f"schema={'✓' if schema_match else '✗'} "
                f"paths={'✓' if path_rewrite_verified else '✗'} "
                f"snapshots={'✓' if snapshot_count_match else '✗'}({dest_snapshot_count})"
            )
            logger.info(f"[validate_dest_tables] {dest_tbl} | {match_str}")

            mismatch_parts = []
            if not row_count_match:
                mismatch_parts.append(
                    f"Row count mismatch: source={src_row_count} dest={dest_row_count}"
                )
            if not partition_count_match:
                mismatch_parts.append(
                    f"Partition mismatch: "
                    f"source={src_partition_count} dest={dest_partition_count}"
                )
            if not schema_match:
                mismatch_parts.append(
                    f"Schema differences: {'; '.join(schema_diffs[:3])}"
                )
            if not path_rewrite_verified:
                mismatch_parts.append(
                    f"Snapshot manifest(s) still reference source prefix ({src_prefix})"
                )
            if not snapshot_count_match:
                mismatch_parts.append(
                    f"Snapshot count mismatch: "
                    f"source={src_snapshot_count} dest={dest_snapshot_count}"
                )

            validation_results.append({
                'source_table': tbl,
                'status': 'COMPLETED',
                'source_row_count': src_row_count,
                'dest_hive_row_count': dest_row_count,
                'source_partition_count': src_partition_count,
                'dest_partition_count': dest_partition_count,
                'row_count_match': row_count_match,
                'partition_count_match': partition_count_match,
                'schema_match': schema_match,
                'path_rewrite_verified': path_rewrite_verified,
                'source_snapshot_count': src_snapshot_count,
                'dest_snapshot_count': dest_snapshot_count,
                'snapshot_count_match': snapshot_count_match,
                'schema_differences': '; '.join(schema_diffs),
                'error': '; '.join(mismatch_parts) if mismatch_parts else None,
            })

        except Exception as e:
            logger.error(f"[validate_dest_tables] FAILED for {dest_tbl}: {e}")
            validation_results.append({
                'source_table': tbl, 'status': 'FAILED', 'error': str(e)[:2000],
            })

    failed = [v for v in validation_results if v['status'] == 'FAILED']
    result_dict = {**table_result, 'validation_results': validation_results, '_has_failures': bool(failed)}
    context['ti'].xcom_push(key='return_value', value=result_dict)

    if failed:
        raise Exception(
            f"Validation failed for {len(failed)}/{len(validation_results)} table(s)"
        )
    return result_dict


@task.pyspark(conn_id='spark_default')
def update_validation_in_tracking(validation_result: dict, spark) -> dict:
    """Update tracking table with validation results."""
    if not isinstance(validation_result, dict) or 'run_id' not in validation_result:
        logger.warning("[update_validation_in_tracking] Skipping invalid input")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = validation_result['run_id']
    src_db = validation_result['source_database']
    dest_db = validation_result['dest_database']
    src_prefix = (validation_result.get('source_s3_prefix') or '').rstrip('/')
    dest_prefix = (validation_result.get('dest_s3_prefix') or '').rstrip('/')
    val_dur = validation_result.get('_task_duration', 0.0)

    for v in validation_result.get('validation_results', []):
        if v['status'] != 'COMPLETED':
            continue

        schema_diffs = (v.get('schema_differences') or '').replace("'", "''")[:2000]
        error_msg = (v.get('error') or '').replace("'", "''")[:2000]

        is_validated = (
            v.get('row_count_match', False) and
            v.get('partition_count_match', False) and
            v.get('schema_match', False) and
            v.get('path_rewrite_verified', False) and
            v.get('snapshot_count_match', False)
        )
        final_status = 'VALIDATED' if is_validated else 'VALIDATION_FAILED'
        error_message_sql = 'NULL' if is_validated else f"'{error_msg}'"

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.rewrite_migration_table_status
            SET validation_status = '{v['status']}',
                validation_completed_at = current_timestamp(),
                validation_duration_seconds = {val_dur},
                dest_hive_row_count = {v.get('dest_hive_row_count', 0)},
                dest_partition_count = {v.get('dest_partition_count', 0)},
                source_partition_count = {v.get('source_partition_count', 0)},
                row_count_match = {str(v.get('row_count_match', False)).lower()},
                partition_count_match = {str(v.get('partition_count_match', False)).lower()},
                schema_match = {str(v.get('schema_match', False)).lower()},
                path_rewrite_verified = {str(v.get('path_rewrite_verified', False)).lower()},
                snapshot_count_match = {str(v.get('snapshot_count_match', False)).lower()},
                source_snapshot_count = {v.get('source_snapshot_count', 0)},
                dest_snapshot_count = {v.get('dest_snapshot_count', 0)},
                schema_differences = '{schema_diffs}',
                overall_status = CASE
                    WHEN overall_status = 'FAILED' THEN overall_status
                    ELSE '{final_status}'
                END,
                error_message = CASE
                    WHEN overall_status = 'FAILED' THEN error_message
                    ELSE {error_message_sql}
                END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND dest_database = '{dest_db}'
              AND source_table = '{v['source_table']}'
        """, task_label=f"update_validation_in_tracking:{v['source_table']}")

    for v in validation_result.get('validation_results', []):
        if v.get('status') == 'FAILED' and v.get('error'):
            per_err = str(v['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.rewrite_migration_table_status
                SET validation_status = 'FAILED',
                    overall_status = 'VALIDATION_FAILED',
                    path_rewrite_verified = false,
                    error_message = '{per_err}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{v['source_table']}'
                  AND validation_status IS NULL
            """, task_label=f"update_validation_in_tracking:failure_patch:{v['source_table']}")

    # Scope catch-all to this config's dest_s3_prefix so parallel mapped-task
    # instances sharing the same source_database don't race on each other's rows.
    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.rewrite_migration_table_status
        SET validation_status = 'SKIPPED',
            overall_status = CASE
                WHEN overall_status = 'FAILED' THEN 'FAILED'
                ELSE 'VALIDATION_FAILED'
            END,
            error_message = COALESCE(error_message, 'Validation task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id  = '{run_id}'
          AND source_database = '{src_db}'
          AND source_s3_prefix = '{src_prefix}'
          AND dest_s3_prefix = '{dest_prefix}'
          AND data_presence_status = 'CONFIRMED'
          AND table_create_status = 'COMPLETED'
          AND validation_status IS NULL
    """, task_label="update_validation_in_tracking:catchall")

    return validation_result


@task.pyspark(conn_id='spark_default')
def generate_html_report(run_id: str, spark) -> dict:
    """Generate HTML report for the rewrite_table_path migration."""
    from datetime import datetime

    config = get_config()
    tracking_db = config['tracking_database']
    report_location = config['report_output_location']

    run_info = spark.sql(f"""
        SELECT * FROM {tracking_db}.rewrite_migration_runs WHERE run_id = '{run_id}'
    """).collect()
    run_row = run_info[0] if run_info else None

    table_status = spark.sql(f"""
        SELECT * FROM {tracking_db}.rewrite_migration_table_status
        WHERE run_id = '{run_id}'
        ORDER BY source_database, source_table
    """).collect()

    def _is_no_rewrite(t):
        sp = (t.source_s3_prefix or '').rstrip('/')
        dp = (t.dest_s3_prefix or '').rstrip('/')
        return bool(sp) and bool(dp) and sp == dp

    _NR_TAG_TITLE = "source_s3_prefix == dest_s3_prefix — rewrite_table_path was skipped"
    _NR_TAG_HTML = f'<span class="nr-tag" title="{_NR_TAG_TITLE}">register-only</span>'

    total_tables = len(table_status)
    successful_tables = sum(1 for t in table_status if t.overall_status == 'VALIDATED')
    failed_tables = sum(1 for t in table_status if 'FAILED' in (t.overall_status or ''))
    missing_tables = sum(1 for t in table_status if t.overall_status == 'DATA_MISSING')
    no_rewrite_tables = sum(1 for t in table_status if _is_no_rewrite(t))
    dag_run_id = run_row.dag_run_id if run_row else 'N/A'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Iceberg Catalog Migration Report — {run_id}</title>
  <style>
    body {{font-family:'Segoe UI',sans-serif;margin:0;padding:20px;background:#f5f5f5}}
    .container {{max-width:1400px;margin:0 auto;background:white;padding:30px;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,.1)}}
    h1 {{color:#2c3e50;border-bottom:3px solid #9b59b6;padding-bottom:10px}}
    h2 {{color:#34495e;margin-top:30px;border-bottom:2px solid #ecf0f1;padding-bottom:8px}}
    .grid {{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:20px;margin:20px 0}}
    .card {{color:white;padding:20px;border-radius:8px;box-shadow:0 4px 6px rgba(0,0,0,.1)}}
    .c1{{background:linear-gradient(135deg,#8e44ad,#9b59b6)}}
    .c2{{background:linear-gradient(135deg,#11998e,#38ef7d)}}
    .c3{{background:linear-gradient(135deg,#f093fb,#f5576c)}}
    .c4{{background:linear-gradient(135deg,#4facfe,#00f2fe)}}
    .c5{{background:linear-gradient(135deg,#f7971e,#ffd200)}}
    .card h3{{margin:0 0 10px;font-size:13px;opacity:.9}}
    .card .v{{font-size:30px;font-weight:bold;margin:0}}
    table {{width:100%;border-collapse:collapse;margin:20px 0;font-size:14px}}
    th {{background:#34495e;color:white;padding:12px;text-align:left}}
    td {{padding:10px 12px;border-bottom:1px solid #ecf0f1}}
    tr:hover{{background:#f8f9fa}}
    .badge {{padding:4px 12px;border-radius:12px;font-size:12px;font-weight:bold;display:inline-block}}
    .ok{{background:#d4edda;color:#155724}} .fail{{background:#f8d7da;color:#721c24}}
    .warn{{background:#fff3cd;color:#856404}}
    .vp{{color:#27ae60;font-weight:bold}} .vf{{color:#e74c3c;font-weight:bold}}
    .vw{{color:#856404;font-weight:bold}} .metric{{font-weight:bold;color:#2980b9}}
    .ts{{color:#95a5a6;font-size:12px}} .divider{{margin:40px 0;border-top:2px dashed #ecf0f1}}
    .nr-tag {{background:#ede7f6;color:#5e35b1;border:1px solid #d1c4e9;padding:2px 8px;
             border-radius:10px;font-size:10px;font-weight:600;letter-spacing:.3px;
             text-transform:uppercase;margin-left:6px;vertical-align:middle}}
    .na {{color:#7f8c8d;font-style:italic}}
  </style>
</head>
<body><div class="container">
<h1>Iceberg Catalog Migration Report</h1>
<div class="ts">
  Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
  Run ID: <strong>{run_id}</strong>&nbsp;|&nbsp;DAG Run: <strong>{dag_run_id}</strong>
</div>

<h2>Migration Summary</h2>
<div class="grid">
  <div class="card c1"><h3>TOTAL TABLES</h3><p class="v">{total_tables}</p></div>
  <div class="card c2"><h3>VALIDATED</h3><p class="v">{successful_tables}</p></div>
  <div class="card c3"><h3>FAILED</h3><p class="v">{failed_tables}</p></div>
  <div class="card c5"><h3>DATA MISSING</h3><p class="v">{missing_tables}</p></div>
  <div class="card c1"><h3>REGISTER-ONLY</h3><p class="v">{no_rewrite_tables}</p></div>
</div>

<div class="divider"></div>
<h2>Data Presence & Metadata Check</h2>
<table>
  <thead><tr>
    <th>Database</th><th>Table</th><th>Dest S3 Path</th>
    <th>Presence Status</th><th>Files Found</th><th>Size (MB)</th>
  </tr></thead><tbody>
"""
    for t in table_status:
        p_status = t.data_presence_status or 'N/A'
        p_class = 'ok' if p_status == 'CONFIRMED' else ('warn' if p_status == 'MISSING' else 'fail')
        nr_badge = _NR_TAG_HTML if _is_no_rewrite(t) else ''
        html += f"""
  <tr>
    <td>{t.source_database}</td>
    <td><strong>{t.source_table}</strong>{nr_badge}</td>
    <td class="ts">{(t.dest_s3_location or '')[:80]}</td>
    <td><span class="badge {p_class}">{p_status}</span></td>
    <td class="metric">{(t.data_presence_file_count or 0):,}</td>
    <td class="metric">{(t.data_presence_size_bytes or 0) / (1024 ** 2):.2f}</td>
  </tr>"""

    html += """
  </tbody></table>

<div class="divider"></div>
<h2>Table Migration Details</h2>
<table>
  <thead><tr>
    <th>Database</th><th>Table</th><th>Format</th><th>Partitioned</th>
    <th>Overall Status</th><th>Discovery</th><th>Table Create</th><th>Validation</th>
  </tr></thead><tbody>
"""
    for t in table_status:
        os_val = t.overall_status or ''
        os_class = 'ok' if os_val == 'VALIDATED' else ('warn' if os_val in ('DATA_MISSING', 'TABLE_CREATED') else 'fail')
        d_dur = f"{t.discovery_duration_seconds:.1f}s" if t.discovery_duration_seconds else 'N/A'
        c_dur = f"{t.table_create_duration_seconds:.1f}s" if t.table_create_duration_seconds else 'N/A'
        v_dur = f"{t.validation_duration_seconds:.1f}s" if t.validation_duration_seconds else 'N/A'
        part_str = f"Yes ({t.source_partition_count or 0})" if t.is_partitioned else 'No'
        nr_badge = _NR_TAG_HTML if _is_no_rewrite(t) else ''
        html += f"""
  <tr>
    <td>{t.source_database}</td>
    <td><strong>{t.source_table}</strong>{nr_badge}</td>
    <td>{t.file_format}</td>
    <td>{part_str}</td>
    <td><span class="badge {os_class}">{os_val}</span></td>
    <td class="ts">{d_dur}</td>
    <td class="ts">{c_dur}{'&nbsp;<small>(existed)</small>' if t.table_already_existed else ''}</td>
    <td class="ts">{v_dur}</td>
  </tr>"""

    html += """
  </tbody></table>

<div class="divider"></div>
<h2>Validation Results</h2>
<table>
  <thead><tr>
    <th>Database</th><th>Table</th>
    <th>Source Rows</th><th>Dest Rows</th><th>Row Match</th>
    <th>Src Partitions</th><th>Dest Partitions</th><th>Partition Match</th>
    <th>Schema Match</th>
    <th>Src Snapshots</th><th>Dest Snapshots</th><th>Snapshot Match</th>
    <th>Path Rewrite</th>
  </tr></thead><tbody>
"""
    for t in table_status:
        if not t.validation_status or t.validation_status == 'SKIPPED':
            continue
        rm, pm, sm, pr, scm = (
            t.row_count_match, t.partition_count_match,
            t.schema_match, t.path_rewrite_verified,
            t.snapshot_count_match,
        )
        no_rewrite = _is_no_rewrite(t)
        nr_badge = _NR_TAG_HTML if no_rewrite else ''
        if no_rewrite:
            pr_cell = '<td class="na">— N/A</td>'
        else:
            pr_cell = (
                f'<td class="{"vp" if pr else "vf"}">'
                f'{"✓ PASS" if pr else "✗ FAIL"}</td>'
            )
        html += f"""
  <tr>
    <td>{t.source_database}</td>
    <td><strong>{t.source_table}</strong>{nr_badge}</td>
    <td class="metric">{(t.source_row_count or 0):,}</td>
    <td class="metric">{(t.dest_hive_row_count or 0):,}</td>
    <td class="{'vp' if rm else 'vf'}">{'✓ PASS' if rm else '✗ FAIL'}</td>
    <td class="metric">{t.source_partition_count or 0}</td>
    <td class="metric">{t.dest_partition_count or 0}</td>
    <td class="{'vp' if pm else 'vw'}">{'✓ PASS' if pm else '⚠ WARN'}</td>
    <td class="{'vp' if sm else 'vf'}">{'✓ PASS' if sm else '✗ FAIL'}</td>
    <td class="metric">{t.source_snapshot_count or 0}</td>
    <td class="metric">{t.dest_snapshot_count or 0}</td>
    <td class="{'vp' if scm else 'vf'}">{'✓ PASS' if scm else '✗ FAIL'}</td>
    {pr_cell}
  </tr>"""

    html += """
  </tbody></table>
<div style="margin-top:50px;padding-top:20px;border-top:1px solid #ecf0f1;color:#95a5a6;font-size:12px">
  <p>Auto-generated by the Iceberg Catalog Migration DAG.</p>
</div></div></body></html>"""

    report_path = f"{report_location}/{run_id}_rewrite_report.html"
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(report_path), hadoop_conf
    )
    stream = fs.create(spark._jvm.org.apache.hadoop.fs.Path(report_path), True)
    stream.write(html.encode('utf-8'))
    stream.close()
    logger.info(f"[generate_html_report] Report written to {report_path}")
    return {'report_path': report_path}


@task.pyspark(conn_id='spark_default')
def send_report_email(report_result: dict, run_id: str, spark) -> dict:
    """Send rewrite migration HTML report via email."""
    import os
    import tempfile

    from airflow.utils.email import send_email

    config = get_config()
    smtp_conn_id = config.get('smtp_conn_id', 'smtp_default')
    recipients_str = config.get('email_recipients', '')

    if not recipients_str:
        logger.warning("[send_report_email] No recipients configured. Skipping.")
        return {'sent': False, 'reason': 'no_recipients'}

    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
    report_path = report_result.get('report_path', '')

    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(report_path), hadoop_conf
        )
        reader = spark._jvm.java.io.BufferedReader(
            spark._jvm.java.io.InputStreamReader(
                fs.open(spark._jvm.org.apache.hadoop.fs.Path(report_path)), "UTF-8"
            )
        )
        lines, line = [], reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        html_content = "\n".join(lines)

        tmp = tempfile.NamedTemporaryFile(
            mode='w', suffix='.html',
            prefix=f'{run_id}_rewrite_report_', delete=False
        )
        tmp.write(html_content)
        tmp.close()

        send_email(
            to=recipients,
            subject=f"Iceberg Catalog Migration Report — {run_id}",
            html_content=(
                f"<p>Iceberg rewrite_table_path migration report for run "
                f"<strong>{run_id}</strong> is attached.</p>"
            ),
            files=[tmp.name],
            conn_id=smtp_conn_id,
        )
        os.unlink(tmp.name)
        logger.info(f"[send_report_email] Report sent to: {recipients}")
        return {'sent': True, 'recipients': recipients, 'report_path': report_path}
    except Exception as e:
        logger.error(f"[send_report_email] Failed: {e}")
        raise Exception(f"Failed to send rewrite migration report email: {e}") from e


@task.pyspark(conn_id='spark_default')
def finalize_run(run_id: str, spark) -> dict:
    """Aggregate final statistics and mark the run complete."""
    config = get_config()
    tracking_db = config['tracking_database']

    stats = {'total': 0, 'successful': 0, 'failed': 0, 'missing': 0}
    final_status = 'FAILED'

    try:
        result = spark.sql(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN overall_status = 'VALIDATED' THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN overall_status LIKE '%FAIL%' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN overall_status = 'DATA_MISSING' THEN 1 ELSE 0 END) as missing
            FROM {tracking_db}.rewrite_migration_table_status
            WHERE run_id = '{run_id}'
        """).collect()

        if result and result[0]['total']:
            stats = {
                'total': result[0]['total'] or 0,
                'successful': result[0]['successful'] or 0,
                'failed': result[0]['failed'] or 0,
                'missing': result[0]['missing'] or 0,
            }
            if stats['failed'] == 0 and stats['missing'] == 0:
                final_status = 'COMPLETED'
            elif stats['failed'] == 0:
                final_status = 'COMPLETED_WITH_MISSING'
            else:
                final_status = 'COMPLETED_WITH_FAILURES'
    except Exception as e:
        logger.error(f"[finalize_run] Error querying tracking table: {e}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.rewrite_migration_runs
        SET status = '{final_status}',
            completed_at = current_timestamp(),
            total_tables = {stats['total']},
            successful_tables = {stats['successful']},
            failed_tables = {stats['failed']},
            missing_tables = {stats['missing']}
        WHERE run_id = '{run_id}'
    """, task_label="finalize_run:update")

    logger.info(
        f"[finalize_run] Run '{run_id}' → {final_status} | "
        f"total={stats['total']} validated={stats['successful']} "
        f"failed={stats['failed']} missing={stats['missing']}"
    )
    return {'run_id': run_id, 'status': final_status, **stats}


# =============================================================================
# DAG 4 DEFINITION
# =============================================================================

with DAG(
    dag_id='iceberg_catalog_migration',
    default_args=default_args,
    description=(
        'Iceberg-to-Iceberg migration via rewrite_table_path. '
        'Preserves full snapshot history, partition transforms, and schema fidelity. '
        'Requires data AND metadata pre-copied to destination S3.'
    ),
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=5,
    tags=['migration', 'iceberg', 'rewrite-table-path'],
    params={
        'excel_file_path': Param(
            default='s3a://config-bucket/iceberg_rewrite_migration.xlsx',
            type='string',
            description=(
                'S3 path to Excel config file. '
                'Required columns: database | table | source_s3_prefix | dest_s3_prefix'
            ),
        ),
    },
    render_template_as_native_obj=True,
) as dag_iceberg_rewrite:

    t_init = init_tracking_tables()
    t_run_id = create_migration_run(
        excel_file_path="{{ params.excel_file_path }}",
        dag_run_id="{{ run_id }}",
    )
    t_excel = parse_excel(
        excel_file_path="{{ params.excel_file_path }}",
        run_id=t_run_id,
    )

    # Dynamic task mapping (one set of tasks per database group)
    t_presence    = validate_data_presence.expand(db_config=t_excel)
    t_presence.operator.trigger_rule = 'all_done'

    t_pres_status = update_data_presence_in_tracking.expand(presence_result=t_presence)
    t_pres_status.operator.trigger_rule = 'all_done'

    t_discover    = discover_tables.expand(presence_result=t_pres_status)
    t_discover.operator.trigger_rule = 'all_done'

    t_record      = update_discovered_tables_in_tracking.expand(discovery=t_discover)
    t_record.operator.trigger_rule = 'all_done'

    t_tables      = rewrite_and_register_tables.expand(presence_result=t_record)
    t_tables.operator.trigger_rule = 'all_done'

    t_tbl_status = update_rewrite_and_register_in_tracking.expand(table_result=t_tables)
    t_tbl_status.operator.trigger_rule = 'all_done'

    t_validate = validate_dest_tables.expand(table_result=t_tbl_status)
    t_validate.operator.max_active_tis_per_dagrun = 3
    t_validate.operator.trigger_rule = 'all_done'

    t_val_status = update_validation_in_tracking.expand(validation_result=t_validate)
    t_val_status.operator.trigger_rule = 'all_done'

    t_report = generate_html_report(run_id=t_run_id)
    t_report.operator.trigger_rule = 'all_done'

    t_email = send_report_email(run_id=t_run_id, report_result=t_report)
    t_email.operator.trigger_rule = 'all_done'

    t_final = finalize_run(run_id=t_run_id)
    t_final.operator.trigger_rule = 'all_done'

    # Dependency chain
    t_init >> t_run_id >> t_excel >> t_presence >> t_pres_status
    t_pres_status >> t_discover >> t_record >> t_tables >> t_tbl_status
    t_tbl_status >> t_validate >> t_val_status >> t_report >> t_email >> t_final
