"""
DAG 2: Iceberg Migration

Converts existing Hive tables in S3 to Apache Iceberg format.
Runs independently after the MapR-to-S3 migration is complete.

Two migration strategies supported:
1. In-place migration: Convert existing Hive table to Iceberg (overwrites metadata)
2. Snapshot migration: Create separate Iceberg table alongside Hive table

Excel columns: database | table | inplace_migration | destination_iceberg_database
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import unquote

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from dotenv import load_dotenv
from migrator_utils.migrations.shared import (
    execute_with_iceberg_retry,
    get_config,
    is_permanent_error,
    permanent_fail,
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

# =============================================================================
# SKIP / FAILURE REASON CLASSIFICATION
# =============================================================================
# Every table that does not get migrated is recorded with an explicit reason
# code instead of a generic "unsupported format" message. The code is stored in
# iceberg_migration_table_status.error_message as "[CODE] detail", so the HTML
# report can render a short human label plus the full underlying detail.

REASON_LABELS = {
    'TABLE_NOT_FOUND': 'Table not found in metastore',
    'DATABASE_NOT_FOUND': 'Database not found in metastore',
    'NO_TABLES_MATCHED_PATTERN': 'No tables matched the Excel pattern',
    'SOURCE_IS_VIEW': 'Source object is a view, not a table',
    'ALREADY_ICEBERG': 'Table is already Iceberg',
    'SOURCE_NOT_V1_TABLE': 'Source is not a Hive V1 table',
    'TEXT_FORMAT_INPLACE_UNSUPPORTED': 'Text-format table (in-place unsupported)',
    'FORMAT_UNDETECTED_INPLACE': 'Storage format could not be determined',
    'UNSUPPORTED_SOURCE_FORMAT': 'Unsupported source file format',
    'UNSUPPORTED_DATA_TYPE': 'Unsupported column data type',
    'METADATA_READ_ERROR': 'Table metadata could not be read',
    'PERMISSION_DENIED': 'Permission denied on table or storage path',
    'DATA_PATH_MISSING': 'Data location missing in storage',
    'DESTINATION_EXISTS': 'Destination table already exists',
    'RESOURCE_ERROR': 'Spark ran out of resources',
    'CONCURRENT_COMMIT_CONFLICT': 'Concurrent Iceberg commit conflict',
    'TASK_DID_NOT_PROCESS': 'Table was never processed by the migration task',
    'MIGRATION_ERROR': 'Migration error',
    'VALIDATION_ERROR': 'Validation error',
}


def reason(code: str, detail: str) -> str:
    """Format a reason as '[CODE] detail' (whitespace collapsed) for error_message."""
    return f"[{code}] {' '.join(str(detail).split())}"


def sql_lit(text, limit: int = 2000) -> str:
    """Escape a value so it can be inlined into a single-quoted Spark SQL literal."""
    return str(text)[:limit].replace("\\", "\\\\").replace("'", "''")


def _err_has(err_lower: str, *needles: str) -> bool:
    return any(n in err_lower for n in needles)


def classify_metadata_error(err_text: str, db: str, obj: str):
    """Map a metastore/DESCRIBE failure to (code, status, detail).

    status is the tracking status to record: SKIPPED for config-level problems
    (the object simply is not there), FAILED for genuine errors that need
    attention (permissions, unreadable metadata).
    """
    e = (err_text or '').lower()
    if _err_has(e, 'nosuchdatabaseexception', 'schema_not_found', 'database not found',
                'unknown database', f"database '{db.lower()}' not found"):
        return ('DATABASE_NOT_FOUND', 'SKIPPED',
                f"Database '{db}' does not exist in the Hive metastore, so '{obj}' could not be read. "
                f"Check the 'database' column in the Excel config. Underlying error: {err_text}")
    if _err_has(e, 'table or view not found', 'nosuchtableexception', 'table_or_view_not_found',
                'table not found', 'cannot find table'):
        return ('TABLE_NOT_FOUND', 'SKIPPED',
                f"Table '{db}.{obj}' does not exist in the Hive metastore. Underlying error: {err_text}")
    if _err_has(e, 'access denied', 'accessdenied', 'permission denied', 'not authorized',
                'status code: 403', 'forbidden'):
        return ('PERMISSION_DENIED', 'FAILED',
                f"Access to '{db}.{obj}' or its storage location was denied. "
                f"Underlying error: {err_text}")
    return ('METADATA_READ_ERROR', 'FAILED',
            f"DESCRIBE FORMATTED failed for '{db}.{obj}', so its storage format, location and partition "
            f"columns are unknown. Underlying error: {err_text}")


def classify_migration_error(err_text: str, src_db: str, tbl: str, dest_table: str, inplace: bool):
    """Map an exception raised while migrating one table to (code, detail)."""
    e = (err_text or '').lower()
    target = f"{src_db}.{tbl} (in place)" if inplace else f"{src_db}.{tbl} -> {dest_table}"
    tail = f"Underlying error: {err_text}"

    if 'non-v1 table' in e:
        return ('SOURCE_NOT_V1_TABLE',
                f"{src_db}.{tbl} is not a Hive V1 table (it is a DataSource V2 table, typically already "
                f"Iceberg), and system.migrate only accepts V1 tables. {tail}")
    if _err_has(e, 'nosuchdatabaseexception', 'schema_not_found', 'database not found', 'unknown database'):
        return ('DATABASE_NOT_FOUND',
                f"A database referenced while migrating {target} does not exist. {tail}")
    if _err_has(e, 'table or view not found', 'nosuchtableexception', 'table_or_view_not_found',
                'cannot find table'):
        return ('TABLE_NOT_FOUND',
                f"Table {src_db}.{tbl} was not found while migrating {target} — it was dropped or renamed "
                f"after discovery, or the name in the Excel config is wrong. {tail}")
    if _err_has(e, 'access denied', 'accessdenied', 'permission denied', 'not authorized',
                'status code: 403', 'forbidden'):
        return ('PERMISSION_DENIED',
                f"Access denied while migrating {target} — the Spark role cannot read the source data or "
                f"write the destination location. {tail}")
    if _err_has(e, 'path does not exist', 'filenotfoundexception', 'no such file or directory',
                'nosuchkey', 'status code: 404'):
        return ('DATA_PATH_MISSING',
                f"The data location for {src_db}.{tbl} is missing in storage — the metastore points at a "
                f"path that no longer contains data. {tail}")
    if _err_has(e, 'already exists', 'tablealreadyexists'):
        return ('DESTINATION_EXISTS',
                f"Destination table {dest_table} already exists and could not be replaced. {tail}")
    if _err_has(e, 'lazysimpleserde', 'unsupported file format', 'is not a parquet file',
                'not an orc file', 'malformed orc', 'unsupported format'):
        return ('UNSUPPORTED_SOURCE_FORMAT',
                f"The source files of {src_db}.{tbl} are in a format Iceberg cannot register or read "
                f"(only Parquet/ORC/Avro data files can be migrated). {tail}")
    if _err_has(e, 'unsupported data type', 'unsupported type', 'cannot convert', 'cannot be cast',
                'cannot up cast'):
        return ('UNSUPPORTED_DATA_TYPE',
                f"A column type in {src_db}.{tbl} has no Iceberg equivalent or could not be converted. {tail}")
    if _err_has(e, 'outofmemory', 'java heap space', 'container killed', 'no space left',
                'gc overhead limit', 'executor lost'):
        return ('RESOURCE_ERROR',
                f"Spark ran out of memory/disk while migrating {target}. {tail}")
    if _err_has(e, 'commitfailedexception', 'concurrent', 'conflict', 'metadata location has changed'):
        return ('CONCURRENT_COMMIT_CONFLICT',
                f"An Iceberg commit for {target} conflicted with another writer. {tail}")
    return ('MIGRATION_ERROR', f"Migration of {target} failed. {tail}")


# =============================================================================
# DAG 2: ICEBERG MIGRATION TASKS
# =============================================================================
@task.pyspark(conn_id='spark_default')
def init_iceberg_tracking_tables(spark) -> dict:
    """Create Iceberg tracking tables for Iceberg migration if they don't exist."""
    config = get_config()
    tracking_db = config['tracking_database']
    tracking_loc = config['tracking_location']
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.iceberg_migration_runs (
            run_id STRING,
            dag_run_id STRING,
            excel_file_path STRING,
            migration_type STRING,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            status STRING,
            total_tables INT,
            successful_tables INT,
            failed_tables INT,
            config_json STRING
        )
        USING iceberg
        LOCATION '{tracking_loc}/iceberg_migration_runs'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.iceberg_migration_table_status (
            run_id STRING,
            dag_run_id STRING,
            source_database STRING,
            source_table STRING,
            migration_type STRING,
            destination_database STRING,
            destination_table STRING,
            table_location STRING,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            migration_duration_seconds DOUBLE,
            status STRING,
            source_hive_row_count BIGINT,
            destination_iceberg_row_count BIGINT,
            row_count_match BOOLEAN,
            source_hive_partition_count INT,
            source_hive_total_partition_count INT,
            dest_iceberg_partition_count INT,
            partition_count_match BOOLEAN,
            schema_match BOOLEAN,
            schema_differences STRING,
            validation_status STRING,
            validation_completed_at TIMESTAMP,
            validation_duration_seconds DOUBLE,
            error_message STRING,
            updated_at TIMESTAMP,
            empty_partition_names STRING
        )
        USING iceberg
        PARTITIONED BY (source_database)
        LOCATION '{tracking_loc}/iceberg_migration_table_status'
    """)
    for _col_name, _col_type in (
        ("empty_partition_names", "STRING"),
    ):
        try:
            spark.sql(
                f"ALTER TABLE {tracking_db}.iceberg_migration_table_status "
                f"ADD COLUMN {_col_name} {_col_type}"
            )
            logger.info(
                f"[init_iceberg_tracking_tables] Added column {_col_name} to iceberg_migration_table_status"
            )
        except Exception:
            pass
    return {'status': 'initialized', 'database': tracking_db}

@task.pyspark(conn_id='spark_default')
def create_iceberg_migration_run(excel_file_path: str, dag_run_id: str, spark) -> str:
    """Create migration run record."""
    import uuid
    from datetime import datetime

    config = get_config()
    tracking_db = config['tracking_database']

    run_id = f"iceberg_run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    spark.sql(f"""
        INSERT INTO {tracking_db}.iceberg_migration_runs
        VALUES (
            '{run_id}',
            '{dag_run_id}',
            '{excel_file_path}',
            NULL,
            current_timestamp(),
            NULL,
            'RUNNING',
            0, 0, 0,
            '{json.dumps(config).replace("'", "''")}'
        )
    """)

    return run_id


@task.pyspark(conn_id='spark_default')
def parse_iceberg_excel(excel_file_path: str, run_id: str, spark) -> list:
    """Read Excel config for Iceberg migration from S3, grouping rows by (database, inplace_migration, destination_iceberg_database)."""
    from io import BytesIO

    import pandas as ps

    df = None
    try:
        binary_df = spark.read.format("binaryFile").load(excel_file_path)
        row = binary_df.select("content").first()
        excel_bytes = bytes(row.content)
        df = ps.read_excel(BytesIO(excel_bytes), engine='openpyxl')
    except Exception as _e:
        permanent_fail("parse_iceberg_excel", _e)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    required_columns = ["database", "table", "inplace_migration", "destination_iceberg_database"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        permanent_fail(
            "parse_iceberg_excel",
            ValueError(
                f"Missing required Excel columns(s): {','.join(missing_columns)}"
            )
        )
    grouped = {}
    for _idx, row in df.iterrows():
        src_db = str(row.get('database', '') or '').strip()
        if not src_db:
            # Nothing can be looked up without a database, and there is no table
            # identity to record in tracking — surface it in the task log instead
            # of dropping the row silently.
            logger.warning(
                f"[ParseIcebergExcel] Excel row {int(_idx) + 2}: blank 'database' column — row ignored "
                f"(table='{row.get('table', '')}')"
            )
            continue

        inplace_val = row.get('inplace_migration', None)
        if inplace_val is None or (
            isinstance(inplace_val, float) and __import__('math').isnan(inplace_val)
        ):
            inplace_migration = False
        else:
            value = str(inplace_val).strip().lower()
            true_values = {"t", "true", "yes", "1"}
            false_values = {"f", "false", "no", "0", "", "nan"}
            if value in true_values:
                inplace_migration = True
            elif value in false_values:
                inplace_migration = False
            else:
                permanent_fail(
                    "parse_iceberg_excel",
                    ValueError(
                        f"Invalid value '{inplace_val}' for column 'inplace_migration'. "
                        "Allowed values are: T, F, TRUE, FALSE, YES, NO, 1, 0."
                    )
                )
        dest_ice_db_val = row.get('destination_iceberg_database', '')
        dest_ice_db = str(dest_ice_db_val).strip() if dest_ice_db_val is not None else ''
        if not dest_ice_db or dest_ice_db.lower() == 'nan':
            dest_ice_db = src_db if inplace_migration else f"{src_db}_iceberg"

        raw_cell_val = row.get('table', '')
        raw_cell = '*' if (raw_cell_val is None or (isinstance(raw_cell_val, float) and __import__('math').isnan(raw_cell_val)) or str(raw_cell_val).strip().lower() in ('', 'nan')) else str(raw_cell_val).strip() or '*'

        key = (src_db, inplace_migration, dest_ice_db)
        if key not in grouped:
            grouped[key] = {'tokens': []}

        for tok in raw_cell.split(','):
            tok = tok.strip()
            if tok:
                grouped[key]['tokens'].append(tok)

    configs = []
    for (src_db, inplace_migration, dest_ice_db), group in grouped.items():
        unique_tokens = list(dict.fromkeys(group['tokens']))
        if '*' in unique_tokens:
            unique_tokens = ['*']

        logger.info(
            f"[ParseIcebergExcel] {src_db} -> dest={dest_ice_db} | inplace={inplace_migration} | "
            f"tokens={unique_tokens[:10]}" + (" ..." if len(unique_tokens) > 10 else "")
        )

        configs.append({
            'source_database': src_db,
            'table_tokens': unique_tokens,
            'inplace_migration': inplace_migration,
            'destination_iceberg_database': dest_ice_db,
            'run_id': run_id,
        })

    logger.info(f"[ParseIcebergExcel] Total database configs emitted: {len(configs)}")
    if not configs:
        permanent_fail("parse_iceberg_excel", ValueError(
            "No valid rows found in Excel — check the 'database' column is populated."
        ))
    return configs


@task.pyspark(conn_id='spark_default')
@track_duration
def discover_hive_tables(db_config: dict, spark) -> dict:
    """Discover Hive tables matching the pattern in the source database."""
    src_db = db_config['source_database']
    raw_tokens = db_config.get('table_tokens') or []
    if not raw_tokens:
        pattern_str = db_config.get('table_pattern', '*')
        raw_tokens = [t.strip() for t in pattern_str.split(',') if t.strip()] or ['*']

    def skip_entry(name, code, status, detail, location=None):
        """Metadata entry for something we already know cannot be migrated.

        migrate_tables_to_iceberg records these verbatim, so the exact reason
        (not a generic 'unsupported format') reaches tracking and the report.
        """
        return {
            'table': name,
            'location': location,
            'source_format': 'UNKNOWN',
            'partition_columns': [],
            'skip_code': code,
            'skip_status': status,
            'skip_message': detail,
        }

    def parse_describe_formatted(desc_rows):
        """Extract location, source_format, partition columns and the raw storage
        descriptors from DESCRIBE FORMATTED output. source_format is one of
        TEXT / PARQUET / ORC / AVRO / ICEBERG / VIEW / UNKNOWN, derived from
        InputFormat / Provider / Serde Library."""
        location = None
        input_format = None
        provider = None
        serde = None
        table_type = None
        partition_columns: list[str] = []
        section = 'columns'
        for row in desc_rows:
            col = (row.col_name or '').strip()
            val = (row.data_type.strip() if row.data_type else '')
            col_lower = col.lower()
            if col_lower.startswith('# partition information'):
                section = 'partitions'
                continue
            if col_lower.startswith('# detailed table information') or col_lower.startswith('# storage information'):
                section = 'details'
                continue
            if not col:
                continue
            if section == 'partitions' and not col.startswith('#'):
                partition_columns.append(col)
            elif section == 'details':
                if col == 'Location':
                    location = val or None
                elif col == 'InputFormat':
                    input_format = val
                elif col == 'Provider':
                    provider = val
                elif col_lower == 'serde library':
                    serde = val
                elif col_lower in ('type', 'table type'):
                    table_type = val
        sig = ' '.join(s for s in (input_format, provider, serde) if s).lower()
        if 'iceberg' in sig:
            # Already migrated (or created) as Iceberg by an earlier run.
            source_format = 'ICEBERG'
        elif (table_type or '').upper() in ('VIEW', 'VIRTUAL_VIEW'):
            source_format = 'VIEW'
        elif 'parquet' in sig:
            source_format = 'PARQUET'
        elif 'orc' in sig:
            source_format = 'ORC'
        elif 'avro' in sig:
            source_format = 'AVRO'
        elif 'lazysimpleserde' in sig or 'text' in sig or 'csv' in sig:
            # LazySimpleSerDe is Hive's default for STORED AS TEXTFILE — these
            # cannot be migrated via Iceberg's migrate/snapshot procedures.
            source_format = 'TEXT'
        else:
            source_format = 'UNKNOWN'
        format_signature = (
            f"InputFormat={input_format or 'n/a'}, Provider={provider or 'n/a'}, "
            f"SerDe={serde or 'n/a'}, Type={table_type or 'n/a'}"
        )
        return {
            'location': location,
            'source_format': source_format,
            'partition_columns': partition_columns,
            'table_type': table_type,
            'format_signature': format_signature,
        }

    tables_metadata = []

    # ---------------------------------------------------------------------
    # Database-level check: without this the whole discovery task explodes and
    # every table of this Excel group ends up with no tracking row at all.
    # ---------------------------------------------------------------------
    if not _database_exists(spark, src_db):
        detail = (
            f"Database '{src_db}' from the Excel config does not exist in the Hive metastore, so none of "
            f"its tables could be discovered (requested: {', '.join(raw_tokens)}). Check the 'database' "
            f"column, or run the MapR-to-S3 migration for this database first."
        )
        logger.error(f"[IcebergDiscover] DATABASE_NOT_FOUND: {detail}")
        for tok in raw_tokens:
            tables_metadata.append(skip_entry(tok, 'DATABASE_NOT_FOUND', 'SKIPPED', detail))
        return {**db_config, 'discovered_tables': tables_metadata}

    # ---------------------------------------------------------------------
    # Token resolution: wildcards that match nothing and literal names that do
    # not exist are recorded with their own reason instead of vanishing (or
    # failing later with a generic error).
    # ---------------------------------------------------------------------
    matched_tables = []
    seen = set()
    for tok in raw_tokens:
        if '*' in tok:
            try:
                if tok == '*':
                    rows = spark.sql(f"SHOW TABLES IN {src_db}").collect()
                else:
                    rows = spark.sql(f"SHOW TABLES IN {src_db} LIKE '{tok}'").collect()
            except Exception as e:
                code, status, detail = classify_metadata_error(str(e), src_db, tok)
                logger.error(f"[IcebergDiscover] {code} while expanding pattern '{tok}': {detail[:300]}")
                tables_metadata.append(skip_entry(tok, code, status, detail))
                continue
            names = [r.tableName for r in rows]
            backups = [n for n in names if _is_iceberg_backup_table(n)]
            for b in backups:
                logger.info(f"[IcebergDiscover] Skipping Iceberg backup table: {src_db}.{b}")
            names = [n for n in names if not _is_iceberg_backup_table(n)]
            if not names:
                detail = (
                    f"No table in database '{src_db}' matched the pattern '{tok}' from the Excel config"
                    + (f" ({len(backups)} Iceberg backup table(s) matched and were excluded on purpose)"
                       if backups else " (the database is empty or the pattern is wrong)")
                    + "."
                )
                logger.warning(f"[IcebergDiscover] NO_TABLES_MATCHED_PATTERN: {detail}")
                tables_metadata.append(
                    skip_entry(tok, 'NO_TABLES_MATCHED_PATTERN', 'SKIPPED', detail)
                )
                continue
            for n in names:
                if n not in seen:
                    seen.add(n)
                    matched_tables.append(n)
        else:
            if tok in seen:
                continue
            seen.add(tok)
            try:
                exists = spark.sql(f"SHOW TABLES IN {src_db} LIKE '{tok}'").count() > 0
            except Exception as e:
                # Don't mislabel a permission/metastore error as "table not found".
                code, status, detail = classify_metadata_error(str(e), src_db, tok)
                logger.error(f"[IcebergDiscover] {code} while looking up '{src_db}.{tok}': {str(e)[:300]}")
                tables_metadata.append(skip_entry(tok, code, status, detail))
                continue
            if not exists:
                detail = (
                    f"Table '{src_db}.{tok}' listed in the Excel config does not exist in the Hive "
                    f"metastore (checked with SHOW TABLES IN {src_db} LIKE '{tok}'). Verify the spelling "
                    f"and that the table was created in S3 by the MapR-to-S3 migration."
                )
                logger.warning(f"[IcebergDiscover] TABLE_NOT_FOUND: {detail}")
                tables_metadata.append(skip_entry(tok, 'TABLE_NOT_FOUND', 'SKIPPED', detail))
                continue
            matched_tables.append(tok)

    logger.info(f"[IcebergDiscover] Database '{src_db}': {len(matched_tables)} table(s) matched tokens={raw_tokens}")

    for tbl in matched_tables:
        logger.info(f"[IcebergDiscover] Getting metadata for {src_db}.{tbl}")
        try:
            desc_df = spark.sql(f"DESCRIBE FORMATTED {src_db}.{tbl}")
            info = parse_describe_formatted(desc_df.collect())

            logger.info(
                f"[IcebergDiscover] {src_db}.{tbl} | format={info['source_format']} | "
                f"partitions={info['partition_columns'] or 'none'} | location={info['location']} | "
                f"{info['format_signature']}"
            )

            tables_metadata.append({
                'table': tbl,
                'location': info['location'],
                'source_format': info['source_format'],
                'partition_columns': info['partition_columns'],
                'table_type': info['table_type'],
                'format_signature': info['format_signature'],
            })
        except Exception as e:
            # A table can disappear between SHOW TABLES and DESCRIBE FORMATTED, or
            # be unreadable for permission reasons — say which one it was.
            code, status, detail = classify_metadata_error(str(e), src_db, tbl)
            logger.error(f"[IcebergDiscover] {code} for {src_db}.{tbl}: {str(e)[:300]}")
            tables_metadata.append(skip_entry(tbl, code, status, detail))

    logger.info(
        f"[IcebergDiscover] Completed discovery for '{src_db}': "
        f"{sum(1 for t in tables_metadata if not t.get('skip_code'))} table(s) ready for migration, "
        f"{sum(1 for t in tables_metadata if t.get('skip_code'))} not migratable"
    )

    return {
        **db_config,
        'discovered_tables': tables_metadata
    }

# =============================================================================
# ICEBERG IN-PLACE BACKUP HELPERS
# =============================================================================

ICEBERG_BACKUP_SUFFIXES = ('_backup_', '__BACKUP__')


def _is_iceberg_backup_table(table_name: str) -> bool:
    """True if table_name looks like a backup produced by system.migrate."""
    n = (table_name or '').strip()
    return n.lower().endswith('_backup_') or n.upper().endswith('__BACKUP__')


def _table_exists(spark, db: str, tbl: str) -> bool:
    """True if {db}.{tbl} exists in the metastore."""
    try:
        return spark.sql(f"SHOW TABLES IN {db} LIKE '{tbl}'").count() > 0
    except Exception:
        return False


def _database_exists(spark, db: str) -> bool:
    """True if database {db} exists in the metastore."""
    try:
        return spark.sql(f"SHOW DATABASES LIKE '{db}'").count() > 0
    except Exception:
        return False


def _is_iceberg_table(spark, db: str, tbl: str) -> bool:
    """True if {db}.{tbl} is ALREADY an Iceberg table (i.e. previously migrated)."""
    try:
        rows = spark.sql(f"DESCRIBE FORMATTED {db}.{tbl}").collect()
    except Exception:
        return False
    for r in rows:
        col = (r.col_name or '').strip().lower()
        val = (r.data_type or '').strip().lower()
        if col == 'provider' and 'iceberg' in val:
            return True
        # Some bundles surface the format only via table properties / storage handler
        if col.startswith('table properties') and 'iceberg' in val:
            return True
    return False


def _drop_stale_inplace_backup(spark, db: str, tbl: str):
    """ Incremental-run cleanup for IN-PLACE migration. """
    if not _is_iceberg_table(spark, db, tbl):
        return None
    for suffix in ICEBERG_BACKUP_SUFFIXES:
        backup = f"{tbl}{suffix}"
        if _table_exists(spark, db, backup):
            try:
                # metadata-only drop; NO PURGE (shared data files)
                spark.sql(f"DROP TABLE IF EXISTS {db}.{backup}")
                logger.warning(
                    f"[IcebergMigrate] Dropped stale backup from a previous run "
                    f"(metadata-only, no PURGE): {db}.{backup}"
                )
                return f"{db}.{backup}"
            except Exception as e:
                logger.warning(
                    f"[IcebergMigrate] Could not drop stale backup {db}.{backup}: {e!r}"
                )
    return None


@task.pyspark(conn_id='spark_default')
@track_duration
def migrate_tables_to_iceberg(discovery: dict, dag_run_id: str, spark, **context) -> dict:
    """Migrate discovered Hive tables to Iceberg format."""
    config = get_config()
    tracking_db = config['tracking_database']

    # Ensure Spark reads Hive tables recursively (handles nested subdirectories
    # under partition paths that are common after MapR-to-S3 copies).
    # system.snapshot does NOT use these settings for its own file listing, but
    # these are needed for the pre-migration hive_count query and any CTAS path.
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")
    spark.conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    spark.conf.set("mapred.input.dir.recursive", "true")

    src_db = discovery['source_database']
    dest_db = discovery['destination_iceberg_database']
    inplace = discovery['inplace_migration']
    run_id = discovery['run_id']
    drop_backup = config.get('iceberg_drop_backup', False)

    if inplace:
        logger.info(
            f"[IcebergMigrate] {src_db} | inplace=True | drop_backup={drop_backup} "
            f"({'backups dropped by Iceberg after commit' if drop_backup else 'backups retained; stale ones cleaned on next run'})"
        )

    # Nothing migratable (e.g. the source database itself is missing) → don't
    # create an empty destination database just to report skips into it.
    _migratable = [t for t in discovery.get('discovered_tables', []) if not t.get('skip_code')]
    if not inplace and _migratable:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_db}")

    results = []
    _permanent_failure = None

    from datetime import datetime as _dt

    def _record_not_migrated(tbl, code, detail, status='SKIPPED', location=None, started_at=None):
        """Record one table that was not migrated, with an explicit reason code.

        Writes '[CODE] detail' into error_message so tracking and the HTML report
        show why this specific table was skipped/failed rather than a generic
        'unsupported format' message.
        """
        mig_type = 'INPLACE' if inplace else 'SNAPSHOT'
        dest_table = f"{src_db}.{tbl}" if inplace else f"{dest_db}.{tbl}"
        duration = (_dt.utcnow() - (started_at or _dt.utcnow())).total_seconds()
        message = reason(code, detail)
        tbl_lit = sql_lit(tbl, 500)

        results.append({
            'source_table': f"{src_db}.{tbl}",
            'destination_table': dest_table,
            'migration_type': mig_type,
            'status': status,
            'reason_code': code,
            'error': message,
        })

        execute_with_iceberg_retry(spark, f"""
            DELETE FROM {tracking_db}.iceberg_migration_table_status
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND source_table = '{tbl_lit}'
        """, task_label=f"migrate:{code.lower()}:delete:{tbl}")

        execute_with_iceberg_retry(spark, f"""
            INSERT INTO {tracking_db}.iceberg_migration_table_status
            VALUES (
                '{run_id}', '{dag_run_id}', '{src_db}', '{tbl_lit}', '{mig_type}',
                '{dest_db}', '{tbl_lit}', '{sql_lit(location or "")}',
                current_timestamp(), current_timestamp(),
                {duration}, '{status}',
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                '{sql_lit(message)}',
                current_timestamp(),
                NULL
            )
        """, task_label=f"migrate:{code.lower()}:insert:{tbl}")

        _log = logger.error if status == 'FAILED' else logger.warning
        _log(
            f"[IcebergMigrate] {status} {src_db}.{tbl} | reason={code} "
            f"({REASON_LABELS.get(code, code)}) | {' '.join(str(detail).split())[:500]}"
        )

    for tbl_meta in discovery.get('discovered_tables', []):
        tbl = tbl_meta['table']
        location = tbl_meta.get('location')
        source_format = tbl_meta.get('source_format', 'UNKNOWN')
        format_signature = tbl_meta.get('format_signature') or 'storage descriptors not reported by DESCRIBE FORMATTED'
        partition_columns = tbl_meta.get('partition_columns', []) or []
        is_partitioned = bool(partition_columns)
        tbl_migrate_start = _dt.utcnow()

        # Reasons already established during discovery: table not found, database
        # not found, wildcard matched nothing, metadata unreadable. Recorded here
        # verbatim so each one keeps its own precise reason.
        if tbl_meta.get('skip_code'):
            _record_not_migrated(
                tbl,
                tbl_meta['skip_code'],
                tbl_meta.get('skip_message') or 'No further detail was captured during discovery.',
                status=tbl_meta.get('skip_status', 'SKIPPED'),
                location=location,
                started_at=tbl_migrate_start,
            )
            continue

        logger.info(
            f"[IcebergMigrate] Starting migration for {src_db}.{tbl} | "
            f"strategy={'INPLACE' if inplace else 'SNAPSHOT'} | dest={dest_db}.{tbl} | "
            f"source_format={source_format} | partition_cols={partition_columns or 'none'}"
        )

        # In-place limitations. Each is recorded as SKIPPED (not FAILED) so the run
        # continues, but with the reason that actually applies to this table instead
        # of one shared "unsupported format" message.
        if inplace:
            inplace_block = None
            if source_format == 'ICEBERG':
                inplace_block = (
                    'ALREADY_ICEBERG',
                    f"{src_db}.{tbl} is already an Iceberg table ({format_signature}), so there is nothing "
                    f"to convert in place — it was most likely migrated by an earlier run."
                )
            elif source_format == 'VIEW':
                inplace_block = (
                    'SOURCE_IS_VIEW',
                    f"{src_db}.{tbl} is a view ({format_signature}), not a physical Hive table. "
                    f"system.migrate can only convert physical tables — recreate the view on top of the "
                    f"migrated Iceberg tables instead."
                )
            elif source_format == 'TEXT':
                inplace_block = (
                    'TEXT_FORMAT_INPLACE_UNSUPPORTED',
                    f"{src_db}.{tbl} stores its data as text ({format_signature}). Iceberg's system.migrate "
                    f"procedure only registers Parquet/ORC/Avro data files (see "
                    f"TableMigrationUtil.listPartition), so this table cannot be converted in place. "
                    f"Set inplace_migration=F for this table to migrate it via CTAS instead."
                )
            elif source_format == 'UNKNOWN':
                inplace_block = (
                    'FORMAT_UNDETECTED_INPLACE',
                    f"The storage format of {src_db}.{tbl} could not be determined ({format_signature}), and "
                    f"in-place migration is only safe for Parquet/ORC/Avro V1 Hive tables. Check the table "
                    f"definition, or set inplace_migration=F to migrate it via CTAS."
                )
            if inplace_block:
                _record_not_migrated(
                    tbl, inplace_block[0], inplace_block[1],
                    location=location, started_at=tbl_migrate_start,
                )
                continue

        try:
            hive_count = spark.sql(f"SELECT COUNT(*) as c FROM {src_db}.{tbl}").collect()[0]['c']
            src_hive_partition_count = 0
            total_registered = 0
            partition_count_ok = False
            empty_partitions = []
            if is_partitioned:
                try:
                    # Fetch the actual distinct partition-column value combinations that
                    # have data files (previously this was wrapped in COUNT(*) and only the
                    # count was kept — that hid *which* partitions were empty). We now keep
                    # the real values so we can name any registered Hive partition that has
                    # no data files: those are exactly the partitions system.snapshot /
                    # system.migrate silently drop, since Iceberg only registers partitions
                    # that contain data files.
                    part_cols_expr = ', '.join(partition_columns)
                    nonempty_rows = spark.sql(f"""
                        SELECT DISTINCT {part_cols_expr} FROM {src_db}.{tbl}
                    """).collect()
                    src_hive_partition_count = len(nonempty_rows)
                    nonempty_keys = {
                        tuple(str(row[col]) for col in partition_columns)
                        for row in nonempty_rows
                    }

                    registered_specs = [
                        r[0] for r in spark.sql(f"SHOW PARTITIONS {src_db}.{tbl}").collect()
                    ]
                    total_registered = len(registered_specs)

                    for spec in registered_specs:
                        kv = {}
                        for segment in spec.split('/'):
                            if '=' in segment:
                                k, _, v = segment.partition('=')
                                kv[k] = unquote(v)
                        key = tuple(kv.get(col) for col in partition_columns)
                        if key not in nonempty_keys:
                            empty_partitions.append(spec)

                    partition_count_ok = True
                    logger.info(
                        f"[IcebergMigrate] {src_db}.{tbl} | "
                        f"strategy={'INPLACE' if inplace else 'SNAPSHOT'} | "
                        f"hive_registered_partitions={total_registered} | "
                        f"hive_nonempty_partitions={src_hive_partition_count}"
                    )
                    if empty_partitions:
                        logger.warning(
                            f"[IcebergMigrate] {src_db}.{tbl}: {len(empty_partitions)} of "
                            f"{total_registered} registered Hive partition(s) contain no data "
                            f"files and will be silently dropped by the Iceberg migration "
                            f"(system.snapshot/system.migrate only register partitions that "
                            f"have data files). Empty partition(s): {empty_partitions}"
                        )
                except Exception as _pcount_err:
                    logger.warning(
                        f"[IcebergMigrate] {src_db}.{tbl}: could not count Hive partitions "
                        f"(partition_columns={partition_columns}): {_pcount_err!r}. "
                        f"Skipping dest .partitions query to avoid spurious mismatch."
                    )

            if inplace:
                migration_type = "INPLACE"
                dest_table = f"{src_db}.{tbl}"
                _drop_stale_inplace_backup(spark, src_db, tbl)
                if drop_backup:
                    spark.sql(
                        f"CALL spark_catalog.system.migrate("
                        f"table => '{src_db}.{tbl}', drop_backup => true)"
                    )
                else:
                    spark.sql(
                        f"CALL spark_catalog.system.migrate(table => '{src_db}.{tbl}')"
                    )
            else:
                migration_type = "SNAPSHOT"
                dest_table = f"{dest_db}.{tbl}"
                try:
                    spark.sql(f"DESCRIBE {dest_table}")
                    logger.info(f"[IcebergMigrate] Destination {dest_table} already exists (prior attempt). Dropping before re-snapshot.")
                    spark.sql(f"DROP TABLE IF EXISTS {dest_table}")
                except Exception:
                    pass
                # Determine migration path for SNAPSHOT strategy:
                # - TEXT (LazySimpleSerDe): system.snapshot rejects it outright — must use CTAS.
                # - AVRO, empty table (hive_count == 0): system.snapshot has no data files to
                #   anchor the new Iceberg table to a distinct destination path, so Iceberg falls
                #   back to writing its metadata/ directory inside the SOURCE table's location.
                #   On a subsequent Airflow retry the AVRO SerDe reader then encounters metadata/
                #   as a subdirectory and raises IOException (not a directory-recursive reader).
                #   CTAS is safe because it writes the new Iceberg table to a fresh location in
                #   the destination warehouse and never touches the source directory.
                # - AVRO, non-empty table: system.snapshot works correctly — it anchors the new
                #   Iceberg table at the destination warehouse path (separate from the source
                #   external location) and metadata/ is written there, not in the source dir.
                # - PARQUET / ORC: system.snapshot is the preferred zero-copy path.
                # - ICEBERG / VIEW source: system.snapshot has no Hive data files to
                #   register, but a plain CTAS read works, so migrate them that way.
                if source_format == 'UNKNOWN':
                    logger.warning(
                        f"[IcebergMigrate] {src_db}.{tbl}: storage format could not be determined "
                        f"({format_signature}) — system.snapshot rejects text/unsupported SerDes, "
                        f"falling back to CTAS."
                    )
                elif source_format in ('VIEW', 'ICEBERG'):
                    logger.info(
                        f"[IcebergMigrate] {src_db}.{tbl} is {source_format} ({format_signature}) — "
                        f"system.snapshot cannot register it; creating {dest_table} via CTAS instead."
                    )
                use_ctas = (
                    source_format in ('TEXT', 'UNKNOWN', 'VIEW', 'ICEBERG')
                    or (source_format == 'AVRO' and hive_count == 0)
                )
                if use_ctas:
                    partition_clause = f"PARTITIONED BY ({', '.join(partition_columns)})" if partition_columns else ""
                    logger.info(
                        f"[IcebergMigrate] {source_format} source (rows={hive_count}) for {src_db}.{tbl}: "
                        f"using CTAS into {dest_table} {partition_clause or '(unpartitioned)'}"
                    )
                    spark.sql(f"""
                        CREATE TABLE {dest_table}
                        USING iceberg
                        {partition_clause}
                        AS SELECT * FROM {src_db}.{tbl}
                    """)
                else:
                    spark.sql(f"CALL spark_catalog.system.snapshot('{src_db}.{tbl}', '{dest_db}.{tbl}')")
                    # system.snapshot uses fs.listStatus() (non-recursive) to register files,
                    # so it misses data files that live in subdirectories under a partition
                    # path (common when the source was copied from MapR with the original
                    # sub-folder layout preserved). Detect the shortfall by comparing the
                    # snapshot row count with the pre-migration Hive row count (which Spark
                    # read recursively above). If rows are missing, drop the incomplete
                    # snapshot and fall back to CTAS, which uses Spark's full recursive reader.
                    spark.catalog.refreshTable(dest_table)
                    snapshot_count = spark.sql(f"SELECT COUNT(*) as c FROM {dest_table}").collect()[0]['c']
                    if snapshot_count < hive_count:
                        logger.warning(
                            f"[IcebergMigrate] {src_db}.{tbl}: system.snapshot registered only "
                            f"{snapshot_count}/{hive_count} rows — partition subdirectories not "
                            f"picked up by non-recursive file listing. Dropping snapshot and "
                            f"falling back to CTAS."
                        )
                        spark.sql(f"DROP TABLE IF EXISTS {dest_table}")
                        partition_clause = f"PARTITIONED BY ({', '.join(partition_columns)})" if partition_columns else ""
                        spark.sql(f"""
                            CREATE TABLE {dest_table}
                            USING iceberg
                            {partition_clause}
                            AS SELECT * FROM {src_db}.{tbl}
                        """)
                        use_ctas = True

            iceberg_count = spark.sql(f"SELECT COUNT(*) as c FROM {dest_table}").collect()[0]['c']
            # For unpartitioned tables, normalize to 0.
            # Iceberg's .partitions metadata table counts partitions that have data files.
            # SHOW PARTITIONS is a Hive-metastore command and returns 0 on Iceberg tables.
            # Guard: only query .partitions when src partition-counting succeeded; if it
            # failed (e.g. DESCRIBE FORMATTED returned false-positive partition_columns for
            # a non-partitioned table), both counts remain 0 → partition_match stays True.
            dest_iceberg_partition_count = 0
            if is_partitioned and partition_count_ok:
                try:
                    spark.catalog.refreshTable(dest_table)
                    try:
                        dest_iceberg_partition_count = spark.sql(
                            f"SELECT COUNT(*) as cnt FROM {dest_table}.partitions "
                            f"WHERE record_count > 0"
                        ).collect()[0]['cnt']
                    except Exception as _rc_err:
                        logger.warning(
                            f"[IcebergMigrate] {dest_table}: record_count filter unavailable "
                            f"({_rc_err!r}); falling back to raw .partitions count."
                        )
                        dest_iceberg_partition_count = spark.sql(
                            f"SELECT COUNT(*) as cnt FROM {dest_table}.partitions"
                        ).collect()[0]['cnt']
                except Exception:
                    pass

            counts_match = (hive_count == iceberg_count)
            if hive_count == 0:
                partition_match = True
                logger.info(
                    f"[IcebergMigrate] {src_db}.{tbl}: empty source (0 rows) — "
                    f"partition validation trivially satisfied "
                    f"(hive_registered={total_registered}, iceberg_data_partitions={dest_iceberg_partition_count})"
                )
            else:
                partition_match = (src_hive_partition_count == dest_iceberg_partition_count)

            logger.info(f"[IcebergMigrate] COMPLETED: {src_db}.{tbl} | hive_rows={hive_count} | iceberg_rows={iceberg_count} | rows_match={counts_match} | partitions_match={partition_match}")

            desc_df = spark.sql(f"DESCRIBE FORMATTED {dest_table}")
            new_location = None
            for row in desc_df.collect():
                if row.col_name and row.col_name.strip() == "Location":
                    new_location = row.data_type.strip() if row.data_type else None
                    break

            results.append({
                'source_table': f"{src_db}.{tbl}",
                'destination_table': dest_table,
                'migration_type': migration_type,
                'status': 'COMPLETED',
                'hive_count': hive_count,
                'iceberg_count': iceberg_count,
                'counts_match': counts_match,
                'hive_partition_count': src_hive_partition_count,
                'hive_total_partition_count': total_registered,
                'iceberg_partition_count': dest_iceberg_partition_count,
                'partition_match': partition_match,
                'empty_partitions': empty_partitions,
                'error': None
            })

            execute_with_iceberg_retry(spark, f"""
                DELETE FROM {tracking_db}.iceberg_migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
            """, task_label=f"migrate:completed:delete:{tbl}")

            empty_partition_names_sql = (
                f"'{sql_lit(', '.join(empty_partitions), 4000)}'" if empty_partitions else "NULL"
            )

            tbl_migrate_duration = (_dt.utcnow() - tbl_migrate_start).total_seconds()
            execute_with_iceberg_retry(spark, f"""
                INSERT INTO {tracking_db}.iceberg_migration_table_status
                VALUES (
                    '{run_id}',
                    '{dag_run_id}',
                    '{src_db}',
                    '{tbl}',
                    '{migration_type}',
                    '{dest_db}',
                    '{tbl}',
                    '{new_location or location or ""}',
                    current_timestamp(),
                    current_timestamp(),
                    {tbl_migrate_duration},
                    'COMPLETED',
                    {hive_count},
                    {iceberg_count},
                    {str(counts_match).lower()},
                    {src_hive_partition_count},
                    {total_registered},
                    {dest_iceberg_partition_count},
                    {str(partition_match).lower()},
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    current_timestamp(),
                    {empty_partition_names_sql}
                )
            """, task_label=f"migrate:completed:insert:{tbl}")

        except Exception as e:
            err_text = str(e)
            dest_table_name = f"{src_db}.{tbl}" if inplace else f"{dest_db}.{tbl}"
            code, detail = classify_migration_error(err_text, src_db, tbl, dest_table_name, inplace)

            # Spark's migrate() only accepts V1 source tables. If a table is already
            # DataSource V2 (for example already Iceberg), treat this as a known
            # skip condition for INPLACE mode instead of failing the whole task.
            if inplace and code == 'SOURCE_NOT_V1_TABLE':
                _record_not_migrated(
                    tbl, code,
                    f"{detail} Set inplace_migration=F to migrate it via snapshot/CTAS, or point the Excel "
                    f"row at the original Hive V1 table.",
                    status='SKIPPED', location=location, started_at=tbl_migrate_start,
                )
                continue

            _record_not_migrated(
                tbl, code, detail,
                status='FAILED', location=location, started_at=tbl_migrate_start,
            )
            if is_permanent_error("iceberg_migrate", e):
                _permanent_failure = (f"migrate_tables_to_iceberg:{src_db}.{tbl}", e)

    failed_migrations = [r for r in results if r['status'] == 'FAILED']
    if _permanent_failure is not None:
        _label, _exc = _permanent_failure
        context['ti'].xcom_push(key='return_value', value={
            'run_id': run_id,
            'source_database': src_db,
            'destination_database': dest_db,
            'migration_type': 'INPLACE' if inplace else 'SNAPSHOT',
            'results': results,
            '_has_failures': True,
            '_failure_summary': str(_exc)[:400],
        })
        permanent_fail(_label, _exc)
    has_failures = len(failed_migrations) > 0

    result_dict = {
        'run_id': run_id,
        'source_database': src_db,
        'destination_database': dest_db,
        'migration_type': 'INPLACE' if inplace else 'SNAPSHOT',
        'results': results,
        '_has_failures': has_failures,
        '_failure_summary': (
            f"Iceberg migration failed for {len(failed_migrations)}/{len(results)} table(s): "
            if has_failures else None
        )
    }

    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(f"Iceberg migration failed — {result_dict['_failure_summary']}. Per-table errors in tracking.")

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_migration_durations(migration_result: dict, spark) -> dict:
    """Update tracking table with migration durations from XCom."""

    if not isinstance(migration_result, dict) or 'run_id' not in migration_result:
        logger.warning(f"[update_migration_durations] Skipping invalid input: {type(migration_result)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']

    run_id = migration_result['run_id']
    src_db = migration_result['source_database']

    # Extract duration from XCom result
    migration_duration = migration_result.get('_task_duration', 0.0)

    # Update all records for this run
    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.iceberg_migration_table_status
        SET migration_duration_seconds = {migration_duration},
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND migration_duration_seconds = 0.0
    """,
    task_label="update_migration_durations:duration_bulk")

    for r in migration_result.get('results', []):
        if r.get('status') == 'FAILED' and r.get('error'):
            per_table_error = str(r['error'])[:2000].replace("'", "''")
            tbl_name = r['source_table'].split('.')[-1]
            src_db_name = r['source_table'].split('.')[0]
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.iceberg_migration_table_status
                SET status = 'FAILED',
                    error_message = '{per_table_error}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db_name}'
                  AND source_table = '{tbl_name}'
                  AND status IS NULL
            """,
            task_label=f"update_migration_durations:failure_patch:{tbl_name}")

    _never_processed = sql_lit(reason(
        'TASK_DID_NOT_PROCESS',
        'The migration task ended before it reached this table (earlier table failed the task, Spark '
        'session lost, or the worker was killed), so no per-table reason was captured. Re-run the DAG for '
        'this database to get a definite result for this table.'
    ))
    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.iceberg_migration_table_status
        SET status = 'FAILED',
            error_message = COALESCE(error_message, '{_never_processed}'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND status IS NULL
    """,
    task_label="update_migration_durations:catchall")

    return migration_result


@task.pyspark(conn_id='spark_default')
@track_duration
def validate_iceberg_tables(migration_result: dict, spark, **context) -> dict:
    """Validate Iceberg tables: row counts, partition counts, schema comparison between source Hive and destination Iceberg."""

    if not isinstance(migration_result, dict) or 'run_id' not in migration_result:
        logger.warning(f"[validate_iceberg_tables] Skipping invalid input: {type(migration_result)}")
        return {}

    src_db = migration_result['source_database']

    validation_results = []

    for r in migration_result.get('results', []):
        if r['status'] != 'COMPLETED':
            continue

        # Extract table name from fully qualified name
        src_tbl_full = r['source_table']
        tbl = src_tbl_full.split('.')[-1]
        dest_tbl = r['destination_table']

        logger.info(f"[IcebergValidation] Validating {src_db}.{tbl} vs {dest_tbl}")
        from datetime import datetime as _dt
        tbl_val_start = _dt.utcnow()

        try:
            # Schema comparison between source Hive and destination Iceberg
            src_hive_schema_df = spark.sql(f"DESCRIBE {src_db}.{tbl}")
            src_hive_schema = [
                {'name': row.col_name, 'type': row.data_type}
                for row in src_hive_schema_df.collect()
                if row.col_name and not row.col_name.startswith('#')
            ]

            dest_iceberg_schema_df = spark.sql(f"DESCRIBE {dest_tbl}")
            dest_iceberg_schema = [
                {'name': row.col_name, 'type': row.data_type}
                for row in dest_iceberg_schema_df.collect()
                if row.col_name and not row.col_name.startswith('#')
            ]

            # Compare schemas
            schema_match = True
            schema_diffs = []

            def _normalize_type_for_iceberg(hive_type: str) -> str:
                """Normalize a Hive column type to its Iceberg-equivalent representation.

                Iceberg's type system does not have TINYINT (8-bit) or SMALLINT (16-bit).
                Both are promoted to Iceberg IntegerType, which DESCRIBE shows as 'int'.
                This normalization prevents false-positive schema-mismatch failures when
                validating Hive tables that use these narrow integer types as partition cols.
                """
                t = hive_type.strip().lower()
                if t in ('tinyint', 'smallint'):
                    return 'int'
                return t

            src_cols = {c['name']: c['type'] for c in src_hive_schema}
            dest_cols = {c['name']: c['type'] for c in dest_iceberg_schema}

            for col_name, col_type in src_cols.items():
                if col_name not in dest_cols:
                    schema_match = False
                    schema_diffs.append(f"Missing column in Iceberg: {col_name}")
                elif _normalize_type_for_iceberg(dest_cols[col_name]) != _normalize_type_for_iceberg(col_type):
                    schema_match = False
                    schema_diffs.append(f"Type mismatch for {col_name}: Hive {col_type} vs Iceberg {dest_cols[col_name]}")
                elif dest_cols[col_name] != col_type:
                    # Types are Iceberg-compatible (e.g. smallint promoted to int) — log but do not flag as failure
                    logger.info(
                        f"[IcebergValidation] Type promoted (expected): {col_name} Hive={col_type} -> Iceberg={dest_cols[col_name]} "
                        f"in {src_db}.{tbl}"
                    )

            for col_name in dest_cols:
                if col_name not in src_cols:
                    schema_match = False
                    schema_diffs.append(f"Extra column in Iceberg: {col_name}")

            row_ok = r.get('counts_match', False)
            part_ok = r.get('partition_match', False)
            logger.info(f"[IcebergValidation] DONE: {src_db}.{tbl} | rows={'✓' if row_ok else '✗'} partitions={'✓' if part_ok else '✗'} schema={'✓' if schema_match else '✗'}")
            if schema_diffs:
                logger.warning(f"[IcebergValidation] Schema diffs for {src_db}.{tbl}: {'; '.join(schema_diffs[:5])}")

            validation_results.append({
                'source_table': tbl,
                'destination_table': dest_tbl,
                'status': 'COMPLETED',
                'source_hive_row_count': r.get('hive_count', 0),
                'dest_iceberg_row_count': r.get('iceberg_count', 0),
                'row_count_match': r.get('counts_match', False),
                'source_hive_partition_count': r.get('hive_partition_count', 0),
                'dest_iceberg_partition_count': r.get('iceberg_partition_count', 0),
                'partition_count_match': r.get('partition_match', False),
                'schema_match': schema_match,
                'schema_differences': '; '.join(schema_diffs) if schema_diffs else '',
                'per_table_validation_duration': (_dt.utcnow() - tbl_val_start).total_seconds(),
                'error': None
            })

        except Exception as e:
            code, detail = classify_migration_error(str(e), src_db, tbl, dest_tbl, False)
            if code == 'MIGRATION_ERROR':
                code = 'VALIDATION_ERROR'
                detail = (
                    f"Comparing {src_db}.{tbl} with {dest_tbl} failed. Underlying error: {str(e)}"
                )
            else:
                detail = f"During validation — {detail}"
            error_msg = reason(code, detail)
            validation_results.append({
                'source_table': tbl,
                'destination_table': dest_tbl,
                'status': 'FAILED',
                'reason_code': code,
                'per_table_validation_duration': (_dt.utcnow() - tbl_val_start).total_seconds(),
                'error': error_msg[:2000]
            })
            logger.error(f"[IcebergValidation] FAILED {src_db}.{tbl} | reason={code} | {error_msg[:500]}")

    failed_validations = [v for v in validation_results if v['status'] == 'FAILED']
    mismatched = [
        v for v in validation_results
        if v.get('status') == 'COMPLETED' and (
            not v.get('row_count_match', True) or
            not v.get('partition_count_match', True)
        )
    ]
    total_failures = len(failed_validations) + len(mismatched)
    has_failures = total_failures > 0

    result_dict = {
        **migration_result,
        'validation_results': validation_results,
        '_has_failures': has_failures,
        '_failure_summary': (
            f"Iceberg validation failed for {total_failures}/{len(validation_results)} table(s)"
            if has_failures else None
        )
    }

    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(f"Iceberg validation failed — {result_dict['_failure_summary']}. Per-table errors in tracking.")

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_iceberg_validation_status(validation_result: dict, spark) -> dict:
    """Update Iceberg tracking with validation results."""

    if not isinstance(validation_result, dict) or 'run_id' not in validation_result:
        logger.warning(f"[update_iceberg_validation_status] Skipping invalid input: {type(validation_result)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']

    run_id = validation_result['run_id']
    src_db = validation_result['source_database']

    # Extract duration from XCom result
    task_level_validation_duration = validation_result.get('_task_duration', 0.0)

    for v in validation_result.get('validation_results', []):
        if v['status'] != 'COMPLETED':
            continue

        schema_diffs = (v.get('schema_differences', '') or '').replace("'", "''")[:2000]

        overall_status = 'VALIDATED' if (
            v.get('row_count_match', False) and
            v.get('partition_count_match', True) and
            v.get('schema_match', False)
        ) else 'VALIDATION_FAILED'

        is_validated = (
            v.get('row_count_match', False) and
            v.get('partition_count_match', True) and
            v.get('schema_match', False)
        )

        if is_validated:
            error_message_sql = "NULL"
        else:
            # Spell out which check failed instead of leaving VALIDATION_FAILED
            # rows with an empty (or pre-existing, unrelated) error_message.
            problems = []
            if not v.get('row_count_match', False):
                _src_rows = v.get('source_hive_row_count', 0) or 0
                _dst_rows = v.get('dest_iceberg_row_count', 0) or 0
                problems.append(
                    f"row count mismatch — source Hive={_src_rows:,} vs destination Iceberg={_dst_rows:,} "
                    f"(difference {_src_rows - _dst_rows:,})"
                )
            if not v.get('partition_count_match', True):
                problems.append(
                    f"partition count mismatch — Hive non-empty partitions="
                    f"{v.get('source_hive_partition_count', 0) or 0} vs Iceberg data partitions="
                    f"{v.get('dest_iceberg_partition_count', 0) or 0}"
                )
            if not v.get('schema_match', False):
                problems.append(
                    f"schema mismatch — {v.get('schema_differences') or 'no column-level detail captured'}"
                )
            if v.get('error'):
                problems.append(f"validation error — {v['error']}")
            detail = '; '.join(problems) or (
                'validation did not pass but no specific difference was captured'
            )
            error_message_sql = f"'{sql_lit(reason('VALIDATION_ERROR', detail))}'"

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.iceberg_migration_table_status
            SET validation_status = '{v['status']}',
                validation_completed_at = current_timestamp(),
                validation_duration_seconds = {v.get('per_table_validation_duration', task_level_validation_duration)},
                source_hive_row_count = {v.get('source_hive_row_count', 0)},
                destination_iceberg_row_count = {v.get('dest_iceberg_row_count', 0)},
                row_count_match = {str(v.get('row_count_match', False)).lower()},
                source_hive_partition_count = {v.get('source_hive_partition_count', 0)},
                dest_iceberg_partition_count = {v.get('dest_iceberg_partition_count', 0)},
                partition_count_match = {str(v.get('partition_count_match', False)).lower()},
                schema_match = {str(v.get('schema_match', False)).lower()},
                schema_differences = '{schema_diffs}',
                status = CASE
                    WHEN status = 'FAILED' THEN status  -- preserve original migration failure
                    ELSE '{overall_status}'
                END,
                error_message = CASE
                    WHEN status = 'FAILED' THEN error_message  -- preserve original error
                    ELSE {error_message_sql}
                END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND source_table = '{v['source_table']}'
        """,
        task_label=f"update_iceberg_validation_status:{v['source_table']}")

    for v in validation_result.get('validation_results', []):
        if v.get('status') == 'FAILED' and v.get('error'):
            per_table_error = str(v['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.iceberg_migration_table_status
                SET validation_status = 'FAILED',
                    status = 'VALIDATION_FAILED',
                    error_message = '{per_table_error}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{v['source_table']}'
                  AND validation_status IS NULL
            """,
            task_label=f"update_iceberg_validation_status:failure_patch:{v['source_table']}")

    _val_never_processed = sql_lit(reason(
        'TASK_DID_NOT_PROCESS',
        'The table migrated successfully but the validation task ended before comparing it with the source '
        '(earlier table failed the task, or the Spark session was lost), so row/partition/schema checks were '
        'never run for it.'
    ))
    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.iceberg_migration_table_status
        SET validation_status = 'SKIPPED',
            status = CASE WHEN status = 'FAILED' THEN 'FAILED' ELSE 'VALIDATION_FAILED' END,
            error_message = COALESCE(error_message, '{_val_never_processed}'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND status = 'COMPLETED'
          AND validation_status IS NULL
    """,
    task_label="update_iceberg_validation_status:catchall")

    return validation_result


@task.pyspark(conn_id='spark_default')
def generate_iceberg_html_report(run_id: str, spark, **context) -> str:
    """Generate comprehensive HTML Iceberg migration report."""
    import re as _re
    from datetime import datetime
    from html import escape as _esc

    config = get_config()
    tracking_db = config['tracking_database']
    report_location = config['report_output_location']

    # Get migration status
    migration_status = spark.sql(f"""
        SELECT * FROM {tracking_db}.iceberg_migration_table_status
        WHERE run_id = '{run_id}'
        ORDER BY source_database, source_table
    """).collect()

    # Calculate summary stats
    total_tables = len(migration_status)
    successful_tables = sum(1 for t in migration_status if t.status in ['VALIDATED', 'COMPLETED'])
    failed_tables = sum(1 for t in migration_status if 'FAILED' in (t.status or ''))
    skipped_tables = sum(1 for t in migration_status if (t.status or '') == 'SKIPPED')
    total_rows = sum(t.source_hive_row_count or 0 for t in migration_status)
    count_mismatches = sum(1 for t in migration_status if not t.row_count_match and t.row_count_match is not None)

    # Validation summary query
    iceberg_validation_summary = spark.sql(f"""
        SELECT
            COUNT(*) as total_tables_validated,
            SUM(CASE WHEN row_count_match = true AND partition_count_match = true AND schema_match = true THEN 1 ELSE 0 END) as tables_passed_validation,
            SUM(CASE WHEN row_count_match = false OR partition_count_match = false OR schema_match = false THEN 1 ELSE 0 END) as tables_failed_validation,
            SUM(CASE WHEN row_count_match = false THEN 1 ELSE 0 END) as total_row_count_mismatches,
            SUM(CASE WHEN partition_count_match = false THEN 1 ELSE 0 END) as total_partition_count_mismatches,
            SUM(CASE WHEN schema_match = false THEN 1 ELSE 0 END) as total_schema_mismatches
        FROM {tracking_db}.iceberg_migration_table_status
        WHERE run_id = '{run_id}'
          AND validation_status = 'COMPLETED'
    """).collect()

    def _row_value(row, field_name, default=None):
        """Safely read a Spark Row field even when the schema varies."""

        if hasattr(row, 'asDict'):
            return row.asDict(recursive=True).get(field_name, default)
        return default

    _REASON_RE = _re.compile(r'^\[([A-Z0-9_]+)\]\s*(.*)$', _re.S)

    def _split_reason(row):
        """Split error_message ('[CODE] detail') into (code, label, detail).

        Rows written before the reason codes existed (or by another writer) fall
        back to code='' and the raw message as the detail, so nothing is lost.
        """
        raw = str(_row_value(row, 'error_message', '') or '').strip()
        if not raw:
            return '', '', ''
        m = _REASON_RE.match(raw)
        if m:
            code = m.group(1)
            return code, REASON_LABELS.get(code, code.replace('_', ' ').capitalize()), m.group(2).strip()
        return '', 'Reason not classified', raw

    # Every table that did not end up as a validated/completed Iceberg table,
    # with the reason that actually applies to it.
    not_migrated = [
        t for t in migration_status
        if (t.status or '') == 'SKIPPED' or 'FAILED' in (t.status or '')
    ]
    reason_breakdown = {}
    for t in not_migrated:
        code, label, _detail = _split_reason(t)
        key = (code, label or 'No reason recorded')
        reason_breakdown[key] = reason_breakdown.get(key, 0) + 1
    reason_breakdown = sorted(reason_breakdown.items(), key=lambda kv: (-kv[1], kv[0][1]))

    # Generate HTML
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Iceberg Migration Report - {run_id}</title>
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
        /* Table/destination identifiers have no spaces to break on, so without an
           explicit cap one long name stretches its column across the table. */
        td.name {{
            max-width: 200px;
            overflow-wrap: anywhere;
            word-break: break-word;
            font-size: 12px;
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
        .status-validated {{
            background-color: #c3e6cb;
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
        .timestamp {{
            color: #95a5a6;
            font-size: 12px;
        }}
        .section-divider {{
            margin: 40px 0;
            border-top: 2px dashed #ecf0f1;
        }}
        .reason-code {{
            display: inline-block;
            padding: 3px 10px;
            border-radius: 10px;
            background-color: #eaf2f8;
            color: #1f4e79;
            font-size: 12px;
            font-weight: bold;
        }}
        .reason-key {{
            margin-top: 2px;
            color: #95a5a6;
            font-size: 11px;
            font-family: Consolas, 'Courier New', monospace;
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
    </style>
</head>
<body>
    <div class="container">
        <h1>Iceberg Migration Report</h1>

        <div class="timestamp">
            Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
            Run ID: <strong>{run_id}</strong>
        </div>

        <h2>Migration Summary</h2>
        <div class="summary-grid">
            <div class="summary-card">
                <h3>TOTAL TABLES</h3>
                <p class="value">{total_tables}</p>
            </div>
            <div class="summary-card success">
                <h3>SUCCESSFUL</h3>
                <p class="value">{successful_tables}</p>
            </div>
            <div class="summary-card warning">
                <h3>FAILED</h3>
                <p class="value">{failed_tables}</p>
            </div>
            <div class="summary-card warning">
                <h3>SKIPPED</h3>
                <p class="value">{skipped_tables}</p>
            </div>
            <div class="summary-card info">
                <h3>TOTAL ROWS</h3>
                <p class="value">{total_rows:,}</p>
            </div>
            <div class="summary-card warning">
                <h3>COUNT MISMATCHES</h3>
                <p class="value">{count_mismatches}</p>
            </div>
        </div>

        <div class="section-divider"></div>

        <h2>Validation Summary</h2>

"""

    if iceberg_validation_summary and iceberg_validation_summary[0]['total_tables_validated']:
        ivs = iceberg_validation_summary[0]
        html += f"""
        <div class="summary-grid">
            <div class="summary-card info">
                <h3>TABLES VALIDATED</h3>
                <p class="value">{ivs.total_tables_validated}</p>
            </div>
            <div class="summary-card success">
                <h3>PASSED VALIDATION</h3>
                <p class="value">{ivs.tables_passed_validation}</p>
            </div>
            <div class="summary-card warning">
                <h3>FAILED VALIDATION</h3>
                <p class="value">{ivs.tables_failed_validation}</p>
            </div>
            <div class="summary-card warning">
                <h3>ROW COUNT MISMATCHES</h3>
                <p class="value">{ivs.total_row_count_mismatches}</p>
            </div>
            <div class="summary-card warning">
                <h3>PARTITION MISMATCHES</h3>
                <p class="value">{ivs.total_partition_count_mismatches}</p>
            </div>
            <div class="summary-card warning">
                <h3>SCHEMA MISMATCHES</h3>
                <p class="value">{ivs.total_schema_mismatches}</p>
            </div>
        </div>
"""
    else:
        html += """
        <p style="color: #95a5a6; font-style: italic;">No validation summary available for this run.</p>
"""

    html += """
        <div class="section-divider"></div>

        <h2>Table Migration Details</h2>
        <table>
            <thead>
                <tr>
                    <th>Source Database</th>
                    <th>Table</th>
                    <th>Migration Type</th>
                    <th>Destination</th>
                    <th>Status</th>
                    <th>Migration Duration</th>
                    <th>Validation Duration</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in migration_status:
        if t.status == 'VALIDATED':
            status_class = 'status-validated'
        elif t.status == 'COMPLETED':
            status_class = 'status-completed'
        elif t.status == 'SKIPPED':
            status_class = 'status-skipped'
        else:
            status_class = 'status-failed'

        migration_dur = f"{t.migration_duration_seconds:.1f}s" if t.migration_duration_seconds else "N/A"
        validation_dur = f"{t.validation_duration_seconds:.1f}s" if t.validation_duration_seconds else "N/A"
        _code, _label, _detail = _split_reason(t)
        badge_title_attr = f' title="{_esc(_detail)}"' if _detail else ''

        # The reason sits under the badge so a SKIPPED/FAILED row explains itself
        # in place — this is the only per-table reason left in the report.
        status_reason_html = ''
        if _label:
            status_reason_html += f'\n                        <div class="status-reason">{_esc(_label)}</div>'
        if _code:
            status_reason_html += f'\n                        <div class="reason-key">{_esc(_code)}</div>'

        html += f"""
                <tr>
                    <td>{_esc(str(t.source_database or ''))}</td>
                    <td class="name"><strong>{_esc(str(t.source_table or ''))}</strong></td>
                    <td>{_esc(str(t.migration_type or ''))}</td>
                    <td class="name">{_esc(str(t.destination_table or ''))}</td>
                    <td class="status-cell">
                        <span class="status-badge {status_class}"{badge_title_attr}>{t.status}</span>{status_reason_html}
                    </td>
                    <td class="duration">{migration_dur}</td>
                    <td class="duration">{validation_dur}</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

        <div class="section-divider"></div>

        <h2>Tables that Skipped or Failed</h2>
"""

    if not not_migrated:
        html += """
        <p style="color: #27ae60; font-weight: bold;">
            Every table in this run was migrated — nothing was skipped or failed.
        </p>
"""
    else:
        html += """
        <h3 style="color: #34495e;">Reason breakdown</h3>
        <table>
            <thead>
                <tr>
                    <th>Reason</th>
                    <th>Reason code</th>
                    <th>Tables affected</th>
                </tr>
            </thead>
            <tbody>
"""
        for (_code, _label), _count in reason_breakdown:
            html += f"""
                <tr>
                    <td><span class="reason-code">{_esc(_label)}</span></td>
                    <td class="reason-key">{_esc(_code or 'unclassified')}</td>
                    <td class="metric">{_count}</td>
                </tr>
"""
        html += """
            </tbody>
        </table>
"""

    html += """
        <div class="section-divider"></div>

        <h2>Validation Results (Hive vs Iceberg)</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>Source Hive Rows</th>
                    <th>Dest Iceberg Rows</th>
                    <th>Row Count Match</th>
                    <th>Source Partitions (non-empty)</th>
                    <th>Hive Total Partitions</th>
                    <th>Empty Partitions (dropped)</th>
                    <th>Dest Partitions</th>
                    <th>Partition Match</th>
                    <th>Schema Match</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in migration_status:
        if t.validation_status == 'COMPLETED':
            row_match_class = 'validation-pass' if t.row_count_match else 'validation-fail'
            row_match_icon = '✓ PASS' if t.row_count_match else '✗ FAIL'
            part_match_class = 'validation-pass' if t.partition_count_match else 'validation-fail'
            part_match_icon = '✓ PASS' if t.partition_count_match else '✗ FAIL'
            schema_match_class = 'validation-pass' if t.schema_match else 'validation-fail'
            schema_match_icon = '✓ PASS' if t.schema_match else '✗ FAIL'
        else:
            row_match_class = part_match_class = schema_match_class = 'duration'
            row_match_icon = part_match_icon = schema_match_icon = 'N/A'

        _empty_names_raw = (_row_value(t, 'empty_partition_names', '') or '').strip()
        _empty_names = [p for p in _empty_names_raw.split(', ') if p] if _empty_names_raw else []
        _empty_cell = (
            f'<span class="validation-fail" title="{_esc(_empty_names_raw)}">'
            f'{len(_empty_names)}: {_esc(", ".join(_empty_names[:3]))}'
            f'{" ..." if len(_empty_names) > 3 else ""}</span>'
        ) if _empty_names else '0'

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td class="name"><strong>{t.source_table}</strong></td>
                    <td class="metric">{(t.source_hive_row_count or 0):,}</td>
                    <td class="metric">{(t.destination_iceberg_row_count or 0):,}</td>
                    <td class="{row_match_class}">{row_match_icon}</td>
                    <td class="metric">{t.source_hive_partition_count or 0}</td>
                    <td class="metric">{_row_value(t, 'source_hive_total_partition_count', 0) or 0}</td>
                    <td class="metric">{_empty_cell}</td>
                    <td class="metric">{t.dest_iceberg_partition_count or 0}</td>
                    <td class="{part_match_class}">{part_match_icon}</td>
                    <td class="{schema_match_class}">{schema_match_icon}</td>
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
                    <th>Database</th>
                    <th>Table</th>
                    <th>Migration Duration</th>
                    <th>Validation Duration</th>
                    <th>Total Duration</th>
                    <th>Rows Migrated</th>
                    <th>Rows/Second</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in migration_status:
        migration_dur = t.migration_duration_seconds or 0
        validation_dur = t.validation_duration_seconds or 0
        total_dur = migration_dur + validation_dur

        rows_per_sec = (t.source_hive_row_count or 0) / (total_dur or 1)

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td class="name"><strong>{t.source_table}</strong></td>
                    <td class="metric">{migration_dur:.1f}s</td>
                    <td class="metric">{validation_dur:.1f}s</td>
                    <td class="metric">{total_dur:.1f}s ({total_dur/60:.1f}m)</td>
                    <td class="metric">{(t.source_hive_row_count or 0):,}</td>
                    <td class="metric">{rows_per_sec:,.0f}</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

        <div style="margin-top: 50px; padding-top: 20px; border-top: 1px solid #ecf0f1; color: #95a5a6; font-size: 12px;">
            <p>This report was automatically generated by the Iceberg Migration DAG.</p>
        </div>
    </div>
</body>
</html>
"""

    # Write HTML to S3
    portal_run_id = context.get('params', {}).get('run_id') or run_id
    report_filename = f"{portal_run_id}_iceberg_report.html"
    report_path = f"{report_location}/{report_filename}"

    # Use Spark to write HTML
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(report_path),
        hadoop_conf
    )

    output_path = spark._jvm.org.apache.hadoop.fs.Path(report_path)
    output_stream = fs.create(output_path, True)
    output_stream.write(html.encode('utf-8'))
    output_stream.close()

    return {'report_path': report_path}


@task.pyspark(conn_id='spark_default')
def finalize_iceberg_run(run_id: str, spark) -> dict:
    """Finalize Iceberg migration run - aggregate statistics."""
    config = get_config()
    tracking_db = config['tracking_database']

    stats = {'total': 0, 'successful': 0, 'failed': 0, 'skipped': 0, 'count_mismatches': 0}
    final_status = 'FAILED'
    overall_migration_type = 'UNKNOWN'

    try:
        stats_result = spark.sql(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status IN ('VALIDATED', 'COMPLETED') THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END) as skipped,
                SUM(CASE WHEN row_count_match = false THEN 1 ELSE 0 END) as count_mismatches
            FROM {tracking_db}.iceberg_migration_table_status
            WHERE run_id = '{run_id}'
        """).collect()

        if not stats_result or stats_result[0]['total'] == 0:
            logger.warning(f"[finalize_iceberg_run] No table records found for run_id '{run_id}'.")
            final_status = 'FAILED'
        else:
            stats = {
                'total': stats_result[0]['total'] or 0,
                'successful': stats_result[0]['successful'] or 0,
                'failed': stats_result[0]['failed'] or 0,
                'skipped': stats_result[0]['skipped'] or 0,
                'count_mismatches': stats_result[0]['count_mismatches'] or 0,
            }
            final_status = 'COMPLETED' if stats['failed'] == 0 else 'COMPLETED_WITH_FAILURES'

    except Exception as e:
        logger.error(f"[finalize_iceberg_run] Failed to query iceberg_migration_table_status: {str(e)}")
        final_status = 'FAILED'

    try:
        migration_type_result = spark.sql(f"""
            SELECT migration_type, COUNT(*) as cnt
            FROM {tracking_db}.iceberg_migration_table_status
            WHERE run_id = '{run_id}'
            GROUP BY migration_type
            ORDER BY cnt DESC
            LIMIT 1
        """).collect()
        overall_migration_type = migration_type_result[0]['migration_type'] if migration_type_result else 'UNKNOWN'
    except Exception as e:
        logger.warning(f"[finalize_iceberg_run] Could not determine migration_type: {str(e)}")

    try:
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.iceberg_migration_runs
            SET status = '{final_status}',
                completed_at = current_timestamp(),
                migration_type = '{overall_migration_type}',
                total_tables = {stats['total']},
                successful_tables = {stats['successful']},
                failed_tables = {stats['failed']}
            WHERE run_id = '{run_id}'
        """, task_label="finalize_iceberg_run:update_iceberg_migration_runs")

        logger.info(f"[finalize_iceberg_run] Run '{run_id}' finalized with status '{final_status}'. "
                    f"total={stats['total']}, successful={stats['successful']}, failed={stats['failed']}")

    except Exception as e:
        logger.error(f"[finalize_iceberg_run] Failed to update iceberg_migration_runs: {str(e)}")
        raise

    return {
        'run_id': run_id,
        'status': final_status,
        'total': stats['total'],
        'successful': stats['successful'],
        'failed': stats['failed'],
        'skipped': stats['skipped'],
        'count_mismatches': stats['count_mismatches']
    }



@task.pyspark(conn_id='spark_default')
def send_iceberg_report_email(report_result: dict, run_id: str, spark) -> dict:
    """Send HTML Iceberg migration report via email using SMTP."""
    import os
    import tempfile

    from airflow.utils.email import send_email

    config = get_config()
    smtp_conn_id = config.get('smtp_conn_id', 'smtp_default')
    recipients_str = config.get('email_recipients', '')

    if not recipients_str:
        logger.warning("[Email] No recipients configured. Skipping email.")
        return {'sent': False, 'reason': 'no_recipients'}

    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
    report_path = report_result.get('report_path', '')

    try:
        logger.info(f"[Email] Reading Iceberg HTML report from S3: {report_path}")
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
            prefix=f'{run_id}_iceberg_report_',
            delete=False
        )
        tmp.write(html_content)
        tmp.close()

        send_email(
            to=recipients,
            subject=f"Iceberg Migration Report - {run_id}",
            html_content=f"<p>Please find the Iceberg migration report for run <strong>{run_id}</strong> attached.</p>",
            files=[tmp.name],
            conn_id=smtp_conn_id,
        )
        os.unlink(tmp.name)
        logger.info(f"[Email] Iceberg report sent to: {recipients}")
        return {'sent': True, 'recipients': recipients, 'report_path': report_path}
    except Exception as e:
        logger.error(f"[Email] Failed to send Iceberg report: {str(e)}")
        if is_permanent_error("send_email", e):
            permanent_fail("send_iceberg_report_email", e)
        raise Exception(f"Failed to send Iceberg report email: {str(e)}") from e

# =============================================================================
# DAG 2 DEFINITION: ICEBERG MIGRATION
# =============================================================================

with DAG(
    dag_id='iceberg_migration',
    default_args=default_args,
    description='Migrate existing Hive tables in S3 to Iceberg format',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=5,
    tags=['migration', 'iceberg', 'hive'],
    params={
        'excel_file_path': Param(
            default='s3a://config-bucket/iceberg_migration.xlsx',
            type='string',
            description='S3 path to Excel config file for Iceberg migration'
        ),
        'iceberg_drop_backup': Param(
            default=False,
            type='boolean',
            description='Drop the <table>_backup_ table after successful in-place migration',
        ),
    },
    render_template_as_native_obj=True,
) as dag_iceberg:

    # Initialize
    t_ice_init = init_iceberg_tracking_tables()
    t_ice_run_id = create_iceberg_migration_run(
        excel_file_path="{{ params.excel_file_path }}",
        dag_run_id="{{ run_id }}"
    )
    t_ice_excel = parse_iceberg_excel(
        excel_file_path="{{ params.excel_file_path }}",
        run_id=t_ice_run_id
    )

    # Per-database processing
    t_ice_discover = discover_hive_tables.expand(db_config=t_ice_excel)
    t_ice_migrate = migrate_tables_to_iceberg.partial(dag_run_id="{{ run_id }}").expand(discovery=t_ice_discover)
    t_ice_migrate.operator.trigger_rule = 'all_done'

    # Duration update
    t_ice_durations = update_migration_durations.expand(migration_result=t_ice_migrate)
    t_ice_durations.operator.trigger_rule = 'all_done'

    # Validation
    t_ice_validate = validate_iceberg_tables.expand(migration_result=t_ice_durations)
    t_ice_validate.operator.max_active_tis_per_dagrun = 3
    t_ice_validate.operator.trigger_rule = 'all_done'
    t_ice_val_status = update_iceberg_validation_status.expand(validation_result=t_ice_validate)
    t_ice_val_status.operator.trigger_rule = 'all_done'

    # Report generation
    t_ice_report = generate_iceberg_html_report(run_id=t_ice_run_id, params="{{ params }}")
    t_ice_report.operator.trigger_rule = 'all_done'

    # Email report
    t_ice_email = send_iceberg_report_email(run_id=t_ice_run_id, report_result=t_ice_report)
    t_ice_email.operator.trigger_rule = 'all_done'

    # Finalize
    t_ice_final = finalize_iceberg_run(run_id=t_ice_run_id)
    t_ice_final.operator.trigger_rule = 'all_done'

    # Dependencies
    t_ice_init >> t_ice_run_id >> t_ice_excel >> t_ice_discover >> t_ice_migrate >> t_ice_durations
    t_ice_durations >> t_ice_validate >> t_ice_val_status >> t_ice_report >> t_ice_email >> t_ice_final
