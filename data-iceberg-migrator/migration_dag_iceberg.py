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
    """Read portal username from Airflow Variable at DAG parse/trigger time."""
    try:
        from airflow.models import Variable
        owner = Variable.get('migration_dag_owner', default_var='')
        if owner:
            return owner
    except Exception:
        pass
    return 'data-migration'

default_args = {
    "owner": _resolve_dag_owner(),
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

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
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (source_database)
        LOCATION '{tracking_loc}/iceberg_migration_table_status'
    """)
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

    # Validate that the table column exists.
    # Without this check, a missing/misspelled 'table' column defaults to ''
    # and is later interpreted as '*', unintentionally selecting all tables.
    if 'table' not in df.columns:
        permanent_fail(
            "parse_iceberg_excel",
            ValueError(
                "Missing required Excel column: 'table'."
            )
        )

    grouped = {}
    for _, row in df.iterrows():
        src_db = str(row.get('database', '') or '').strip()
        if not src_db:
            continue

        inplace_val = row.get('inplace_migration', None)
        if inplace_val is None or (isinstance(inplace_val, float) and __import__('math').isnan(inplace_val)) or str(inplace_val).strip().lower() in ('', 'nan', 'f', 'false', 'no', '0'):
            inplace_migration = False
        else:
            inplace_migration = str(inplace_val).strip().upper() in ('T', 'TRUE', 'YES', '1')

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

    def resolve_tokens(spark, db, tokens):
        resolved = []
        seen = set()
        for tok in tokens:
            if tok == '*':
                rows = spark.sql(f"SHOW TABLES IN {db}").collect()
                for r in rows:
                    t = r.tableName
                    if _is_iceberg_backup_table(t):
                        logger.info(f"[IcebergDiscover] Skipping Iceberg backup table: {db}.{t}")
                        continue
                    if t not in seen:
                        seen.add(t)
                        resolved.append(t)
            elif '*' in tok:
                rows = spark.sql(f"SHOW TABLES IN {db} LIKE '{tok}'").collect()
                for r in rows:
                    t = r.tableName
                    if _is_iceberg_backup_table(t):
                        logger.info(f"[IcebergDiscover] Skipping Iceberg backup table: {db}.{t}")
                        continue
                    if t not in seen:
                        seen.add(t)
                        resolved.append(t)
            else:
                if tok not in seen:
                    seen.add(tok)
                    resolved.append(tok)
        return resolved

    def parse_describe_formatted(desc_rows):
        """Extract location, source_format, and partition column names from
        DESCRIBE FORMATTED output. source_format is one of
        TEXT / PARQUET / ORC / AVRO / UNKNOWN, derived from InputFormat /
        Provider / Serde Library."""
        location = None
        input_format = None
        provider = None
        serde = None
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
                elif col.lower() == 'serde library':
                    serde = val
        sig = ' '.join(s for s in (input_format, provider, serde) if s).lower()
        if 'parquet' in sig:
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
        return location, source_format, partition_columns

    matched_tables = resolve_tokens(spark, src_db, raw_tokens)

    logger.info(f"[IcebergDiscover] Database '{src_db}': {len(matched_tables)} table(s) matched tokens={raw_tokens}")

    tables_metadata = []
    for tbl in matched_tables:
        logger.info(f"[IcebergDiscover] Getting metadata for {src_db}.{tbl}")
        try:
            desc_df = spark.sql(f"DESCRIBE FORMATTED {src_db}.{tbl}")
            location, source_format, partition_columns = parse_describe_formatted(desc_df.collect())

            logger.info(
                f"[IcebergDiscover] {src_db}.{tbl} | format={source_format} | "
                f"partitions={partition_columns or 'none'} | location={location}"
            )

            tables_metadata.append({
                'table': tbl,
                'location': location,
                'source_format': source_format,
                'partition_columns': partition_columns,
            })
        except Exception as e:
            logger.error(f"[IcebergDiscover] Failed to get metadata for {src_db}.{tbl}: {str(e)[:300]}")
            tables_metadata.append({
                'table': tbl,
                'location': None,
                'source_format': 'UNKNOWN',
                'partition_columns': [],
                'discovery_error': str(e),
            })

    logger.info(f"[IcebergDiscover] Completed discovery for '{src_db}': {len(tables_metadata)} table(s) ready for migration")

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

    if not inplace:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_db}")

    results = []
    _permanent_failure = None

    for tbl_meta in discovery.get('discovered_tables', []):
        tbl = tbl_meta['table']
        location = tbl_meta.get('location')
        source_format = tbl_meta.get('source_format', 'UNKNOWN')
        partition_columns = tbl_meta.get('partition_columns', []) or []
        is_partitioned = bool(partition_columns)

        logger.info(
            f"[IcebergMigrate] Starting migration for {src_db}.{tbl} | "
            f"strategy={'INPLACE' if inplace else 'SNAPSHOT'} | dest={dest_db}.{tbl} | "
            f"source_format={source_format} | partition_cols={partition_columns or 'none'}"
        )
        from datetime import datetime as _dt
        tbl_migrate_start = _dt.utcnow()

        # Text-format INPLACE is unsupported: Iceberg's migrate procedure rejects
        # LazySimpleSerDe (see TableMigrationUtil.listPartition), and we can't
        # safely overwrite the source Hive table in place. Record as SKIPPED
        # (not FAILED) so the run continues and the report distinguishes a
        # known limitation from an actual failure.
        if source_format in ('TEXT', 'UNKNOWN') and inplace:
            if source_format == 'UNKNOWN':
                error_msg = (
                    "Inplace Iceberg migration skipped: format detection returned UNKNOWN "
                    "(likely a text-format table whose SerDe was not parsed from DESCRIBE FORMATTED)"
                ).replace("'", "''")
                logger.warning(f"[IcebergMigrate] SKIPPED unknown-format inplace: {src_db}.{tbl}")
            else:
                error_msg = (
                    "Inplace Iceberg migration is not supported for text-format Hive tables "
                ).replace("'", "''")
            tbl_fail_duration = (_dt.utcnow() - tbl_migrate_start).total_seconds()
            results.append({
                'source_table': f"{src_db}.{tbl}",
                'destination_table': f"{src_db}.{tbl}",
                'migration_type': 'INPLACE',
                'status': 'SKIPPED',
                'error': error_msg,
            })
            execute_with_iceberg_retry(spark, f"""
                DELETE FROM {tracking_db}.iceberg_migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
            """, task_label=f"migrate:text_inplace_skip:delete:{tbl}")
            execute_with_iceberg_retry(spark, f"""
                INSERT INTO {tracking_db}.iceberg_migration_table_status
                VALUES (
                    '{run_id}', '{dag_run_id}', '{src_db}', '{tbl}', 'INPLACE',
                    '{dest_db}', '{tbl}', '{location or ""}',
                    current_timestamp(), current_timestamp(),
                    {tbl_fail_duration}, 'SKIPPED',
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    '{error_msg}',
                    current_timestamp()
                )
            """, task_label=f"migrate:text_inplace_skip:insert:{tbl}")
            if source_format != 'UNKNOWN':  # UNKNOWN already warned above
                logger.warning(f"[IcebergMigrate] SKIPPED text-format inplace: {src_db}.{tbl}")
            continue

        try:
            hive_count = spark.sql(f"SELECT COUNT(*) as c FROM {src_db}.{tbl}").collect()[0]['c']
            src_hive_partition_count = 0
            total_registered = 0
            partition_count_ok = False
            if is_partitioned:
                try:
                    # Count only partitions that have actual data files — these are the only
                    # ones system.snapshot can register in Iceberg (empty Hive partition dirs
                    # have no data files and are not visible to Iceberg after snapshot).
                    # Using DISTINCT on partition columns avoids a separate S3 scan; it piggy-
                    # backs on the data already read for hive_count.
                    part_cols_expr = ', '.join(partition_columns)
                    src_hive_partition_count = spark.sql(f"""
                        SELECT COUNT(*) as cnt FROM (
                            SELECT DISTINCT {part_cols_expr} FROM {src_db}.{tbl}
                        )
                    """).collect()[0]['cnt']
                    total_registered = spark.sql(f"SHOW PARTITIONS {src_db}.{tbl}").count()
                    partition_count_ok = True
                    logger.info(
                        f"[IcebergMigrate] {src_db}.{tbl} | "
                        f"strategy={'INPLACE' if inplace else 'SNAPSHOT'} | "
                        f"hive_registered_partitions={total_registered} | "
                        f"hive_nonempty_partitions={src_hive_partition_count}"
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
                if source_format == 'UNKNOWN':
                    logger.warning(
                        f"[IcebergMigrate] {src_db}.{tbl}: source_format=UNKNOWN — "
                        f"system.snapshot rejects text-format SerDes; falling back to CTAS. "
                        f"Check DESCRIBE FORMATTED output (InputFormat/Serde Library may not have been parsed)."
                    )
                use_ctas = source_format in ('TEXT', 'UNKNOWN') or (source_format == 'AVRO' and hive_count == 0)
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
                'iceberg_partition_count': dest_iceberg_partition_count,
                'partition_match': partition_match,
                'error': None
            })

            execute_with_iceberg_retry(spark, f"""
                DELETE FROM {tracking_db}.iceberg_migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
            """, task_label=f"migrate:completed:delete:{tbl}")

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
                    current_timestamp()
                )
            """, task_label=f"migrate:completed:insert:{tbl}")

        except Exception as e:
            err_text = str(e)
            # Spark's migrate() only accepts V1 source tables. If a table is already
            # DataSource V2 (for example already Iceberg), treat this as a known
            # skip condition for INPLACE mode instead of failing the whole task.
            if inplace and 'non-v1 table' in err_text.lower():
                skip_msg = (
                    f"Inplace Iceberg migration skipped for {src_db}.{tbl}: source table is non-v1 "
                    f"(likely already Iceberg/DataSource V2). Use snapshot mode or point to the original Hive V1 table."
                )
                skip_msg_sql = skip_msg[:2000].replace("'", "''")
                tbl_skip_duration = (_dt.utcnow() - tbl_migrate_start).total_seconds()

                results.append({
                    'source_table': f"{src_db}.{tbl}",
                    'destination_table': f"{src_db}.{tbl}",
                    'migration_type': 'INPLACE',
                    'status': 'SKIPPED',
                    'error': skip_msg,
                })

                execute_with_iceberg_retry(spark, f"""
                    DELETE FROM {tracking_db}.iceberg_migration_table_status
                    WHERE run_id = '{run_id}'
                      AND source_database = '{src_db}'
                      AND source_table = '{tbl}'
                """, task_label=f"migrate:non_v1_inplace_skip:delete:{tbl}")

                execute_with_iceberg_retry(spark, f"""
                    INSERT INTO {tracking_db}.iceberg_migration_table_status
                    VALUES (
                        '{run_id}',
                        '{dag_run_id}',
                        '{src_db}',
                        '{tbl}',
                        'INPLACE',
                        '{dest_db}',
                        '{tbl}',
                        '{location or ""}',
                        current_timestamp(),
                        current_timestamp(),
                        {tbl_skip_duration},
                        'SKIPPED',
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        NULL,
                        '{skip_msg_sql}',
                        current_timestamp()
                    )
                """, task_label=f"migrate:non_v1_inplace_skip:insert:{tbl}")

                logger.warning(f"[IcebergMigrate] SKIPPED non-v1 inplace source: {src_db}.{tbl} | reason={err_text[:300]}")
                continue

            error_msg = f"Migration to Iceberg failed for {dest_db}.{tbl}: {err_text[:2000]}".replace("'", "''")
            tbl_fail_duration = (_dt.utcnow() - tbl_migrate_start).total_seconds()

            results.append({
                'source_table': f"{src_db}.{tbl}",
                'destination_table': f"{dest_db}.{tbl}" if not inplace else f"{src_db}.{tbl}",
                'migration_type': "INPLACE" if inplace else "SNAPSHOT",
                'status': 'FAILED',
                'error': str(e)
            })

            execute_with_iceberg_retry(spark, f"""
                DELETE FROM {tracking_db}.iceberg_migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
            """, task_label=f"migrate:failed:delete:{tbl}")

            execute_with_iceberg_retry(spark, f"""
                INSERT INTO {tracking_db}.iceberg_migration_table_status
                VALUES (
                    '{run_id}',
                    '{dag_run_id}',
                    '{src_db}',
                    '{tbl}',
                    '{"INPLACE" if inplace else "SNAPSHOT"}',
                    '{dest_db}',
                    '{tbl}',
                    '{location or ""}',
                    current_timestamp(),
                    current_timestamp(),
                    {tbl_fail_duration},
                    'FAILED',
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    '{error_msg}',
                    current_timestamp()
                )
            """, task_label=f"migrate:failed:insert:{tbl}")
            logger.error(f"ERROR: {error_msg}")
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

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.iceberg_migration_table_status
        SET status = 'FAILED',
            error_message = COALESCE(error_message, 'Iceberg migration task did not process this table'),
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
    dest_db = migration_result['destination_database']

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
            error_msg = f"Validation failed for {dest_db}.{tbl}: {str(e)[:2000]}"
            validation_results.append({
                'source_table': tbl,
                'destination_table': dest_tbl,
                'status': 'FAILED',
                'per_table_validation_duration': (_dt.utcnow() - tbl_val_start).total_seconds(),
                'error': str(e)[:2000]
            })
            logger.error(f"ERROR: {error_msg}")

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

        if not is_validated and v.get('error'):
            mismatch_msg = str(v['error']).replace("'", "''")[:2000]
            error_message_sql = f"'{mismatch_msg}'"
        elif is_validated:
            error_message_sql = "NULL"
        else:
            error_message_sql = "error_message"

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

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.iceberg_migration_table_status
        SET validation_status = 'SKIPPED',
            status = CASE WHEN status = 'FAILED' THEN 'FAILED' ELSE 'VALIDATION_FAILED' END,
            error_message = COALESCE(error_message, 'Iceberg validation task did not process this table'),
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
    from datetime import datetime

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
        row_error = _row_value(t, 'error', '') or _row_value(t, 'error_message', '') or ''
        reason_tooltip = str(row_error).replace('"', '&quot;') if t.status in ('SKIPPED', 'FAILED') else ''
        badge_title_attr = f' title="{reason_tooltip}"' if reason_tooltip else ''

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td>{t.migration_type}</td>
                    <td>{t.destination_table}</td>
                    <td><span class="status-badge {status_class}"{badge_title_attr}>{t.status}</span></td>
                    <td class="duration">{migration_dur}</td>
                    <td class="duration">{validation_dur}</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

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

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{(t.source_hive_row_count or 0):,}</td>
                    <td class="metric">{(t.destination_iceberg_row_count or 0):,}</td>
                    <td class="{row_match_class}">{row_match_icon}</td>
                    <td class="metric">{t.source_hive_partition_count or 0}</td>
                    <td class="metric">{_row_value(t, 'source_hive_total_partition_count', 0) or 0}</td>
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
                    <td><strong>{t.source_table}</strong></td>
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
