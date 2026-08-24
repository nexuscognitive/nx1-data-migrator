"""
DAG 4: Parquet HMS Registration

Registers plain parquet files on S3 as external Hive Metastore tables.
Used after a prior migration moved parquet files to S3 without Hive metadata:
this DAG infers the schema from the parquet files, creates an EXTERNAL table
on top of them, and registers Hive-style (key=value) partitions via
MSCK REPAIR TABLE.

Excel columns: database | table | s3_location
"""

import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from dotenv import load_dotenv
from migrator_utils.migrations.shared import (
    cell_str,
    execute_with_iceberg_retry,
    get_config,
    normalize_s3,
    track_duration,
)

_dag_stem = Path(__file__).stem
logger = logging.getLogger(__name__)

# Excel cells are interpolated into Spark SQL unescaped — restrict identifiers
# so a stray quote/space/semicolon fails fast at parse time instead of breaking
# SQL (or silently creating a junk database) mid-run.
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")

_dag_dir = Path(__file__).resolve().parent
_config_dir = str(_dag_dir / "migrator_utils" / "migration_configs")
if os.path.isdir(_config_dir):
    load_dotenv(os.path.join(_config_dir, "env.shared"))
    load_dotenv(os.path.join(_config_dir, f"env.{_dag_stem}"), override=True)
else:
    logger.warning(
        f"Config directory {_config_dir} not found — env files not loaded, using Airflow Variables / defaults"
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
    "owner": _resolve_dag_owner(),
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# =============================================================================
# DAG 4: PARQUET HMS REGISTRATION TASKS
# =============================================================================
@task.pyspark(conn_id="spark_default")
def init_hms_tracking_tables(spark) -> dict:
    """Create Iceberg tracking tables for HMS registration if they don't exist."""
    config = get_config()
    tracking_db = config["tracking_database"]
    tracking_loc = config["tracking_location"]
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.hms_registration_runs (
            run_id STRING,
            dag_run_id STRING,
            excel_file_path STRING,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            status STRING,
            total_tables INT,
            successful_tables INT,
            failed_tables INT,
            skipped_tables INT
        )
        USING iceberg
        LOCATION '{tracking_loc}/hms_registration_runs'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.hms_registration_status (
            run_id STRING,
            dag_run_id STRING,
            database_name STRING,
            table_name STRING,
            s3_location STRING,
            partition_columns STRING,
            registration_duration_seconds DOUBLE,
            status STRING,
            hms_row_count BIGINT,
            parquet_row_count BIGINT,
            row_count_match BOOLEAN,
            hms_partition_count INT,
            s3_partition_count INT,
            partition_count_match BOOLEAN,
            validation_status STRING,
            validation_duration_seconds DOUBLE,
            error_message STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (database_name)
        LOCATION '{tracking_loc}/hms_registration_status'
    """)
    return {"status": "initialized", "database": tracking_db}


@task.pyspark(conn_id="spark_default")
def create_hms_registration_run(excel_file_path: str, dag_run_id: str, spark) -> str:
    """Create HMS registration run record."""
    import uuid
    from datetime import datetime

    config = get_config()
    tracking_db = config["tracking_database"]

    run_id = f"hms_reg_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    spark.sql(f"""
        INSERT INTO {tracking_db}.hms_registration_runs
        VALUES (
            '{run_id}',
            '{dag_run_id}',
            '{excel_file_path}',
            current_timestamp(),
            NULL,
            'RUNNING',
            0, 0, 0, 0
        )
    """)

    return run_id


@task.pyspark(conn_id="spark_default")
def parse_parquet_hms_excel(excel_file_path: str, run_id: str, spark) -> list:
    """Read Excel config from S3: one config dict per table to register."""
    from io import BytesIO

    import pandas as ps

    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    excel_bytes = bytes(row.content)
    df = ps.read_excel(BytesIO(excel_bytes), engine="openpyxl")

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    configs = []
    seen = set()
    for idx, row in df.iterrows():
        database = cell_str(row.get("database"))
        table = cell_str(row.get("table"))
        s3_location = cell_str(row.get("s3_location"))

        if not database or not table or not s3_location:
            logger.warning(
                f"[ParseParquetHmsExcel] Row {idx}: missing required cell (database/table/s3_location) — skipping"
            )
            continue

        if not _VALID_IDENTIFIER.match(database) or not _VALID_IDENTIFIER.match(table):
            logger.warning(
                f"[ParseParquetHmsExcel] Row {idx}: invalid database/table identifier "
                f"{database!r}.{table!r} (only letters, digits and _ allowed) — skipping"
            )
            continue

        if "'" in s3_location:
            logger.warning(f"[ParseParquetHmsExcel] Row {idx}: s3_location contains a quote — skipping")
            continue

        key = (database.lower(), table.lower())
        if key in seen:
            logger.warning(f"[ParseParquetHmsExcel] Duplicate row for {database}.{table} — first occurrence wins")
            continue
        seen.add(key)

        configs.append(
            {
                "database": database,
                "table": table,
                "s3_location": normalize_s3(s3_location).rstrip("/"),
                "run_id": run_id,
            }
        )

    logger.info(f"[ParseParquetHmsExcel] {len(configs)} table(s) to register")
    return configs


def _discover_partition_columns(spark, location: str) -> list:
    """Return ordered Hive partition column names from key=value directory layout.

    Follows the first key=value directory at each level (Hive layouts are
    uniform per level, so one branch is enough to learn the column order).
    Hidden dirs (.spark-staging, _temporary) and plain subdirectories are
    skipped; the walk stops at the first level with no key=value directory —
    the table is treated as unpartitioned from that level down.
    """
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(location), hadoop_conf)

    columns = []
    current = location
    while True:
        statuses = fs.listStatus(jvm.org.apache.hadoop.fs.Path(current))
        next_dir = None
        for st in statuses:
            if not st.isDirectory():
                continue
            name = st.getPath().getName()
            if name.startswith((".", "_")):
                continue
            if "=" in name:
                columns.append(name.split("=", 1)[0])
                next_dir = st.getPath().toString()
                break
        if next_dir is None:
            break
        current = next_dir
    return columns


def _count_leaf_partition_dirs(spark, location: str, depth: int) -> int:
    """Count leaf key=value directories `depth` levels below location.

    Mirrors what MSCK REPAIR registers: every key=value directory chain is a
    partition, whether or not it contains data files — so empty partition
    directories are counted on both sides of the validation comparison.
    """
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(location), hadoop_conf)

    current = [location]
    for _ in range(depth):
        next_level = []
        for path in current:
            for st in fs.listStatus(jvm.org.apache.hadoop.fs.Path(path)):
                if not st.isDirectory():
                    continue
                name = st.getPath().getName()
                if name.startswith((".", "_")) or "=" not in name:
                    continue
                next_level.append(st.getPath().toString())
        current = next_level
    return len(current)


def _table_location(spark, database: str, table: str) -> str | None:
    """Return the storage location of an existing HMS table, or None if not found."""
    for row in spark.sql(f"DESCRIBE FORMATTED {database}.{table}").collect():
        if (row["col_name"] or "").strip() == "Location":
            return (row["data_type"] or "").strip()
    return None


@task.pyspark(conn_id="spark_default")
@track_duration
def register_parquet_tables(table_config: dict, dag_run_id: str, spark, **context) -> dict:
    """Register one parquet S3 location as an external HMS table; repair partitions via MSCK."""
    from datetime import datetime as _dt

    config = get_config()
    tracking_db = config["tracking_database"]

    run_id = table_config["run_id"]
    database = table_config["database"]
    table = table_config["table"]
    s3_location = table_config["s3_location"]

    started = _dt.utcnow()
    result = {
        "run_id": run_id,
        "database": database,
        "table": table,
        "s3_location": s3_location,
        "partition_columns": [],
        "status": "FAILED",
        "error": None,
    }

    def _record(status: str, partition_cols: list, error: str | None):
        duration = (_dt.utcnow() - started).total_seconds()
        error_sql = "'" + error[:2000].replace("'", "''") + "'" if error else "NULL"
        execute_with_iceberg_retry(
            spark,
            f"""
            DELETE FROM {tracking_db}.hms_registration_status
            WHERE run_id = '{run_id}'
              AND database_name = '{database}'
              AND table_name = '{table}'
        """,
            task_label=f"hms_register:delete:{database}.{table}",
        )
        execute_with_iceberg_retry(
            spark,
            f"""
            INSERT INTO {tracking_db}.hms_registration_status
            VALUES (
                '{run_id}', '{dag_run_id}', '{database}', '{table}', '{s3_location}',
                '{",".join(partition_cols)}',
                {duration}, '{status}',
                NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL,
                {error_sql},
                current_timestamp(), current_timestamp()
            )
        """,
            task_label=f"hms_register:insert:{database}.{table}",
        )

    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

        if spark.catalog.tableExists(f"{database}.{table}"):
            existing_location = _table_location(spark, database, table)
            if existing_location and normalize_s3(existing_location).rstrip("/") == s3_location:
                # Same location — typically an Airflow retry after a failure
                # between CREATE and MSCK. MSCK is idempotent, so repair and
                # report success rather than letting a SKIPPED record mask an
                # earlier FAILED attempt for this run.
                logger.warning(
                    f"[HmsRegister] Table {database}.{table} already exists at {s3_location} — "
                    f"running MSCK REPAIR to ensure partitions are registered"
                )
                partition_columns = _discover_partition_columns(spark, s3_location)
                result["partition_columns"] = partition_columns
                if partition_columns:
                    spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
                result["status"] = "REGISTERED"
                _record("REGISTERED", partition_columns, None)
                logger.info(f"[HmsRegister] COMPLETED (already existed): {database}.{table}")
                return result

            msg = (
                f"Table {database}.{table} already exists in HMS at a different location "
                f"({existing_location or 'unknown'}) — skipping registration"
            )
            logger.warning(f"[HmsRegister] {msg}")
            result["status"] = "SKIPPED"
            result["error"] = msg
            _record("SKIPPED", [], msg)
            return result

        # Partition columns are registered as STRING: typed inference would
        # collapse month=01/month=1 into one value, lose leading zeros, and
        # make the registered type depend on the values present today. Keeping
        # partition keys string-typed also matches Athena/Glue guidance.
        spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        # mergeSchema unions the footers of ALL files, so columns added or
        # removed by schema evolution are not silently dropped from the table
        # (the default samples a single footer). Incompatible type evolution
        # across files fails loudly here instead of corrupting reads later.
        df = spark.read.option("mergeSchema", "true").parquet(s3_location)
        partition_columns = _discover_partition_columns(spark, s3_location)
        result["partition_columns"] = partition_columns

        schema_types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        missing = [c for c in partition_columns if c not in schema_types]
        if missing:
            raise Exception(
                f"Partition column(s) {missing} found in directory layout but "
                f"not in the schema inferred by Spark from {s3_location}"
            )

        data_cols = [
            f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields if f.name not in partition_columns
        ]
        if not data_cols:
            raise Exception(f"No data columns inferred from {s3_location} (all columns are partition columns?)")

        partition_clause = ""
        if partition_columns:
            part_defs = ", ".join(f"`{c}` {schema_types[c]}" for c in partition_columns)
            partition_clause = f"PARTITIONED BY ({part_defs})"

        logger.info(
            f"[HmsRegister] Creating {database}.{table} | location={s3_location} | "
            f"partition_cols={partition_columns or 'none'} | data_cols={len(data_cols)}"
        )

        spark.sql(f"""
            CREATE EXTERNAL TABLE {database}.{table} (
                {", ".join(data_cols)}
            )
            {partition_clause}
            STORED AS PARQUET
            LOCATION '{s3_location}'
        """)

        if partition_columns:
            logger.info(f"[HmsRegister] MSCK REPAIR TABLE {database}.{table}")
            spark.sql(f"MSCK REPAIR TABLE {database}.{table}")

        result["status"] = "REGISTERED"
        result["error"] = None
        _record("REGISTERED", partition_columns, None)
        logger.info(f"[HmsRegister] COMPLETED: {database}.{table}")

    except Exception as e:
        error_msg = f"HMS registration failed for {database}.{table}: {str(e)[:2000]}"
        logger.error(f"[HmsRegister] {error_msg}")
        result["status"] = "FAILED"
        result["error"] = str(e)[:2000]
        result["_has_failures"] = True
        result["_failure_summary"] = error_msg
        try:
            _record("FAILED", result["partition_columns"], str(e))
        except Exception as track_err:
            logger.error(f"[HmsRegister] Could not record FAILED status for {database}.{table}: {track_err}")
        # Push the result before raising so downstream all_done tasks still see it
        context["ti"].xcom_push(key="return_value", value=result)
        raise Exception(error_msg) from e

    return result


@task.pyspark(conn_id="spark_default")
@track_duration
def validate_registered_tables(registration_result: dict, spark, **context) -> dict:
    """Validate a registered table: row count via HMS vs direct parquet read, partition counts."""
    if not isinstance(registration_result, dict) or "run_id" not in registration_result:
        logger.warning(f"[HmsValidate] Skipping invalid input: {type(registration_result)}")
        return {}

    database = registration_result["database"]
    table = registration_result["table"]
    s3_location = registration_result["s3_location"]
    partition_columns = registration_result.get("partition_columns") or []

    if registration_result.get("status") != "REGISTERED":
        logger.info(
            f"[HmsValidate] {database}.{table}: status={registration_result.get('status')} — skipping validation"
        )
        return {**registration_result, "validation_status": "SKIPPED"}

    result = {**registration_result, "validation_status": "FAILED"}

    try:
        hms_count = spark.sql(f"SELECT COUNT(*) as c FROM {database}.{table}").collect()[0]["c"]
        parquet_df = spark.read.parquet(s3_location)
        parquet_count = parquet_df.count()
        row_count_match = hms_count == parquet_count

        hms_partition_count = 0
        s3_partition_count = 0
        partition_count_match = True
        if partition_columns:
            hms_partition_count = spark.sql(f"SHOW PARTITIONS {database}.{table}").count()
            # Count leaf key=value directories rather than DISTINCT over the
            # data, so empty partition dirs (registered by MSCK but holding no
            # rows) don't produce a false mismatch.
            s3_partition_count = _count_leaf_partition_dirs(spark, s3_location, len(partition_columns))
            partition_count_match = hms_partition_count == s3_partition_count

        mismatches = []
        if not row_count_match:
            mismatches.append(f"row count mismatch: hms={hms_count} parquet={parquet_count}")
        if not partition_count_match:
            mismatches.append(f"partition count mismatch: hms={hms_partition_count} s3={s3_partition_count}")

        result.update(
            {
                "validation_status": "COMPLETED",
                "hms_row_count": hms_count,
                "parquet_row_count": parquet_count,
                "row_count_match": row_count_match,
                "hms_partition_count": hms_partition_count,
                "s3_partition_count": s3_partition_count,
                "partition_count_match": partition_count_match,
                "validation_error": "; ".join(mismatches) if mismatches else None,
            }
        )

        logger.info(
            f"[HmsValidate] DONE: {database}.{table} | rows={'OK' if row_count_match else 'MISMATCH'} | "
            f"partitions={'OK' if partition_count_match else 'MISMATCH'}"
        )

        if mismatches:
            result["_has_failures"] = True
            result["_failure_summary"] = f"Validation mismatches for {database}.{table}: {'; '.join(mismatches)}"
            context["ti"].xcom_push(key="return_value", value=result)
            raise Exception(result["_failure_summary"])

    except Exception as e:
        if result.get("_has_failures"):
            raise  # mismatch path above — already recorded and pushed
        result["validation_status"] = "FAILED"
        result["validation_error"] = str(e)[:2000]
        result["_has_failures"] = True
        result["_failure_summary"] = f"Validation failed for {database}.{table}: {str(e)[:300]}"
        context["ti"].xcom_push(key="return_value", value=result)
        raise Exception(result["_failure_summary"]) from e

    return result


@task.pyspark(conn_id="spark_default")
def update_hms_validation_status(validation_result: dict, spark) -> dict:
    """Write validation results back to the tracking table."""
    if not isinstance(validation_result, dict) or "run_id" not in validation_result:
        logger.warning(f"[update_hms_validation_status] Skipping invalid input: {type(validation_result)}")
        return {}

    config = get_config()
    tracking_db = config["tracking_database"]

    run_id = validation_result["run_id"]
    database = validation_result["database"]
    table = validation_result["table"]
    validation_status = validation_result.get("validation_status", "SKIPPED")
    validation_duration = validation_result.get("_task_duration", 0.0)

    if validation_status == "SKIPPED":
        # SKIPPED/FAILED registrations keep their original tracking row untouched
        return validation_result

    if validation_status == "COMPLETED":
        validated = validation_result.get("row_count_match", False) and validation_result.get(
            "partition_count_match", False
        )
        overall_status = "VALIDATED" if validated else "VALIDATION_FAILED"
        error = validation_result.get("validation_error")
        error_sql = "'" + str(error)[:2000].replace("'", "''") + "'" if error else "NULL"
        execute_with_iceberg_retry(
            spark,
            f"""
            UPDATE {tracking_db}.hms_registration_status
            SET status = '{overall_status}',
                validation_status = '{validation_status}',
                validation_duration_seconds = {validation_duration},
                hms_row_count = {validation_result.get("hms_row_count", 0)},
                parquet_row_count = {validation_result.get("parquet_row_count", 0)},
                row_count_match = {str(validation_result.get("row_count_match", False)).lower()},
                hms_partition_count = {validation_result.get("hms_partition_count", 0)},
                s3_partition_count = {validation_result.get("s3_partition_count", 0)},
                partition_count_match = {str(validation_result.get("partition_count_match", False)).lower()},
                error_message = {error_sql},
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND database_name = '{database}'
              AND table_name = '{table}'
        """,
            task_label=f"hms_validation_status:{database}.{table}",
        )
    else:
        error = str(validation_result.get("validation_error", "unknown validation error"))[:2000].replace("'", "''")
        execute_with_iceberg_retry(
            spark,
            f"""
            UPDATE {tracking_db}.hms_registration_status
            SET status = 'VALIDATION_FAILED',
                validation_status = 'FAILED',
                validation_duration_seconds = {validation_duration},
                error_message = '{error}',
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND database_name = '{database}'
              AND table_name = '{table}'
        """,
            task_label=f"hms_validation_status:failed:{database}.{table}",
        )

    return validation_result


@task.pyspark(conn_id="spark_default")
def generate_hms_html_report(run_id: str, spark, **context) -> dict:
    """Generate HTML report for the HMS registration run and write it to S3."""
    from datetime import datetime

    config = get_config()
    tracking_db = config["tracking_database"]
    report_location = config["report_output_location"]

    rows = spark.sql(f"""
        SELECT * FROM {tracking_db}.hms_registration_status
        WHERE run_id = '{run_id}'
        ORDER BY database_name, table_name
    """).collect()

    total_tables = len(rows)
    successful_tables = sum(1 for t in rows if t.status in ("REGISTERED", "VALIDATED"))
    validated_tables = sum(1 for t in rows if t.status == "VALIDATED")
    skipped_tables = sum(1 for t in rows if (t.status or "") == "SKIPPED")
    failed_tables = sum(1 for t in rows if "FAILED" in (t.status or ""))
    total_rows_registered = sum(t.hms_row_count or 0 for t in rows)

    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Parquet HMS Registration Report - {run_id}</title>
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
        <h1>Parquet HMS Registration Report</h1>

        <div class="timestamp">
            Generated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} UTC<br>
            Run ID: <strong>{run_id}</strong>
        </div>

        <h2>Registration Summary</h2>
        <div class="summary-grid">
            <div class="summary-card">
                <h3>TOTAL TABLES</h3>
                <p class="value">{total_tables}</p>
            </div>
            <div class="summary-card success">
                <h3>REGISTERED</h3>
                <p class="value">{successful_tables}</p>
            </div>
            <div class="summary-card success">
                <h3>VALIDATED</h3>
                <p class="value">{validated_tables}</p>
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
                <p class="value">{total_rows_registered:,}</p>
            </div>
        </div>

        <div class="section-divider"></div>

        <h2>Table Registration Details</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>S3 Location</th>
                    <th>Partition Columns</th>
                    <th>Status</th>
                    <th>Registration Duration</th>
                    <th>Validation Duration</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in rows:
        if t.status == "VALIDATED":
            status_class = "status-validated"
        elif t.status == "REGISTERED":
            status_class = "status-completed"
        elif t.status == "SKIPPED":
            status_class = "status-skipped"
        else:
            status_class = "status-failed"

        reg_dur = f"{t.registration_duration_seconds:.1f}s" if t.registration_duration_seconds else "N/A"
        val_dur = f"{t.validation_duration_seconds:.1f}s" if t.validation_duration_seconds else "N/A"
        reason_tooltip = (
            str(t.error_message or "").replace('"', "&quot;")
            if t.status in ("SKIPPED", "FAILED", "VALIDATION_FAILED")
            else ""
        )
        badge_title_attr = f' title="{reason_tooltip}"' if reason_tooltip else ""

        html += f"""
                <tr>
                    <td>{t.database_name}</td>
                    <td><strong>{t.table_name}</strong></td>
                    <td class="duration">{t.s3_location}</td>
                    <td>{t.partition_columns or "—"}</td>
                    <td><span class="status-badge {status_class}"{badge_title_attr}>{t.status}</span></td>
                    <td class="duration">{reg_dur}</td>
                    <td class="duration">{val_dur}</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

        <div class="section-divider"></div>

        <h2>Validation Results (HMS vs Parquet)</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>HMS Rows</th>
                    <th>Parquet Rows</th>
                    <th>Row Count Match</th>
                    <th>HMS Partitions</th>
                    <th>S3 Partitions</th>
                    <th>Partition Match</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in rows:
        if t.validation_status == "COMPLETED":
            row_match_class = "validation-pass" if t.row_count_match else "validation-fail"
            row_match_icon = "✓ PASS" if t.row_count_match else "✗ FAIL"
            part_match_class = "validation-pass" if t.partition_count_match else "validation-fail"
            part_match_icon = "✓ PASS" if t.partition_count_match else "✗ FAIL"
        else:
            row_match_class = part_match_class = "duration"
            row_match_icon = part_match_icon = "N/A"

        html += f"""
                <tr>
                    <td>{t.database_name}</td>
                    <td><strong>{t.table_name}</strong></td>
                    <td class="metric">{(t.hms_row_count or 0):,}</td>
                    <td class="metric">{(t.parquet_row_count or 0):,}</td>
                    <td class="{row_match_class}">{row_match_icon}</td>
                    <td class="metric">{t.hms_partition_count or 0}</td>
                    <td class="metric">{t.s3_partition_count or 0}</td>
                    <td class="{part_match_class}">{part_match_icon}</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

        <div style="margin-top: 50px; padding-top: 20px; border-top: 1px solid #ecf0f1; color: #95a5a6; font-size: 12px;">
            <p>This report was automatically generated by the Parquet HMS Registration DAG.</p>
        </div>
    </div>
</body>
</html>
"""

    portal_run_id = context.get("params", {}).get("run_id") or run_id
    report_filename = f"{portal_run_id}_hms_report.html"
    report_path = f"{report_location}/{report_filename}"

    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(report_path), hadoop_conf)

    output_path = spark._jvm.org.apache.hadoop.fs.Path(report_path)
    output_stream = fs.create(output_path, True)
    output_stream.write(html.encode("utf-8"))
    output_stream.close()

    return {"report_path": report_path}


@task.pyspark(conn_id="spark_default")
def send_hms_report_email(report_result: dict, run_id: str, spark) -> dict:
    """Send HTML HMS registration report via email using SMTP."""
    import os
    import tempfile

    from airflow.utils.email import send_email

    config = get_config()
    smtp_conn_id = config.get("smtp_conn_id", "smtp_default")
    recipients_str = config.get("email_recipients", "")

    if not recipients_str:
        logger.warning("[Email] No recipients configured. Skipping email.")
        return {"sent": False, "reason": "no_recipients"}

    recipients = [r.strip() for r in recipients_str.split(",") if r.strip()]
    report_path = report_result.get("report_path", "")

    try:
        logger.info(f"[Email] Reading HMS HTML report from S3: {report_path}")
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(report_path), hadoop_conf)
        s3_path_obj = spark._jvm.org.apache.hadoop.fs.Path(report_path)
        reader = spark._jvm.java.io.BufferedReader(spark._jvm.java.io.InputStreamReader(fs.open(s3_path_obj), "UTF-8"))
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        html_content = "\n".join(lines)

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".html", prefix=f"{run_id}_hms_report_", delete=False)
        tmp.write(html_content)
        tmp.close()

        send_email(
            to=recipients,
            subject=f"Parquet HMS Registration Report - {run_id}",
            html_content=f"<p>Please find the parquet HMS registration report for run <strong>{run_id}</strong> attached.</p>",
            files=[tmp.name],
            conn_id=smtp_conn_id,
        )
        os.unlink(tmp.name)
        logger.info(f"[Email] HMS report sent to: {recipients}")
        return {"sent": True, "recipients": recipients, "report_path": report_path}
    except Exception as e:
        logger.error(f"[Email] Failed to send HMS report: {str(e)}")
        raise Exception(f"Failed to send HMS report email: {str(e)}") from e


@task.pyspark(conn_id="spark_default")
def finalize_hms_run(run_id: str, spark) -> dict:
    """Finalize HMS registration run - aggregate statistics."""
    config = get_config()
    tracking_db = config["tracking_database"]

    stats = {"total": 0, "successful": 0, "failed": 0, "skipped": 0}
    final_status = "FAILED"

    try:
        stats_result = spark.sql(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status IN ('REGISTERED', 'VALIDATED') THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN status IN ('FAILED', 'VALIDATION_FAILED') THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END) as skipped
            FROM {tracking_db}.hms_registration_status
            WHERE run_id = '{run_id}'
        """).collect()

        if not stats_result or stats_result[0]["total"] == 0:
            logger.warning(f"[finalize_hms_run] No table records found for run_id '{run_id}'.")
            final_status = "FAILED"
        else:
            stats = {
                "total": stats_result[0]["total"] or 0,
                "successful": stats_result[0]["successful"] or 0,
                "failed": stats_result[0]["failed"] or 0,
                "skipped": stats_result[0]["skipped"] or 0,
            }
            final_status = "COMPLETED" if stats["failed"] == 0 else "COMPLETED_WITH_FAILURES"

    except Exception as e:
        logger.error(f"[finalize_hms_run] Failed to query hms_registration_status: {str(e)}")
        final_status = "FAILED"

    try:
        execute_with_iceberg_retry(
            spark,
            f"""
            UPDATE {tracking_db}.hms_registration_runs
            SET status = '{final_status}',
                completed_at = current_timestamp(),
                total_tables = {stats["total"]},
                successful_tables = {stats["successful"]},
                failed_tables = {stats["failed"]},
                skipped_tables = {stats["skipped"]}
            WHERE run_id = '{run_id}'
        """,
            task_label="finalize_hms_run:update_hms_registration_runs",
        )

        logger.info(
            f"[finalize_hms_run] Run '{run_id}' finalized with status '{final_status}'. "
            f"total={stats['total']}, successful={stats['successful']}, failed={stats['failed']}"
        )

    except Exception as e:
        logger.error(f"[finalize_hms_run] Failed to update hms_registration_runs: {str(e)}")
        raise

    return {
        "run_id": run_id,
        "status": final_status,
        "total": stats["total"],
        "successful": stats["successful"],
        "failed": stats["failed"],
        "skipped": stats["skipped"],
    }


# =============================================================================
# DAG 4 DEFINITION: PARQUET HMS REGISTRATION
# =============================================================================

with DAG(
    dag_id="parquet_hms_registration",
    default_args=default_args,
    description="Register plain parquet files on S3 as external Hive Metastore tables",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=3,
    tags=["migration", "hive", "parquet", "hms"],
    params={
        "excel_file_path": Param(
            default="s3a://config-bucket/parquet_hms.xlsx",
            type="string",
            description="S3 path to Excel config file for parquet HMS registration",
        )
    },
    render_template_as_native_obj=True,
) as dag_parquet_hms:
    # Initialize
    t_hms_init = init_hms_tracking_tables()
    t_hms_run_id = create_hms_registration_run(
        excel_file_path="{{ params.excel_file_path }}", dag_run_id="{{ run_id }}"
    )
    t_hms_excel = parse_parquet_hms_excel(excel_file_path="{{ params.excel_file_path }}", run_id=t_hms_run_id)

    # Per-table registration
    t_hms_register = register_parquet_tables.partial(dag_run_id="{{ run_id }}").expand(table_config=t_hms_excel)
    t_hms_register.operator.trigger_rule = "all_done"

    # Validation
    t_hms_validate = validate_registered_tables.expand(registration_result=t_hms_register)
    t_hms_validate.operator.max_active_tis_per_dagrun = 3
    t_hms_validate.operator.trigger_rule = "all_done"
    t_hms_val_status = update_hms_validation_status.expand(validation_result=t_hms_validate)
    t_hms_val_status.operator.trigger_rule = "all_done"

    # Report generation
    t_hms_report = generate_hms_html_report(run_id=t_hms_run_id, params="{{ params }}")
    t_hms_report.operator.trigger_rule = "all_done"

    # Email report
    t_hms_email = send_hms_report_email(run_id=t_hms_run_id, report_result=t_hms_report)
    t_hms_email.operator.trigger_rule = "all_done"

    # Finalize
    t_hms_final = finalize_hms_run(run_id=t_hms_run_id)
    t_hms_final.operator.trigger_rule = "all_done"

    # Dependencies
    t_hms_init >> t_hms_run_id >> t_hms_excel >> t_hms_register >> t_hms_validate
    t_hms_validate >> t_hms_val_status >> t_hms_report >> t_hms_email >> t_hms_final
