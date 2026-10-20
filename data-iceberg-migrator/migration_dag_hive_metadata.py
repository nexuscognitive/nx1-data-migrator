"""
DAG: Hive metadata migration

Registers already-migrated Hive tables in a target Hive catalog. Source
definitions are read through Hive on the edge node; target metadata is created
through Spark in Airflow. This DAG never copies table data or migrates views.

Excel columns: source_database | source_table | target_database | target_table
               | target_location
"""

import base64
import html
import json
import logging
import os
import re
import shlex
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.ssh.hooks.ssh import SSHHook
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))

from migrator_utils.migrations.shared import (  # noqa: E402
    _login_shell,
    cell_str,
    cluster_login,
    execute_with_iceberg_retry,
    get_config,
    normalize_s3,
    sql_quote,
)

logger = logging.getLogger(__name__)
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DDL_HEADER = re.compile(
    r"(?is)\A(\s*CREATE\s+(?:EXTERNAL\s+)?TABLE\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?)(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)"
    r"(?:\s*\.\s*(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*))?"
)

_dag_stem = Path(__file__).stem
_config_dir = str(Path(__file__).resolve().parent / "migrator_utils" / "migration_configs")
if os.path.isdir(_config_dir):
    load_dotenv(os.path.join(_config_dir, "env.shared"))
    load_dotenv(os.path.join(_config_dir, f"env.{_dag_stem}"), override=True)
else:
    logger.warning("Config directory %s not found — using Airflow Variables / defaults", _config_dir)


def _resolve_dag_owner() -> str:
    """Resolve the same DAG owner principal used by the other migration DAGs."""
    try:
        from airflow.models import Variable

        owner = (Variable.get("migration_dag_owner", default_var="") or "").strip()
        if owner and "," not in owner:
            return owner
    except Exception:
        pass
    return "data-migration"


default_args = {
    "owner": _resolve_dag_owner(),
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _valid_identifier(value: str, label: str) -> str:
    if not _VALID_IDENTIFIER.fullmatch(value):
        raise ValueError(
            f"Invalid {label} {value!r}; use letters, digits, and underscores, starting with a letter or underscore."
        )
    return value


def _normalize_target_location(location: str) -> str:
    if location.startswith(("s3://", "s3a://", "s3n://")):
        return normalize_s3(location).rstrip("/")
    return location.rstrip("/")


def _tracking_tables(config: dict) -> tuple[str, str, str]:
    tracking_database = _valid_identifier(config["tracking_database"], "tracking_database")
    return (
        tracking_database,
        f"{tracking_database}.hive_metadata_migration_runs",
        f"{tracking_database}.hive_metadata_migration_objects",
    )


def _object_key_clause(table_config: dict) -> str:
    return (
        f"run_id = '{sql_quote(table_config['run_id'])}' "
        f"AND source_database = '{sql_quote(table_config['source_database'])}' "
        f"AND source_object = '{sql_quote(table_config['source_table'])}'"
    )


def _update_object_tracking(spark, table_config: dict, assignments: str, task_label: str) -> None:
    config = get_config()
    _, _, objects_table = _tracking_tables(config)
    execute_with_iceberg_retry(
        spark,
        f"UPDATE {objects_table} SET {assignments}, updated_at = current_timestamp() "
        f"WHERE {_object_key_clause(table_config)}",
        task_label=task_label,
    )


def _location_clause_spans(ddl: str) -> list[tuple[int, int]]:
    """Find LOCATION string clauses while ignoring quoted values and SQL comments."""
    spans = []
    index = 0
    while index < len(ddl):
        if ddl.startswith("--", index):
            newline = ddl.find("\n", index + 2)
            index = len(ddl) if newline < 0 else newline + 1
            continue
        if ddl.startswith("/*", index):
            comment_end = ddl.find("*/", index + 2)
            if comment_end < 0:
                return spans
            index = comment_end + 2
            continue
        if ddl[index] in ("'", '"', "`"):
            quote = ddl[index]
            index += 1
            while index < len(ddl):
                if ddl[index] == quote:
                    if quote == "'" and ddl[index : index + 2] == "''":
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            continue
        if ddl[index].isalpha() or ddl[index] == "_":
            start = index
            index += 1
            while index < len(ddl) and (ddl[index].isalnum() or ddl[index] == "_"):
                index += 1
            if ddl[start:index].upper() != "LOCATION":
                continue
            value_start = index
            while value_start < len(ddl) and ddl[value_start].isspace():
                value_start += 1
            if value_start >= len(ddl) or ddl[value_start] != "'":
                continue
            value_end = value_start + 1
            while value_end < len(ddl):
                if ddl[value_end] == "'":
                    if ddl[value_end : value_end + 2] == "''":
                        value_end += 2
                        continue
                    spans.append((start, value_end + 1))
                    index = value_end + 1
                    break
                value_end += 1
            continue
        index += 1
    return spans


def _rewrite_create_table_ddl(ddl: str, table_config: dict) -> str:
    """Retarget a Hive SHOW CREATE TABLE statement without changing its schema."""
    target_database = _valid_identifier(table_config["target_database"], "target_database")
    target_table = _valid_identifier(table_config["target_table"], "target_table")
    location = table_config["target_location"]
    if "'" in location or "\n" in location:
        raise ValueError("target_location contains an unsupported quote or newline")

    header_match = _DDL_HEADER.match(ddl)
    if not header_match:
        raise ValueError("Unsupported SHOW CREATE TABLE output: expected CREATE TABLE or CREATE EXTERNAL TABLE")

    ddl = f"{header_match.group(1)}`{target_database}`.`{target_table}`" + ddl[header_match.end() :]
    location_spans = _location_clause_spans(ddl)
    if len(location_spans) != 1:
        raise ValueError(f"Expected exactly one LOCATION clause in source table DDL, found {len(location_spans)}")
    start, end = location_spans[0]
    ddl = ddl[:start] + "LOCATION '" + location.replace("'", "''") + "'" + ddl[end:]
    return ddl


@task.pyspark(conn_id="spark_default")
def init_metadata_tracking_tables(spark) -> dict:
    """Create durable run- and object-level Iceberg tracking tables."""
    config = get_config()
    tracking_database, runs_table, objects_table = _tracking_tables(config)
    tracking_location = str(config["tracking_location"]).rstrip("/")
    if "'" in tracking_location or "\n" in tracking_location:
        raise ValueError("tracking_location contains an unsupported quote or newline")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {tracking_database} LOCATION '{tracking_location}'")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {runs_table} (
            run_id STRING,
            dag_run_id STRING,
            excel_file_path STRING,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            status STRING,
            total_objects INT,
            expected_objects INT,
            created_objects INT,
            existing_objects INT,
            skipped_objects INT,
            failed_objects INT,
            config_json STRING
        )
        USING iceberg
        LOCATION '{tracking_location}/hive_metadata_migration_runs'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {objects_table} (
            run_id STRING,
            source_database STRING,
            source_object STRING,
            target_database STRING,
            target_object STRING,
            object_type STRING,
            source_location STRING,
            target_location STRING,
            source_definition STRING,
            source_schema_json STRING,
            partition_columns_json STRING,
            allow_empty BOOLEAN,
            data_presence_status STRING,
            data_presence_checked_at TIMESTAMP,
            discovery_status STRING,
            discovery_completed_at TIMESTAMP,
            discovery_duration_seconds DOUBLE,
            metadata_creation_status STRING,
            metadata_creation_completed_at TIMESTAMP,
            metadata_creation_duration_seconds DOUBLE,
            validation_status STRING,
            validation_completed_at TIMESTAMP,
            validation_duration_seconds DOUBLE,
            actual_target_location STRING,
            schema_match BOOLEAN,
            schema_differences STRING,
            overall_status STRING,
            error_message STRING,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (source_database)
        LOCATION '{tracking_location}/hive_metadata_migration_objects'
    """)
    return {
        "status": "initialized",
        "database": tracking_database,
        "runs_table": runs_table,
        "objects_table": objects_table,
    }


@task.pyspark(conn_id="spark_default")
def create_metadata_migration_run(excel_file_path: str, dag_run_id: str, spark) -> str:
    """Create the run record before parsing or extracting table metadata."""
    config = get_config()
    _, runs_table, _ = _tracking_tables(config)
    run_id = f"hm_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    run_config = {
        "cluster_type": config.get("cluster_type", ""),
        "tracking_database": config["tracking_database"],
    }
    execute_with_iceberg_retry(
        spark,
        f"""
        INSERT INTO {runs_table} (
            run_id, dag_run_id, excel_file_path, started_at, completed_at,
            status, total_objects, expected_objects, created_objects,
            existing_objects, skipped_objects, failed_objects, config_json
        ) VALUES (
            '{run_id}',
            '{sql_quote(dag_run_id)}',
            '{sql_quote(excel_file_path)}',
            current_timestamp(),
            NULL,
            'RUNNING',
            0, 0, 0, 0, 0, 0,
            '{sql_quote(json.dumps(run_config))}'
        )
        """,
        task_label="create_metadata_migration_run",
    )
    return run_id


def _parse_allow_empty(value, row_number: int) -> bool:
    normalized = cell_str(value).lower()
    if not normalized:
        return False
    if normalized in ("true", "yes", "1"):
        return True
    if normalized in ("false", "no", "0"):
        return False
    raise ValueError(
        f"Excel row {row_number} has invalid allow_empty value {normalized!r}; use true/false, yes/no, or 1/0"
    )


@task.pyspark(conn_id="spark_default")
def parse_metadata_excel(excel_file_path: str, run_id: str, spark) -> list[dict]:
    """Read and validate one explicit source-to-target table mapping per row."""
    from io import BytesIO

    import pandas as ps

    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    if row is None or row.content is None:
        raise ValueError(f"Excel config is empty or unreadable: {excel_file_path}")

    df = ps.read_excel(BytesIO(bytes(row.content)), engine="openpyxl")
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    required = {
        "source_database",
        "source_table",
        "target_database",
        "target_table",
        "target_location",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required Excel column(s): {', '.join(missing)}")

    configs = []
    seen_sources = set()
    seen_targets = set()
    for index, row in df.iterrows():
        config = {column: cell_str(row.get(column)) for column in required}
        if any(not value for value in config.values()):
            raise ValueError(f"Excel row {index + 2} has a blank required value")

        for key in ("source_database", "source_table", "target_database", "target_table"):
            _valid_identifier(config[key], key)
        if "'" in config["target_location"] or "\n" in config["target_location"]:
            raise ValueError(f"Excel row {index + 2} has an unsafe target_location")
        config["target_location"] = _normalize_target_location(config["target_location"])
        config["allow_empty"] = _parse_allow_empty(row.get("allow_empty"), index + 2)
        config["run_id"] = run_id

        source_key = (config["source_database"].lower(), config["source_table"].lower())
        target_key = (config["target_database"].lower(), config["target_table"].lower())
        if source_key in seen_sources:
            raise ValueError(f"Duplicate source object in Excel: {source_key[0]}.{source_key[1]}")
        if target_key in seen_targets:
            raise ValueError(f"Multiple Excel rows target {target_key[0]}.{target_key[1]}")
        seen_sources.add(source_key)
        seen_targets.add(target_key)
        configs.append(config)

    if not configs:
        raise ValueError("Excel config contains no table mappings")
    config = get_config()
    _, runs_table, _ = _tracking_tables(config)
    execute_with_iceberg_retry(
        spark,
        f"UPDATE {runs_table} SET expected_objects = {len(configs)} WHERE run_id = '{sql_quote(run_id)}'",
        task_label="hive_metadata:set_expected_objects",
    )
    logger.info("[HiveMetadata] Validated %s table mapping(s)", len(configs))
    return configs


@task
def setup_source_cluster_login(run_id: str) -> dict:
    """Authenticate to the edge cluster using the shared migration setup."""
    return cluster_login(run_id)


@task
def extract_source_table_definition(table_config: dict, cluster_setup: dict) -> dict:
    """Extract one table definition on the edge node and leave its payload there."""
    started = time.monotonic()
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config["ssh_conn_id"])
    temp_dir = str(cluster_setup.get("temp_dir") or "").rstrip("/")
    if not temp_dir:
        raise ValueError("Source cluster setup did not return a temp_dir")
    source_database = table_config["source_database"]
    source_table = table_config["source_table"]
    safe_object_key = re.sub(r"[^A-Za-z0-9_-]", "_", f"{source_database}_{source_table}")
    metadata_file = f"{temp_dir}/metadata_{safe_object_key}.json"
    payload = base64.b64encode(json.dumps(table_config).encode("utf-8")).decode("ascii")
    pyspark_script = f'''import base64
import json
import os
from pyspark.sql import SparkSession

item = json.loads(base64.b64decode("{payload}").decode("utf-8"))
spark = (
    SparkSession.builder
    .appName("hive_metadata_source_extract")
    .config("spark.hadoop.hive.exec.scratchdir", {json.dumps(str(config.get("hive_scratch_dir") or "/tmp/hive"))})
    .config("spark.hadoop.hive.exec.local.scratchdir", {json.dumps(str(config.get("hive_scratch_dir") or "/tmp/hive"))})
    .enableHiveSupport()
    .getOrCreate()
)
database = item["source_database"]
table = item["source_table"]
quoted = "`" + database + "`.`" + table + "`"
result = {{"source_database": database, "source_table": table}}
try:
    details = spark.sql("DESCRIBE FORMATTED " + quoted).collect()
    table_type = ""
    source_location = ""
    for row in details:
        key = (row["col_name"] or "").strip().rstrip(":").lower()
        value = (row["data_type"] or "").strip()
        if key in ("type", "table type"):
            table_type = value.upper()
        elif key == "location":
            source_location = value
    if "VIEW" in table_type:
        result.update({{"status": "SKIPPED", "reason": "SOURCE_IS_VIEW",
                       "object_type": "VIEW", "source_location": source_location}})
    else:
        ddl_rows = spark.sql("SHOW CREATE TABLE " + quoted).collect()
        ddl = "\\n".join(str(row[0]) for row in ddl_rows).strip()
        if not ddl:
            raise ValueError("SHOW CREATE TABLE returned no DDL")
        schema = [
            {{"name": field.name, "type": field.dataType.simpleString()}}
            for field in spark.table(quoted).schema.fields
        ]
        partition_columns = []
        in_partition_section = False
        for row in details:
            key = (row["col_name"] or "").strip().rstrip(":").lower()
            value = (row["data_type"] or "").strip()
            if key == "partition information":
                in_partition_section = True
            elif in_partition_section and key == "partition columns":
                partition_columns = [
                    part.strip("` ")
                    for part in value.split(",")
                    if part.strip("` ")
                ]
                break
            elif in_partition_section and not key:
                in_partition_section = False
        result.update({{"status": "EXTRACTED", "object_type": "TABLE",
                       "source_location": source_location, "source_definition": ddl,
                       "source_schema_json": json.dumps(schema),
                       "partition_columns_json": json.dumps(partition_columns)}})
except Exception as exc:
    result.update({{"status": "FAILED", "error": str(exc)[:2000]}})

with open({json.dumps(metadata_file)}, "w") as output:
    json.dump(result, output)
spark.stop()
'''
    remote_script = f"{temp_dir}/extract_{safe_object_key}.py"

    try:
        with ssh.get_conn() as client:
            sftp = client.open_sftp()
            try:
                with sftp.file(remote_script, "w") as script_file:
                    script_file.write(pyspark_script)
            finally:
                sftp.close()

            _, stdout, stderr = client.exec_command(
                _login_shell(
                    f"pyspark --master local[*] < {shlex.quote(remote_script)}",
                    config.get("cluster_type", "MapR"),
                ),
                timeout=3600,
                get_pty=False,
            )
            output = stdout.read().decode("utf-8", errors="replace")
            error_output = stderr.read().decode("utf-8", errors="replace")
            exit_code = stdout.channel.recv_exit_status()
            if exit_code:
                raise RuntimeError(
                    f"Source Hive extraction failed (exit={exit_code}): {(error_output or output)[-2000:]}"
                )
    except Exception as exc:
        return {
            **table_config,
            "status": "FAILED",
            "error": str(exc)[:2000],
            "discovery_duration_seconds": time.monotonic() - started,
        }
    finally:
        try:
            with ssh.get_conn() as client:
                _, cleanup_stdout, cleanup_stderr = client.exec_command(
                    _login_shell(
                        f"rm -f -- {shlex.quote(remote_script)}",
                        config.get("cluster_type", "MapR"),
                    ),
                    timeout=60,
                )
                if cleanup_stdout.channel.recv_exit_status():
                    cleanup_error = cleanup_stderr.read().decode("utf-8", errors="replace")
                    logger.warning(
                        "[HiveMetadata] Could not remove edge extraction script %s: %s",
                        remote_script,
                        cleanup_error[-500:],
                    )
        except Exception:
            logger.warning(
                "[HiveMetadata] Could not clean up edge extraction script %s",
                remote_script,
                exc_info=True,
            )

    return {
        **table_config,
        "status": "EXTRACTED",
        "remote_metadata_path": metadata_file,
        "discovery_duration_seconds": time.monotonic() - started,
    }


def _target_table_location(spark, target_name: str) -> str | None:
    for row in spark.sql(f"DESCRIBE FORMATTED {target_name}").collect():
        if (row["col_name"] or "").strip().lower() == "location":
            return (row["data_type"] or "").strip()
    return None


def _contains_data_file(fs, path) -> bool:
    """Return on the first visible file, ignoring Hadoop/Spark marker files."""
    files = fs.listFiles(path, True)
    while files.hasNext():
        file_status = files.next()
        name = str(file_status.getPath().getName())
        if not name.startswith((".", "_")):
            return True
    return False


@task.pyspark(conn_id="spark_default")
def record_metadata_discovery(discovery: dict, cluster_setup: dict, spark) -> dict:
    """Persist one extracted definition from the edge node to durable tracking."""
    table_config = discovery
    status = discovery.get("status", "FAILED")
    metadata = {}
    error = discovery.get("error")

    if status == "EXTRACTED":
        config = get_config()
        ssh = SSHHook(ssh_conn_id=config["ssh_conn_id"])
        try:
            with ssh.get_conn() as client:
                sftp = client.open_sftp()
                try:
                    with sftp.file(discovery["remote_metadata_path"], "r") as metadata_stream:
                        metadata = json.loads(metadata_stream.read().decode("utf-8"))
                finally:
                    sftp.close()
                cleanup = f"rm -f -- {shlex.quote(discovery['remote_metadata_path'])}"
                _, cleanup_stdout, cleanup_stderr = client.exec_command(
                    _login_shell(cleanup, config.get("cluster_type", "MapR")),
                    timeout=60,
                )
                cleanup_code = cleanup_stdout.channel.recv_exit_status()
                if cleanup_code:
                    cleanup_error = cleanup_stderr.read().decode("utf-8", errors="replace")
                    logger.warning(
                        "[HiveMetadata] Could not remove edge metadata file %s: %s",
                        discovery["remote_metadata_path"],
                        cleanup_error[-500:],
                    )
        except Exception as exc:
            status = "FAILED"
            error = f"Could not retrieve source metadata from edge node: {str(exc)[:1800]}"
            logger.exception(
                "[HiveMetadata] Failed to retrieve source definition for %s.%s",
                discovery["source_database"],
                discovery["source_table"],
            )
        else:
            status = metadata.get("status", "FAILED")
            error = metadata.get("error") or metadata.get("reason")

    config = get_config()
    _, _, objects_table = _tracking_tables(config)
    source_definition = metadata.get("source_definition", "")
    schema_json = metadata.get("source_schema_json", "[]")
    partition_columns_json = metadata.get("partition_columns_json", "[]")
    object_type = metadata.get("object_type", "TABLE")
    source_location = metadata.get("source_location", "")
    duration = float(discovery.get("discovery_duration_seconds", 0.0))
    final_status = status if status in ("EXTRACTED", "SKIPPED") else "FAILED"

    execute_with_iceberg_retry(
        spark,
        f"DELETE FROM {objects_table} WHERE {_object_key_clause(table_config)}",
        task_label=f"hive_metadata:replace_discovery:{table_config['source_table']}",
    )
    execute_with_iceberg_retry(
        spark,
        f"""
        INSERT INTO {objects_table} (
            run_id, source_database, source_object, target_database, target_object,
            object_type, source_location, target_location, source_definition,
            source_schema_json, partition_columns_json, allow_empty,
            data_presence_status, data_presence_checked_at, discovery_status,
            discovery_completed_at, discovery_duration_seconds,
            metadata_creation_status, metadata_creation_completed_at,
            metadata_creation_duration_seconds, validation_status,
            validation_completed_at, validation_duration_seconds,
            actual_target_location, schema_match, schema_differences,
            overall_status, error_message, updated_at
        ) VALUES (
            '{sql_quote(table_config["run_id"])}',
            '{sql_quote(table_config["source_database"])}',
            '{sql_quote(table_config["source_table"])}',
            '{sql_quote(table_config["target_database"])}',
            '{sql_quote(table_config["target_table"])}',
            '{sql_quote(object_type)}',
            '{sql_quote(source_location)}',
            '{sql_quote(table_config["target_location"])}',
            '{sql_quote(source_definition)}',
            '{sql_quote(schema_json)}',
            '{sql_quote(partition_columns_json)}',
            {str(bool(table_config.get("allow_empty", False))).lower()},
            'NOT_CHECKED',
            NULL,
            '{final_status}',
            current_timestamp(),
            {duration},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            '{final_status}',
            {f"'{sql_quote(error, 2000)}'" if error else "NULL"},
            current_timestamp()
        )
        """,
        task_label=f"hive_metadata:record_discovery:{table_config['source_table']}",
    )
    return {
        "run_id": table_config["run_id"],
        "source_database": table_config["source_database"],
        "source_table": table_config["source_table"],
        "target_database": table_config["target_database"],
        "target_table": table_config["target_table"],
        "target_location": table_config["target_location"],
        "allow_empty": bool(table_config.get("allow_empty", False)),
        "status": final_status,
        "message": error,
    }


@task.pyspark(conn_id="spark_default")
def create_target_table_metadata(table_ref: dict, spark) -> dict:
    """Create table metadata at the supplied location without overwriting objects."""
    source_name = f"{table_ref['source_database']}.{table_ref['source_table']}"
    target_name = f"{table_ref['target_database']}.{table_ref['target_table']}"
    result = {
        "source": source_name,
        "target": target_name,
        "target_location": table_ref["target_location"],
        "allow_empty": bool(table_ref.get("allow_empty", False)),
        "data_presence_status": "NOT_CHECKED",
        **{
            key: table_ref[key]
            for key in ("run_id", "source_database", "source_table", "target_database", "target_table")
        },
        "status": table_ref["status"],
        "message": None,
    }

    if table_ref["status"] in ("SKIPPED", "FAILED"):
        result["message"] = table_ref.get("message") or table_ref["status"]
        creation_status = "SKIPPED" if table_ref["status"] == "SKIPPED" else "NOT_RUN"
        error_sql = f"'{sql_quote(result['message'], 2000)}'"
        _update_object_tracking(
            spark,
            table_ref,
            (
                f"metadata_creation_status = '{creation_status}', "
                "metadata_creation_completed_at = current_timestamp(), "
                "metadata_creation_duration_seconds = 0.0, "
                f"error_message = {error_sql}"
            ),
            task_label=f"hive_metadata:creation_not_run:{table_ref['source_table']}",
        )
        logger.warning("[HiveMetadata] %s: %s", source_name, result["message"])
        return result

    started = time.monotonic()
    try:
        target_database = _valid_identifier(table_ref["target_database"], "target_database")
        target_table = _valid_identifier(table_ref["target_table"], "target_table")
        quoted_target = f"`{target_database}`.`{target_table}`"
        path = table_ref["target_location"]
        jvm = spark._jvm
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(path), spark._jsc.hadoopConfiguration())
        target_path = jvm.org.apache.hadoop.fs.Path(path)
        if not fs.exists(target_path):
            result["data_presence_status"] = "PATH_MISSING"
            raise ValueError(f"Target data location does not exist: {path}")
        has_data_file = _contains_data_file(fs, target_path)
        if has_data_file:
            result["data_presence_status"] = "DATA_FOUND"
        elif result["allow_empty"]:
            result["data_presence_status"] = "EMPTY_ALLOWED"
        else:
            result["data_presence_status"] = "EMPTY_REJECTED"
            raise ValueError(
                f"No visible data files found under target location {path}; "
                "set allow_empty=true only for an intentionally empty table."
            )

        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{target_database}`")
        if spark.catalog.tableExists(f"{target_database}.{target_table}"):
            existing_location = _target_table_location(spark, quoted_target)
            expected_location = table_ref["target_location"]
            if existing_location and _normalize_target_location(existing_location) == expected_location:
                result["status"] = "ALREADY_EXISTS"
                result["actual_target_location"] = existing_location
                result["message"] = (
                    "Target object already exists at the requested location; it was not overwritten "
                    "or schema-reconciled."
                )
                logger.info("[HiveMetadata] %s already exists at the requested location", target_name)
            else:
                result["status"] = "CONFLICT"
                result["message"] = (
                    f"Target object already exists at a different or unknown location "
                    f"({existing_location or 'unknown'}); it was not modified."
                )
                logger.error("[HiveMetadata] %s: %s", target_name, result["message"])
        else:
            config = get_config()
            _, _, objects_table = _tracking_tables(config)
            tracked = spark.sql(
                f"SELECT source_definition FROM {objects_table} WHERE {_object_key_clause(table_ref)}"
            ).collect()
            if not tracked:
                raise ValueError(f"No persisted source definition found for {source_name}")
            source_definition = tracked[0]["source_definition"]
            if not source_definition:
                raise ValueError(f"Persisted source definition is empty for {source_name}")

            ddl = _rewrite_create_table_ddl(source_definition, table_ref)
            spark.sql(ddl)
            actual_location = _target_table_location(spark, quoted_target)
            if actual_location and _normalize_target_location(actual_location) != path:
                raise RuntimeError(f"Created target location mismatch: expected {path}, found {actual_location}")
            result["status"] = "CREATED"
            result["actual_target_location"] = actual_location or path
            result["message"] = "Target table metadata created; data was not copied."
            logger.info("[HiveMetadata] Created %s at %s", target_name, path)
    except Exception as exc:
        if result["data_presence_status"] == "NOT_CHECKED":
            result["data_presence_status"] = "CHECK_FAILED"
        result["status"] = "FAILED"
        result["message"] = str(exc)[:2000]
        logger.exception("[HiveMetadata] Failed to create %s", target_name)
    duration = time.monotonic() - started
    status = result["status"]
    error_sql = f"'{sql_quote(result['message'], 2000)}'" if status in ("FAILED", "CONFLICT") else "NULL"
    actual_location_sql = (
        f"'{sql_quote(result['actual_target_location'])}'" if result.get("actual_target_location") else "NULL"
    )
    creation_status = "CONFLICT" if status == "CONFLICT" else status
    _update_object_tracking(
        spark,
        table_ref,
        (
            f"metadata_creation_status = '{creation_status}', "
            "metadata_creation_completed_at = current_timestamp(), "
            f"metadata_creation_duration_seconds = {duration}, "
            f"data_presence_status = '{result['data_presence_status']}', "
            "data_presence_checked_at = current_timestamp(), "
            f"actual_target_location = {actual_location_sql}, "
            f"overall_status = '{creation_status}', error_message = {error_sql}"
        ),
        task_label=f"hive_metadata:create:{table_ref['source_table']}",
    )
    return result


@task.pyspark(conn_id="spark_default")
def validate_target_table_metadata(table_result: dict, spark) -> dict:
    """Compare target table schema and location with the captured source metadata."""
    table_ref = table_result
    source_name = table_result["source"]
    target_name = table_result["target"]
    started = time.monotonic()
    result = dict(table_result)
    creation_status = table_result["status"]

    if creation_status not in ("CREATED", "ALREADY_EXISTS"):
        result["validation_status"] = "NOT_RUN"
        _update_object_tracking(
            spark,
            table_ref,
            (
                "validation_status = 'NOT_RUN', "
                "validation_completed_at = current_timestamp(), "
                "validation_duration_seconds = 0.0"
            ),
            task_label=f"hive_metadata:validation_not_run:{table_ref['source_table']}",
        )
        return result

    config = get_config()
    _, _, objects_table = _tracking_tables(config)
    try:
        stored = spark.sql(
            f"SELECT source_schema_json FROM {objects_table} WHERE {_object_key_clause(table_ref)}"
        ).collect()
        if not stored:
            raise ValueError(f"No source schema tracking record found for {source_name}")
        source_schema = json.loads(stored[0]["source_schema_json"] or "[]")
        target_schema = [
            {"name": field.name, "type": field.dataType.simpleString()}
            for field in spark.table(
                f"`{_valid_identifier(table_ref['target_database'], 'target_database')}`."
                f"`{_valid_identifier(table_ref['target_table'], 'target_table')}`"
            ).schema.fields
        ]
        source_by_name = {field["name"].lower(): field["type"].lower() for field in source_schema}
        target_by_name = {field["name"].lower(): field["type"].lower() for field in target_schema}
        differences = []
        for name, source_type in source_by_name.items():
            if name not in target_by_name:
                differences.append(f"Missing target column: {name}")
            elif target_by_name[name] != source_type:
                differences.append(f"Type mismatch for {name}: source={source_type}, target={target_by_name[name]}")
        for name in target_by_name.keys() - source_by_name.keys():
            differences.append(f"Unexpected target column: {name}")

        actual_location = _target_table_location(
            spark,
            f"`{_valid_identifier(table_ref['target_database'], 'target_database')}`."
            f"`{_valid_identifier(table_ref['target_table'], 'target_table')}`",
        )
        if not actual_location:
            differences.append("Could not read target table location")
        elif _normalize_target_location(actual_location) != table_ref["target_location"]:
            differences.append(f"Location mismatch: expected={table_ref['target_location']}, actual={actual_location}")

        validation_status = "VALIDATED" if not differences else "VALIDATION_FAILED"
        message = "; ".join(differences) if differences else None
        result["validation_status"] = validation_status
        result["schema_match"] = not any(
            item.startswith(("Missing target column:", "Type mismatch for", "Unexpected target column:"))
            for item in differences
        )
        result["schema_differences"] = differences
        result["actual_target_location"] = actual_location
        result["message"] = message
    except Exception as exc:
        validation_status = "VALIDATION_FAILED"
        message = str(exc)[:2000]
        result.update(
            {
                "validation_status": validation_status,
                "schema_match": False,
                "schema_differences": [message],
                "message": message,
            }
        )
        logger.exception("[HiveMetadata] Validation failed for %s", target_name)

    duration = time.monotonic() - started
    error_sql = f"'{sql_quote(result['message'], 2000)}'" if result.get("message") else "NULL"
    actual_location_sql = (
        f"'{sql_quote(result['actual_target_location'])}'" if result.get("actual_target_location") else "NULL"
    )
    schema_differences_sql = f"'{sql_quote(json.dumps(result.get('schema_differences', [])))}'"
    overall_status = "VALIDATED" if validation_status == "VALIDATED" else "VALIDATION_FAILED"
    _update_object_tracking(
        spark,
        table_ref,
        (
            f"validation_status = '{validation_status}', "
            "validation_completed_at = current_timestamp(), "
            f"validation_duration_seconds = {duration}, "
            f"actual_target_location = {actual_location_sql}, "
            f"schema_match = {str(result.get('schema_match', False)).lower()}, "
            f"schema_differences = {schema_differences_sql}, "
            f"overall_status = '{overall_status}', error_message = {error_sql}"
        ),
        task_label=f"hive_metadata:validate:{table_ref['source_table']}",
    )
    return result


@task.pyspark(conn_id="spark_default")
def finalize_metadata_migration_run(run_id: str, spark) -> dict:
    """Aggregate durable object statuses and finalize the run record."""
    config = get_config()
    _, runs_table, objects_table = _tracking_tables(config)
    run_rows = spark.sql(f"SELECT expected_objects FROM {runs_table} WHERE run_id = '{sql_quote(run_id)}'").collect()
    expected_total = int(run_rows[0]["expected_objects"] or 0) if run_rows else 0
    statuses = [
        {
            "overall_status": row["overall_status"] or "FAILED",
            "metadata_creation_status": row["metadata_creation_status"],
            "validation_status": row["validation_status"],
        }
        for row in spark.sql(
            f"SELECT overall_status, metadata_creation_status, validation_status "
            f"FROM {objects_table} WHERE run_id = '{sql_quote(run_id)}'"
        ).collect()
    ]
    total = max(expected_total, len(statuses))
    created = sum(
        row["metadata_creation_status"] == "CREATED" and row["validation_status"] == "VALIDATED" for row in statuses
    )
    skipped = sum(row["overall_status"] in ("SKIPPED", "SOURCE_IS_VIEW") for row in statuses)
    existing = sum(
        row["metadata_creation_status"] == "ALREADY_EXISTS" and row["validation_status"] == "VALIDATED"
        for row in statuses
    )
    failed = sum(
        row["overall_status"] in ("FAILED", "CONFLICT", "VALIDATION_FAILED")
        or row["validation_status"] == "VALIDATION_FAILED"
        or row["overall_status"] not in ("VALIDATED", "SKIPPED")
        for row in statuses
    ) + max(0, expected_total - len(statuses))
    if not total:
        final_status = "FAILED"
        failed = max(failed, 1)
    elif failed:
        final_status = "COMPLETED_WITH_FAILURES"
    elif skipped:
        final_status = "COMPLETED_WITH_SKIPS"
    else:
        final_status = "COMPLETED"

    execute_with_iceberg_retry(
        spark,
        f"""
        UPDATE {runs_table}
        SET status = '{final_status}',
            completed_at = current_timestamp(),
            total_objects = {total},
            created_objects = {created},
            existing_objects = {existing},
            skipped_objects = {skipped},
            failed_objects = {failed}
        WHERE run_id = '{sql_quote(run_id)}'
        """,
        task_label="finalize_metadata_migration_run",
    )
    summary = {
        "run_id": run_id,
        "status": final_status,
        "total": total,
        "created": created,
        "already_exists": existing,
        "skipped": skipped,
        "failed": failed,
    }
    logger.info("[HiveMetadata] Final run summary: %s", summary)
    return summary


@task.pyspark(conn_id="spark_default")
def generate_metadata_migration_report(run_id: str, spark) -> dict:
    """Render durable run/object tracking into an HTML report in the configured S3 path."""
    config = get_config()
    _, runs_table, objects_table = _tracking_tables(config)
    run_rows = spark.sql(f"SELECT * FROM {runs_table} WHERE run_id = '{sql_quote(run_id)}'").collect()
    if not run_rows:
        raise ValueError(f"No run tracking record found for {run_id}")
    object_rows = spark.sql(
        f"SELECT * FROM {objects_table} WHERE run_id = '{sql_quote(run_id)}' ORDER BY source_database, source_object"
    ).collect()

    run = run_rows[0]
    report_path = f"{str(config['report_output_location']).rstrip('/')}/{run_id}_hive_metadata_report.html"

    def status_badge(value: object) -> str:
        label = str(value or "UNKNOWN")
        normalized = label.upper()
        if normalized in ("VALIDATED", "COMPLETED", "CREATED"):
            status_class = "success"
        elif normalized in ("SKIPPED", "COMPLETED_WITH_SKIPS", "EMPTY_ALLOWED"):
            status_class = "skipped"
        elif normalized in ("FAILED", "CONFLICT", "VALIDATION_FAILED", "COMPLETED_WITH_FAILURES"):
            status_class = "failed"
        else:
            status_class = "neutral"
        return f'<span class="badge {status_class}">{html.escape(label)}</span>'

    row_html = []
    for item in object_rows:
        values = (
            item["source_database"],
            item["source_object"],
            item["target_database"],
            item["target_object"],
            item["object_type"],
            item["target_location"],
        )
        cells = [f"<td>{html.escape(str(value or ''))}</td>" for value in values]
        cells.extend(
            (
                f"<td>{status_badge(item['data_presence_status'])}</td>",
                f"<td>{'Yes' if item['allow_empty'] else 'No'}</td>",
                f"<td>{status_badge(item['overall_status'])}</td>",
                f"<td>{status_badge(item['validation_status'])}</td>",
                f"<td>{html.escape(str(item['error_message'] or ''))}</td>",
            )
        )
        row_html.append("<tr>" + "".join(cells) + "</tr>")

    status = str(run["status"] or "UNKNOWN")
    summary_cards = [
        ("Total tables", run["total_objects"] or 0, "info"),
        ("Created & validated", run["created_objects"] or 0, "success"),
        ("Already present", run["existing_objects"] or 0, "info"),
        ("Skipped", run["skipped_objects"] or 0, "skipped"),
        ("Failed", run["failed_objects"] or 0, "failed"),
    ]
    cards_html = "".join(
        f'<div class="summary-card {style}"><h2>{html.escape(label)}</h2><p class="value">{value}</p></div>'
        for label, value, style in summary_cards
    )
    dag_run_id = html.escape(str(run["dag_run_id"] or ""))

    content = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hive Metadata Migration Report - {html.escape(run_id)}</title>
<style>
*{{box-sizing:border-box}}
body{{font-family:'Segoe UI',Tahoma,Arial,sans-serif;margin:0;padding:24px;background:#f5f6fa;color:#243447}}
.container{{max-width:1500px;margin:0 auto;background:#fff;padding:30px;border-radius:10px;box-shadow:0 3px 14px rgba(0,0,0,.10)}}
h1{{margin:0 0 8px;color:#2c3e50;border-bottom:3px solid #3498db;padding-bottom:14px}}
.subtitle{{color:#68788a;margin:0 0 20px}}
.run-meta{{display:flex;flex-wrap:wrap;gap:8px 24px;color:#637184;font-size:13px;margin:18px 0 28px}}
.run-meta strong{{color:#34495e}}
h2.section-title{{font-size:20px;color:#34495e;margin:24px 0 14px}}
.summary-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:16px}}
.summary-card{{padding:18px;border-radius:8px;color:#fff;background:linear-gradient(135deg,#4facfe,#2878c7);box-shadow:0 3px 8px rgba(0,0,0,.10)}}
.summary-card.success{{background:linear-gradient(135deg,#11998e,#38a879)}}
.summary-card.skipped{{background:linear-gradient(135deg,#f6a742,#e87932)}}
.summary-card.failed{{background:linear-gradient(135deg,#f16b70,#c83e55)}}
.summary-card h2{{font-size:12px;text-transform:uppercase;letter-spacing:.06em;margin:0 0 10px;opacity:.95}}
.summary-card .value{{font-size:30px;font-weight:700;margin:0}}
.table-wrap{{overflow-x:auto;border:1px solid #e2e7ed;border-radius:8px}}
table{{width:100%;border-collapse:collapse;min-width:1200px;font-size:13px}}
th{{position:sticky;top:0;background:#34495e;color:#fff;padding:12px 10px;text-align:left;white-space:nowrap}}
td{{padding:10px;border-bottom:1px solid #e8edf2;vertical-align:top}}
tbody tr:nth-child(even){{background:#f8fafc}}tbody tr:hover{{background:#eef6fc}}
td:nth-child(6),td:last-child{{overflow-wrap:anywhere;max-width:300px}}
.badge{{display:inline-block;padding:4px 9px;border-radius:14px;font-size:11px;font-weight:700;white-space:nowrap;background:#e8edf2;color:#526171}}
.badge.success{{background:#d8f3e7;color:#13764c}}.badge.failed{{background:#fde2e2;color:#a52834}}
.badge.skipped{{background:#fff0d5;color:#93600c}}.badge.neutral{{background:#e8edf2;color:#526171}}
.empty{{text-align:center;color:#718096;padding:26px}}
@media(max-width:700px){{body{{padding:10px}}.container{{padding:18px}}}}
</style></head>
<body><main class="container">
<h1>Hive Metadata Migration Report</h1>
<p class="subtitle">Table metadata registration and target validation summary</p>
<div class="run-meta"><span><strong>Run ID:</strong> {html.escape(run_id)}</span>
<span><strong>Airflow run:</strong> {dag_run_id}</span>
<span><strong>Run status:</strong> {status_badge(status)}</span></div>
<h2 class="section-title">Migration Summary</h2>
<section class="summary-grid">{cards_html}</section>
<h2 class="section-title">Table Results</h2>
<div class="table-wrap"><table><thead><tr><th>Source database</th><th>Source table</th>
<th>Target database</th><th>Target table</th><th>Object type</th><th>Target location</th>
<th>Data presence</th><th>Allow empty</th><th>Status</th><th>Validation</th><th>Details</th></tr></thead>
<tbody>{"".join(row_html) if row_html else '<tr><td class="empty" colspan="11">No table results recorded for this run.</td></tr>'}</tbody>
</table></div></main></body></html>"""

    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(report_path), spark._jsc.hadoopConfiguration())
    output_stream = fs.create(jvm.org.apache.hadoop.fs.Path(report_path), True)
    try:
        output_stream.write(content.encode("utf-8"))
    finally:
        output_stream.close()
    return {"run_id": run_id, "report_path": report_path}


@task.pyspark(conn_id="spark_default")
def send_metadata_migration_report(report_result: dict, spark) -> dict:
    """Email the HTML report when recipients are configured."""
    from airflow.utils.email import send_email

    config = get_config()
    recipients = [value.strip() for value in config.get("email_recipients", "").split(",") if value.strip()]
    if not recipients:
        logger.info("[HiveMetadata] No email recipients configured; skipping report email")
        return {"sent": False, "reason": "no_recipients"}

    path = report_result["report_path"]
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(path), spark._jsc.hadoopConfiguration())
    reader = jvm.java.io.BufferedReader(
        jvm.java.io.InputStreamReader(fs.open(jvm.org.apache.hadoop.fs.Path(path)), "UTF-8")
    )
    lines = []
    try:
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
    finally:
        reader.close()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".html", prefix="hive_metadata_report_", delete=False) as temp:
        temp.write("\n".join(lines))
        temp_path = temp.name
    try:
        send_email(
            to=recipients,
            subject=f"Hive Metadata Migration Report - {report_result['run_id']}",
            html_content=f"<p>Hive metadata migration completed for run <strong>{html.escape(report_result['run_id'])}</strong>.</p>",
            files=[temp_path],
            conn_id=config.get("smtp_conn_id", "smtp_default"),
        )
    finally:
        os.unlink(temp_path)
    return {"sent": True, "recipients": recipients, "report_path": path}


with DAG(
    dag_id="hive_metadata_migration",
    default_args=default_args,
    description="Register already-migrated Hive tables in a target catalog without copying data",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["migration", "hive", "metadata"],
    params={
        "excel_file_path": Param(
            default="s3a://config-bucket/hive_metadata_migration.xlsx",
            type="string",
            description="S3 path to the source-to-target Hive table mapping workbook",
        ),
    },
    render_template_as_native_obj=True,
) as dag_hive_metadata:
    t_tracking = init_metadata_tracking_tables()
    t_run_id = create_metadata_migration_run(
        excel_file_path="{{ params.excel_file_path }}",
        dag_run_id="{{ run_id }}",
    )
    t_config = parse_metadata_excel(
        excel_file_path="{{ params.excel_file_path }}",
        run_id=t_run_id,
    )
    t_source_login = setup_source_cluster_login(run_id="{{ run_id }}")
    t_extract = extract_source_table_definition.partial(cluster_setup=t_source_login).expand(table_config=t_config)
    t_discovery = record_metadata_discovery.partial(cluster_setup=t_source_login).expand(discovery=t_extract)
    t_create = create_target_table_metadata.expand(table_ref=t_discovery)
    t_validate = validate_target_table_metadata.expand(table_result=t_create)
    t_finalize = finalize_metadata_migration_run(run_id=t_run_id)
    t_report = generate_metadata_migration_report(run_id=t_run_id)
    t_email = send_metadata_migration_report(report_result=t_report)
    t_finalize.operator.trigger_rule = "all_done"
    t_report.operator.trigger_rule = "all_done"
    t_email.operator.trigger_rule = "all_done"

    t_tracking >> t_run_id >> t_config
    t_config >> t_source_login >> t_extract
    t_extract >> t_discovery >> t_create >> t_validate >> t_finalize >> t_report >> t_email
