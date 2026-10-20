"""Tests for the metadata-only Hive migration DAG."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import migration_dag_hive_metadata as m
import pytest

from .helpers import make_excel_bytes, setup_spark_excel


def _excel_row(**overrides):
    row = {
        "source_database": "sales",
        "source_table": "orders",
        "target_database": "sales_target",
        "target_table": "orders",
        "target_location": "s3a://warehouse/finance/orders",
    }
    row.update(overrides)
    return row


class TestParseMetadataExcel:
    def test_parses_explicit_mapping_and_run_id(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([_excel_row()]))

        result = m.parse_metadata_excel.function("s3a://config/mapping.xlsx", run_id="run-123", spark=mock_spark)

        assert result == [{**_excel_row(), "allow_empty": False, "run_id": "run-123"}]

    def test_parses_optional_allow_empty_value(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([_excel_row(allow_empty="yes")]))

        result = m.parse_metadata_excel.function("s3a://config/mapping.xlsx", run_id="run-123", spark=mock_spark)

        assert result[0]["allow_empty"] is True

    def test_rejects_invalid_allow_empty_value(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([_excel_row(allow_empty="sometimes")]))

        with pytest.raises(ValueError, match="invalid allow_empty"):
            m.parse_metadata_excel.function("s3a://config/mapping.xlsx", run_id="run-123", spark=mock_spark)

    def test_normalizes_headers_and_s3_location(self, mock_spark):
        setup_spark_excel(
            mock_spark,
            make_excel_bytes(
                [
                    {
                        "Source Database": "sales",
                        "Source Table": "orders",
                        "Target Database": "sales_target",
                        "Target Table": "orders",
                        "Target Location": "s3://warehouse/finance/orders/",
                    }
                ]
            ),
        )

        result = m.parse_metadata_excel.function("s3a://config/mapping.xlsx", run_id="run-123", spark=mock_spark)

        assert result[0]["target_location"] == "s3a://warehouse/finance/orders"

    def test_missing_required_column_fails(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([{"source_database": "sales"}]))

        with pytest.raises(ValueError, match="Missing required Excel column"):
            m.parse_metadata_excel.function("s3a://config/mapping.xlsx", run_id="run-123", spark=mock_spark)

    def test_duplicate_target_fails_instead_of_choosing_a_row(self, mock_spark):
        setup_spark_excel(
            mock_spark,
            make_excel_bytes(
                [
                    _excel_row(),
                    _excel_row(source_database="archive", source_table="orders_old"),
                ]
            ),
        )

        with pytest.raises(ValueError, match="Multiple Excel rows target"):
            m.parse_metadata_excel.function("s3a://config/mapping.xlsx", run_id="run-123", spark=mock_spark)

    def test_unsafe_identifiers_are_rejected(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([_excel_row(source_table="orders; DROP TABLE x")]))

        with pytest.raises(ValueError, match="Invalid source_table"):
            m.parse_metadata_excel.function("s3a://config/mapping.xlsx", run_id="run-123", spark=mock_spark)


class TestCreateMetadataMigrationRun:
    def test_persists_run_configuration(self, mock_spark):
        with (
            patch.object(
                m,
                "get_config",
                return_value={
                    "tracking_database": "migration_tracking",
                    "cluster_type": "HDP",
                },
            ),
            patch.object(m, "execute_with_iceberg_retry") as execute,
        ):
            m.create_metadata_migration_run.function(
                "s3a://config/mapping.xlsx",
                "airflow-run-1",
                spark=mock_spark,
            )

        insert_sql = execute.call_args.args[1]
        assert '"cluster_type": "HDP"' in insert_sql


class TestRewriteCreateTableDdl:
    def test_rewrites_table_name_and_location_only(self):
        source_ddl = """CREATE EXTERNAL TABLE `sales`.`orders` (
  `id` bigint,
  `amount` decimal(12, 2)
)
PARTITIONED BY (`day` string)
STORED AS PARQUET
LOCATION 'hdfs://source/warehouse/sales/orders'
TBLPROPERTIES ('owner'='analytics')"""

        rewritten = m._rewrite_create_table_ddl(source_ddl, _excel_row())

        assert "CREATE EXTERNAL TABLE `sales_target`.`orders`" in rewritten
        assert "LOCATION 's3a://warehouse/finance/orders'" in rewritten
        assert "`id` bigint" in rewritten
        assert "`day` string" in rewritten
        assert "'owner'='analytics'" in rewritten
        assert "hdfs://source/warehouse/sales/orders" not in rewritten

    def test_requires_one_source_location(self):
        with pytest.raises(ValueError, match="exactly one LOCATION"):
            m._rewrite_create_table_ddl("CREATE TABLE sales.orders (id bigint)", _excel_row())

    def test_ignores_location_text_inside_properties(self):
        ddl = (
            "CREATE TABLE sales.orders (id bigint) STORED AS PARQUET "
            "LOCATION 'hdfs://source/orders' "
            "TBLPROPERTIES ('note'='LOCATION ''do not rewrite''')"
        )

        rewritten = m._rewrite_create_table_ddl(ddl, _excel_row())

        assert "LOCATION 's3a://warehouse/finance/orders'" in rewritten
        assert "LOCATION ''do not rewrite''" in rewritten


class TestSourceDefinitionExtraction:
    def test_extracts_to_edge_artifact_without_returning_ddl(self):
        stdout = MagicMock()
        stdout.read.return_value = b""
        stdout.channel.recv_exit_status.return_value = 0
        stderr = MagicMock()
        stderr.read.return_value = b""
        client = MagicMock()
        client.exec_command.return_value = (MagicMock(), stdout, stderr)
        script_file = MagicMock()
        sftp = MagicMock()
        sftp.file.return_value.__enter__.return_value = script_file
        client.open_sftp.return_value = sftp
        ssh = MagicMock()
        ssh.get_conn.return_value.__enter__.return_value = client

        with (
            patch.object(m, "SSHHook", return_value=ssh),
            patch.object(
                m,
                "get_config",
                return_value={
                    "ssh_conn_id": "cluster_edge_ssh",
                    "cluster_type": "HDP",
                    "hive_scratch_dir": "/tmp/hive",
                },
            ),
        ):
            result = m.extract_source_table_definition.function(
                {**_excel_row(), "run_id": "run-123"},
                {"temp_dir": "/tmp/migration/test-run"},
            )

        assert result["status"] == "EXTRACTED"
        assert result["remote_metadata_path"].endswith("metadata_sales_orders.json")
        assert "source_definition" not in result
        remote_script = script_file.write.call_args.args[0]
        compile(remote_script, "extract_metadata.py", "exec")
        assert "SHOW CREATE TABLE" in remote_script
        assert "source_schema_json" in remote_script


class TestTargetDataPresence:
    class FileIterator:
        def __init__(self, names):
            self.names = iter(names)

        def hasNext(self):
            if not hasattr(self, "next_name"):
                self.next_name = next(self.names, None)
            return self.next_name is not None

        def next(self):
            name = self.next_name
            del self.next_name
            return SimpleNamespace(getPath=lambda: SimpleNamespace(getName=lambda: name))

    def test_recursively_skips_marker_files_and_finds_data(self):
        fs = MagicMock()
        fs.listFiles.return_value = self.FileIterator(["_SUCCESS", ".metadata", "part-00000.parquet"])
        path = object()

        assert m._contains_data_file(fs, path) is True
        fs.listFiles.assert_called_once_with(path, True)

    def test_empty_location_has_no_data_files(self):
        fs = MagicMock()
        fs.listFiles.return_value = self.FileIterator(["_SUCCESS", ".metadata"])

        assert m._contains_data_file(fs, object()) is False


class TestRecordMetadataDiscovery:
    def test_persists_retrieved_definition_and_schema(self):
        table_ref = {**_excel_row(), "run_id": "run-123"}
        metadata = {
            "status": "EXTRACTED",
            "object_type": "TABLE",
            "source_location": "hdfs://source/orders",
            "source_definition": "CREATE EXTERNAL TABLE sales.orders (...) LOCATION 'hdfs://source/orders'",
            "source_schema_json": '[{"name": "id", "type": "bigint"}]',
            "partition_columns_json": '["day"]',
        }
        metadata_stream = MagicMock()
        metadata_stream.read.return_value = json.dumps(metadata).encode()
        sftp = MagicMock()
        sftp.file.return_value.__enter__.return_value = metadata_stream
        stdout = MagicMock()
        stdout.channel.recv_exit_status.return_value = 0
        client = MagicMock()
        client.open_sftp.return_value = sftp
        client.exec_command.return_value = (MagicMock(), stdout, MagicMock())
        ssh = MagicMock()
        ssh.get_conn.return_value.__enter__.return_value = client

        with (
            patch.object(
                m,
                "get_config",
                return_value={
                    "ssh_conn_id": "cluster_edge_ssh",
                    "cluster_type": "HDP",
                    "tracking_database": "migration_tracking",
                },
            ),
            patch.object(m, "SSHHook", return_value=ssh),
            patch.object(m, "execute_with_iceberg_retry") as execute,
        ):
            result = m.record_metadata_discovery.function(
                {
                    **table_ref,
                    "status": "EXTRACTED",
                    "remote_metadata_path": "/tmp/metadata.json",
                    "discovery_duration_seconds": 2.5,
                },
                {"temp_dir": "/tmp"},
                spark=MagicMock(),
            )

        assert result["status"] == "EXTRACTED"
        insert_sql = execute.call_args_list[-1].args[1]
        assert "CREATE EXTERNAL TABLE sales.orders" in insert_sql
        assert '[{"name": "id", "type": "bigint"}]' in insert_sql
        assert "source_definition" in insert_sql


class TestCreateTargetTableMetadata:
    def _setup_creation(self, mock_spark):
        mock_spark.catalog.tableExists.return_value = False
        mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value.exists.return_value = True
        source_row = MagicMock()
        source_row.__getitem__.return_value = (
            "CREATE EXTERNAL TABLE sales.orders (id bigint) STORED AS PARQUET LOCATION 'hdfs://source/orders'"
        )
        mock_spark.sql.return_value.collect.return_value = [source_row]
        return source_row

    def test_creates_metadata_using_persisted_ddl(self, mock_spark):
        self._setup_creation(mock_spark)
        table_ref = {
            **_excel_row(),
            "run_id": "run-123",
            "status": "EXTRACTED",
        }

        with (
            patch.object(
                m,
                "get_config",
                return_value={"tracking_database": "migration_tracking"},
            ),
            patch.object(m, "execute_with_iceberg_retry"),
            patch.object(m, "_contains_data_file", return_value=True),
        ):
            result = m.create_target_table_metadata.function(table_ref, spark=mock_spark)

        assert result["status"] == "CREATED"
        assert any(
            "CREATE EXTERNAL TABLE `sales_target`.`orders`" in str(call)
            and "LOCATION 's3a://warehouse/finance/orders'" in str(call)
            for call in mock_spark.sql.call_args_list
        )

    def test_does_not_overwrite_existing_object_at_other_location(self, mock_spark):
        mock_spark.catalog.tableExists.return_value = True
        location_row = MagicMock()
        location_row.__getitem__.side_effect = lambda key: {
            "col_name": "Location",
            "data_type": "s3a://warehouse/other/orders",
        }[key]
        mock_spark.sql.return_value.collect.return_value = [location_row]
        with (
            patch.object(m, "get_config", return_value={"tracking_database": "migration_tracking"}),
            patch.object(m, "execute_with_iceberg_retry"),
            patch.object(m, "_contains_data_file", return_value=True),
        ):
            result = m.create_target_table_metadata.function(
                {**_excel_row(), "run_id": "run-123", "status": "EXTRACTED"},
                spark=mock_spark,
            )

        assert result["status"] == "CONFLICT"
        assert not any("CREATE EXTERNAL TABLE" in str(call) for call in mock_spark.sql.call_args_list)

    def test_skips_a_source_view(self, mock_spark):
        with (
            patch.object(
                m,
                "get_config",
                return_value={"tracking_database": "migration_tracking"},
            ),
            patch.object(m, "execute_with_iceberg_retry"),
        ):
            result = m.create_target_table_metadata.function(
                {
                    **_excel_row(),
                    "run_id": "run-123",
                    "status": "SKIPPED",
                    "message": "SOURCE_IS_VIEW",
                },
                spark=mock_spark,
            )

        assert result["status"] == "SKIPPED"
        assert "SOURCE_IS_VIEW" in result["message"]

    def test_missing_target_data_path_fails_without_creating_table(self, mock_spark):
        self._setup_creation(mock_spark)
        mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value.exists.return_value = False
        with (
            patch.object(m, "get_config", return_value={"tracking_database": "migration_tracking"}),
            patch.object(m, "execute_with_iceberg_retry"),
        ):
            result = m.create_target_table_metadata.function(
                {**_excel_row(), "run_id": "run-123", "status": "EXTRACTED"},
                spark=mock_spark,
            )

        assert result["status"] == "FAILED"
        assert "Target data location does not exist" in result["message"]
        assert not any("CREATE EXTERNAL TABLE" in str(call) for call in mock_spark.sql.call_args_list)

    def test_empty_location_fails_by_default(self, mock_spark):
        self._setup_creation(mock_spark)
        with (
            patch.object(m, "get_config", return_value={"tracking_database": "migration_tracking"}),
            patch.object(m, "execute_with_iceberg_retry"),
            patch.object(m, "_contains_data_file", return_value=False),
        ):
            result = m.create_target_table_metadata.function(
                {**_excel_row(), "run_id": "run-123", "status": "EXTRACTED"},
                spark=mock_spark,
            )

        assert result["status"] == "FAILED"
        assert result["data_presence_status"] == "EMPTY_REJECTED"
        assert "No visible data files found" in result["message"]
        assert not any("CREATE EXTERNAL TABLE" in str(call) for call in mock_spark.sql.call_args_list)

    def test_explicitly_allows_empty_table_location(self, mock_spark):
        self._setup_creation(mock_spark)
        with (
            patch.object(m, "get_config", return_value={"tracking_database": "migration_tracking"}),
            patch.object(m, "execute_with_iceberg_retry"),
            patch.object(m, "_contains_data_file", return_value=False),
        ):
            result = m.create_target_table_metadata.function(
                {
                    **_excel_row(allow_empty=True),
                    "run_id": "run-123",
                    "status": "EXTRACTED",
                },
                spark=mock_spark,
            )

        assert result["status"] == "CREATED"
        assert result["data_presence_status"] == "EMPTY_ALLOWED"


class TestTargetMetadataValidation:
    def test_compares_schema_and_location(self, mock_spark):
        source_row = MagicMock()
        source_row.__getitem__.return_value = '[{"name": "id", "type": "bigint"}]'
        mock_spark.sql.return_value.collect.return_value = [source_row]
        mock_spark.table.return_value.schema.fields = [
            SimpleNamespace(name="id", dataType=SimpleNamespace(simpleString=lambda: "bigint"))
        ]
        location_row = MagicMock()
        location_row.__getitem__.side_effect = lambda key: {
            "col_name": "Location",
            "data_type": "s3://warehouse/finance/orders/",
        }[key]
        mock_spark.sql.side_effect = lambda query: (
            SimpleNamespace(collect=lambda: [source_row])
            if "source_schema_json" in query
            else SimpleNamespace(collect=lambda: [location_row])
        )
        table_result = {
            **_excel_row(),
            "run_id": "run-123",
            "source": "sales.orders",
            "target": "sales_target.orders",
            "status": "CREATED",
        }

        with (
            patch.object(
                m,
                "get_config",
                return_value={"tracking_database": "migration_tracking"},
            ),
            patch.object(m, "execute_with_iceberg_retry"),
        ):
            result = m.validate_target_table_metadata.function(table_result, spark=mock_spark)

        assert result["validation_status"] == "VALIDATED"
        assert result["schema_match"] is True
        assert result["schema_differences"] == []


class TestMetadataMigrationReport:
    def test_writes_escaped_run_and_object_details(self, mock_spark):
        run = {
            "status": "COMPLETED",
            "config_json": "{}",
            "dag_run_id": "scheduled__2026-10-20",
            "total_objects": 1,
            "created_objects": 1,
            "existing_objects": 0,
            "skipped_objects": 0,
            "failed_objects": 0,
        }
        item = {
            "source_database": "sales",
            "source_object": "<orders>",
            "target_database": "sales_target",
            "target_object": "orders",
            "object_type": "TABLE",
            "target_location": "s3a://warehouse/orders",
            "data_presence_status": "DATA_FOUND",
            "allow_empty": False,
            "overall_status": "VALIDATED",
            "validation_status": "VALIDATED",
            "error_message": None,
        }
        mock_spark.sql.side_effect = [
            SimpleNamespace(collect=lambda: [run]),
            SimpleNamespace(collect=lambda: [item]),
        ]
        output_stream = MagicMock()
        mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value.create.return_value = output_stream

        with patch.object(
            m,
            "get_config",
            return_value={
                "tracking_database": "migration_tracking",
                "report_output_location": "s3a://reports/hive",
            },
        ):
            result = m.generate_metadata_migration_report.function("run-123", spark=mock_spark)

        assert result["report_path"] == "s3a://reports/hive/run-123_hive_metadata_report.html"
        report_html = output_stream.write.call_args.args[0].decode()
        assert "<title>Hive Metadata Migration Report - run-123</title>" in report_html
        assert "<h1>Hive Metadata Migration Report</h1>" in report_html
        assert "Migration Summary" in report_html
        assert "Created &amp; validated" in report_html
        assert 'class="badge success">VALIDATED</span>' in report_html
        assert "&lt;orders&gt;" in report_html
        assert "COMPLETED" in report_html


class TestFinalizeMetadataMigrationRun:
    def test_aggregates_created_existing_skipped_and_failed(self, mock_spark):
        rows = [
            {"overall_status": "VALIDATED", "metadata_creation_status": "CREATED", "validation_status": "VALIDATED"},
            {
                "overall_status": "VALIDATED",
                "metadata_creation_status": "ALREADY_EXISTS",
                "validation_status": "VALIDATED",
            },
            {"overall_status": "SKIPPED", "metadata_creation_status": None, "validation_status": "NOT_RUN"},
            {"overall_status": "FAILED", "metadata_creation_status": None, "validation_status": "NOT_RUN"},
        ]

        class Row(dict):
            def __getitem__(self, key):
                return super().__getitem__(key)

        mock_spark.sql.side_effect = [
            SimpleNamespace(collect=lambda: [Row({"expected_objects": 4})]),
            SimpleNamespace(collect=lambda: [Row(item) for item in rows]),
        ]
        with (
            patch.object(
                m,
                "get_config",
                return_value={"tracking_database": "migration_tracking"},
            ),
            patch.object(m, "execute_with_iceberg_retry"),
        ):
            result = m.finalize_metadata_migration_run.function("run-123", spark=mock_spark)

        assert result == {
            "run_id": "run-123",
            "status": "COMPLETED_WITH_FAILURES",
            "total": 4,
            "created": 1,
            "already_exists": 1,
            "skipped": 1,
            "failed": 1,
        }
