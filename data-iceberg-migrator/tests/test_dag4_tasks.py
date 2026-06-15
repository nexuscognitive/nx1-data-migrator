"""DAG 4 Task Tests: parquet_hms_registration pipeline."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import migration_dag_parquet_hms as m
import pytest

from .helpers import make_excel_bytes, setup_spark_excel


def _make_field(name, type_str):
    """Build a mock Spark StructField. NOTE: MagicMock(name=...) sets the mock's
    own name, so .name must be assigned after construction."""
    f = MagicMock()
    f.name = name
    f.dataType.simpleString.return_value = type_str
    return f


def _make_status(name, is_dir, full_path=""):
    """Build a mock Hadoop FileStatus for partition-dir discovery."""
    st = MagicMock()
    st.isDirectory.return_value = is_dir
    p = MagicMock()
    p.getName.return_value = name
    p.toString.return_value = full_path or f"s3a://test-bucket/x/{name}"
    st.getPath.return_value = p
    return st


class TestInitHmsTrackingTables:
    def test_creates_database_and_tables(self, mock_spark):
        result = m.init_hms_tracking_tables.function(spark=mock_spark)
        assert result == {"status": "initialized", "database": "migration_tracking"}
        sql = " ".join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert "hms_registration_runs" in sql
        assert "hms_registration_status" in sql


class TestCreateHmsRegistrationRun:
    def test_creates_run_with_running_status(self, mock_spark):
        run_id = m.create_hms_registration_run.function(
            excel_file_path="s3a://bucket/hms.xlsx",
            dag_run_id="dag_test",
            spark=mock_spark,
        )
        assert run_id.startswith("hms_reg_")
        assert "RUNNING" in " ".join(str(c) for c in mock_spark.sql.call_args_list)


class TestParseParquetHmsExcel:
    def test_parses_rows(self, mock_spark, sample_hms_run_id):
        setup_spark_excel(
            mock_spark,
            make_excel_bytes(
                [
                    {
                        "database": "sales_data",
                        "table": "transactions",
                        "s3_location": "s3a://b/sales_data/transactions",
                    },
                    {"database": "sales_data", "table": "orders", "s3_location": "s3a://b/sales_data/orders"},
                ]
            ),
        )
        result = m.parse_parquet_hms_excel.function("s3a://b/f.xlsx", sample_hms_run_id, spark=mock_spark)
        assert len(result) == 2
        assert result[0] == {
            "run_id": sample_hms_run_id,
            "database": "sales_data",
            "table": "transactions",
            "s3_location": "s3a://b/sales_data/transactions",
        }

    def test_normalizes_s3_prefix_and_trailing_slash(self, mock_spark, sample_hms_run_id):
        setup_spark_excel(
            mock_spark,
            make_excel_bytes(
                [
                    {"database": "db1", "table": "t1", "s3_location": "s3://b/db1/t1/"},
                ]
            ),
        )
        result = m.parse_parquet_hms_excel.function("s3a://b/f.xlsx", sample_hms_run_id, spark=mock_spark)
        assert result[0]["s3_location"] == "s3a://b/db1/t1"

    def test_normalizes_column_headers(self, mock_spark, sample_hms_run_id):
        setup_spark_excel(
            mock_spark,
            make_excel_bytes(
                [
                    {"Database": "db1", " Table ": "t1", "S3 Location": "s3a://b/db1/t1"},
                ]
            ),
        )
        result = m.parse_parquet_hms_excel.function("s3a://b/f.xlsx", sample_hms_run_id, spark=mock_spark)
        assert len(result) == 1
        assert result[0]["database"] == "db1"

    def test_skips_rows_with_missing_required_cells(self, mock_spark, sample_hms_run_id):
        setup_spark_excel(
            mock_spark,
            make_excel_bytes(
                [
                    {"database": "db1", "table": "", "s3_location": "s3a://b/x"},
                    {"database": "", "table": "t1", "s3_location": "s3a://b/x"},
                    {"database": "db1", "table": "t1", "s3_location": ""},
                    {"database": "db1", "table": "t_ok", "s3_location": "s3a://b/db1/t_ok"},
                ]
            ),
        )
        result = m.parse_parquet_hms_excel.function("s3a://b/f.xlsx", sample_hms_run_id, spark=mock_spark)
        assert len(result) == 1
        assert result[0]["table"] == "t_ok"

    def test_skips_rows_with_invalid_identifiers(self, mock_spark, sample_hms_run_id):
        setup_spark_excel(
            mock_spark,
            make_excel_bytes(
                [
                    {"database": "db1; DROP TABLE x", "table": "t1", "s3_location": "s3a://b/x"},
                    {"database": "db1", "table": "t1'", "s3_location": "s3a://b/x"},
                    {"database": "db one", "table": "t1", "s3_location": "s3a://b/x"},
                    {"database": "db1", "table": "t_ok", "s3_location": "s3a://b/db1/t_ok"},
                ]
            ),
        )
        result = m.parse_parquet_hms_excel.function("s3a://b/f.xlsx", sample_hms_run_id, spark=mock_spark)
        assert len(result) == 1
        assert result[0]["table"] == "t_ok"

    def test_skips_s3_location_with_quote(self, mock_spark, sample_hms_run_id):
        setup_spark_excel(
            mock_spark,
            make_excel_bytes(
                [
                    {"database": "db1", "table": "t1", "s3_location": "s3a://b/it's-bad"},
                ]
            ),
        )
        result = m.parse_parquet_hms_excel.function("s3a://b/f.xlsx", sample_hms_run_id, spark=mock_spark)
        assert result == []

    def test_duplicate_rows_first_wins(self, mock_spark, sample_hms_run_id):
        setup_spark_excel(
            mock_spark,
            make_excel_bytes(
                [
                    {"database": "db1", "table": "t1", "s3_location": "s3a://b/first"},
                    {"database": "db1", "table": "t1", "s3_location": "s3a://b/second"},
                ]
            ),
        )
        result = m.parse_parquet_hms_excel.function("s3a://b/f.xlsx", sample_hms_run_id, spark=mock_spark)
        assert len(result) == 1
        assert result[0]["s3_location"] == "s3a://b/first"


class TestDiscoverPartitionColumns:
    def test_finds_nested_partition_columns(self, mock_spark):
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        fs_mock.listStatus.side_effect = [
            [_make_status("dt=2024-01-01", True, "s3a://b/t/dt=2024-01-01")],
            [_make_status("region=eu", True, "s3a://b/t/dt=2024-01-01/region=eu")],
            [_make_status("part-00000.parquet", False)],
        ]
        cols = m._discover_partition_columns(mock_spark, "s3a://b/t")
        assert cols == ["dt", "region"]

    def test_unpartitioned_layout_returns_empty(self, mock_spark):
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        fs_mock.listStatus.side_effect = [
            [_make_status("part-00000.parquet", False), _make_status("part-00001.parquet", False)],
        ]
        assert m._discover_partition_columns(mock_spark, "s3a://b/t") == []

    def test_ignores_hidden_and_non_kv_directories(self, mock_spark):
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        fs_mock.listStatus.side_effect = [
            [_make_status(".spark-staging", True), _make_status("data", True)],
        ]
        assert m._discover_partition_columns(mock_spark, "s3a://b/t") == []


class TestCountLeafPartitionDirs:
    def test_counts_nested_leaves_including_empty(self, mock_spark):
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        # depth 2: root has two dt dirs; first has two region dirs, second has
        # one region dir that is empty — empty leaves still count
        fs_mock.listStatus.side_effect = [
            [
                _make_status("dt=2024-01-01", True, "s3a://b/t/dt=2024-01-01"),
                _make_status("dt=2024-01-02", True, "s3a://b/t/dt=2024-01-02"),
                _make_status("_SUCCESS", False),
            ],
            [
                _make_status("region=eu", True, "s3a://b/t/dt=2024-01-01/region=eu"),
                _make_status("region=us", True, "s3a://b/t/dt=2024-01-01/region=us"),
            ],
            [
                _make_status("region=eu", True, "s3a://b/t/dt=2024-01-02/region=eu"),
            ],
        ]
        assert m._count_leaf_partition_dirs(mock_spark, "s3a://b/t", 2) == 3

    def test_ignores_hidden_and_non_kv_directories(self, mock_spark):
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        fs_mock.listStatus.side_effect = [
            [
                _make_status("dt=2024-01-01", True, "s3a://b/t/dt=2024-01-01"),
                _make_status(".spark-staging", True),
                _make_status("not_a_partition", True),
            ],
        ]
        assert m._count_leaf_partition_dirs(mock_spark, "s3a://b/t", 1) == 1


class TestRegisterParquetTables:
    def _setup_parquet_read(self, mock_spark, fields):
        df = MagicMock()
        df.schema.fields = fields
        mock_spark.read.option.return_value.parquet.return_value = df
        return df

    def _setup_fs_listing(self, mock_spark, listings):
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        fs_mock.listStatus.side_effect = listings

    def test_partitioned_table_ddl_and_msck(self, mock_spark, sample_hms_table_config, mock_iceberg_retry):
        mock_spark.catalog.tableExists.return_value = False
        self._setup_parquet_read(
            mock_spark,
            [
                _make_field("id", "bigint"),
                _make_field("amount", "double"),
                _make_field("dt", "string"),
            ],
        )
        self._setup_fs_listing(
            mock_spark,
            [
                [_make_status("dt=2024-01-01", True, "s3a://b/t/dt=2024-01-01")],
                [_make_status("part-00000.parquet", False)],
            ],
        )
        result = m.register_parquet_tables.function.__wrapped__(
            table_config=sample_hms_table_config,
            dag_run_id="dag_test",
            spark=mock_spark,
            ti=MagicMock(),
        )
        assert result["status"] == "REGISTERED"
        assert result["partition_columns"] == ["dt"]
        all_sql = " ".join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert "create external table" in all_sql
        assert "partitioned by" in all_sql
        assert "stored as parquet" in all_sql
        assert "msck repair table" in all_sql
        # partition column must be in PARTITIONED BY, not in the data column list
        create_sql = next(str(c) for c in mock_spark.sql.call_args_list if "external table" in str(c).lower())
        before_partitioned_by = create_sql.lower().split("partitioned by")[0]
        assert "`dt`" not in before_partitioned_by

    def test_unpartitioned_table_no_partition_clause_no_msck(
        self, mock_spark, sample_hms_table_config, mock_iceberg_retry
    ):
        mock_spark.catalog.tableExists.return_value = False
        self._setup_parquet_read(
            mock_spark,
            [
                _make_field("id", "bigint"),
                _make_field("amount", "double"),
            ],
        )
        self._setup_fs_listing(
            mock_spark,
            [
                [_make_status("part-00000.parquet", False)],
            ],
        )
        result = m.register_parquet_tables.function.__wrapped__(
            table_config=sample_hms_table_config,
            dag_run_id="dag_test",
            spark=mock_spark,
            ti=MagicMock(),
        )
        assert result["status"] == "REGISTERED"
        assert result["partition_columns"] == []
        all_sql = " ".join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert "partitioned by" not in all_sql
        assert "msck" not in all_sql

    def _setup_existing_table(self, mock_spark, location):
        def router(sql):
            df = MagicMock()
            if "describe formatted" in sql.lower():
                df.collect.return_value = [
                    {"col_name": "# Detailed Table Information", "data_type": ""},
                    {"col_name": "Location", "data_type": location},
                ]
            else:
                df.collect.return_value = []
            return df

        mock_spark.catalog.tableExists.return_value = True
        mock_spark.sql.side_effect = router

    def test_existing_table_at_different_location_is_skipped(
        self, mock_spark, sample_hms_table_config, mock_iceberg_retry
    ):
        self._setup_existing_table(mock_spark, "s3a://other-bucket/elsewhere")
        result = m.register_parquet_tables.function.__wrapped__(
            table_config=sample_hms_table_config,
            dag_run_id="dag_test",
            spark=mock_spark,
            ti=MagicMock(),
        )
        assert result["status"] == "SKIPPED"
        mock_spark.read.option.assert_not_called()
        all_sql = " ".join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert "msck" not in all_sql
        assert any("SKIPPED" in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_existing_table_at_same_location_repairs_and_registers(
        self, mock_spark, sample_hms_table_config, mock_iceberg_retry
    ):
        # Airflow retry scenario: a prior attempt created the table but died
        # before MSCK — the retry must repair partitions and report success,
        # not overwrite the FAILED tracking row with SKIPPED.
        self._setup_existing_table(mock_spark, sample_hms_table_config["s3_location"])
        self._setup_fs_listing(
            mock_spark,
            [
                [_make_status("dt=2024-01-01", True, "s3a://b/t/dt=2024-01-01")],
                [_make_status("part-00000.parquet", False)],
            ],
        )
        result = m.register_parquet_tables.function.__wrapped__(
            table_config=sample_hms_table_config,
            dag_run_id="dag_test",
            spark=mock_spark,
            ti=MagicMock(),
        )
        assert result["status"] == "REGISTERED"
        assert result["partition_columns"] == ["dt"]
        all_sql = " ".join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert "msck repair table" in all_sql
        assert "create external table" not in all_sql
        assert any("REGISTERED" in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_existing_table_same_location_trailing_slash_and_s3_prefix_match(
        self, mock_spark, sample_hms_table_config, mock_iceberg_retry
    ):
        self._setup_existing_table(mock_spark, sample_hms_table_config["s3_location"].replace("s3a://", "s3://") + "/")
        self._setup_fs_listing(
            mock_spark,
            [
                [_make_status("part-00000.parquet", False)],
            ],
        )
        result = m.register_parquet_tables.function.__wrapped__(
            table_config=sample_hms_table_config,
            dag_run_id="dag_test",
            spark=mock_spark,
            ti=MagicMock(),
        )
        assert result["status"] == "REGISTERED"

    def test_registration_failure_records_failed_and_raises(
        self, mock_spark, sample_hms_table_config, mock_iceberg_retry
    ):
        mock_spark.catalog.tableExists.return_value = False
        mock_spark.read.option.return_value.parquet.side_effect = Exception("Path does not exist")
        with pytest.raises(Exception, match="HMS registration failed"):
            m.register_parquet_tables.function.__wrapped__(
                table_config=sample_hms_table_config,
                dag_run_id="dag_test",
                spark=mock_spark,
                ti=MagicMock(),
            )
        assert any("FAILED" in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_partition_dir_missing_from_schema_fails(self, mock_spark, sample_hms_table_config, mock_iceberg_retry):
        mock_spark.catalog.tableExists.return_value = False
        # directory layout says dt= but Spark schema has no dt column
        self._setup_parquet_read(mock_spark, [_make_field("id", "bigint")])
        self._setup_fs_listing(
            mock_spark,
            [
                [_make_status("dt=2024-01-01", True, "s3a://b/t/dt=2024-01-01")],
                [_make_status("part-00000.parquet", False)],
            ],
        )
        with pytest.raises(Exception, match="not in the schema inferred"):
            m.register_parquet_tables.function.__wrapped__(
                table_config=sample_hms_table_config,
                dag_run_id="dag_test",
                spark=mock_spark,
                ti=MagicMock(),
            )

    def test_tracking_insert_written_on_success(self, mock_spark, sample_hms_table_config, mock_iceberg_retry):
        mock_spark.catalog.tableExists.return_value = False
        self._setup_parquet_read(mock_spark, [_make_field("id", "bigint"), _make_field("dt", "string")])
        self._setup_fs_listing(
            mock_spark,
            [
                [_make_status("dt=2024-01-01", True, "s3a://b/t/dt=2024-01-01")],
                [_make_status("part-00000.parquet", False)],
            ],
        )
        m.register_parquet_tables.function.__wrapped__(
            table_config=sample_hms_table_config,
            dag_run_id="dag_test",
            spark=mock_spark,
            ti=MagicMock(),
        )
        retry_sql = " ".join(str(c) for c in mock_iceberg_retry.call_args_list)
        assert "hms_registration_status" in retry_sql
        assert "REGISTERED" in retry_sql

    def test_disables_type_inference_and_merges_schema(self, mock_spark, sample_hms_table_config, mock_iceberg_retry):
        """Partition columns must register as STRING (inference off) and the schema
        must be merged across all footers so evolved columns are not dropped."""
        mock_spark.catalog.tableExists.return_value = False
        self._setup_parquet_read(mock_spark, [_make_field("id", "bigint"), _make_field("dt", "string")])
        self._setup_fs_listing(
            mock_spark,
            [
                [_make_status("dt=2024-01-01", True, "s3a://b/t/dt=2024-01-01")],
                [_make_status("part-00000.parquet", False)],
            ],
        )
        m.register_parquet_tables.function.__wrapped__(
            table_config=sample_hms_table_config,
            dag_run_id="dag_test",
            spark=mock_spark,
            ti=MagicMock(),
        )
        mock_spark.conf.set.assert_any_call("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        mock_spark.read.option.assert_called_once_with("mergeSchema", "true")


class TestValidateRegisteredTables:
    def _setup_counts(self, mock_spark, hms_rows=1000, parquet_rows=1000, hms_partitions=2, s3_partitions=2):
        def router(sql):
            sl = sql.lower()
            df = MagicMock()
            if "count(*)" in sl:
                row = MagicMock()
                row.__getitem__ = lambda self, k: hms_rows
                df.collect.return_value = [row]
            elif "show partitions" in sl:
                df.count.return_value = hms_partitions
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router

        parquet_df = MagicMock()
        parquet_df.count.return_value = parquet_rows
        mock_spark.read.parquet.return_value = parquet_df

        # the fixture's partition_columns is ['dt'] (depth 1), so the S3-side
        # count is one listStatus call on the table root
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        fs_mock.listStatus.return_value = [
            _make_status(f"dt=2024-01-{i:02d}", True, f"s3a://b/t/dt=2024-01-{i:02d}")
            for i in range(1, s3_partitions + 1)
        ]

    def test_counts_match_validated(self, mock_spark, sample_hms_registration_result):
        self._setup_counts(mock_spark)
        result = m.validate_registered_tables.function.__wrapped__(
            registration_result=sample_hms_registration_result,
            spark=mock_spark,
            ti=MagicMock(),
        )
        assert result["validation_status"] == "COMPLETED"
        assert result["row_count_match"] is True
        assert result["partition_count_match"] is True

    def test_row_count_mismatch_raises(self, mock_spark, sample_hms_registration_result):
        self._setup_counts(mock_spark, hms_rows=900, parquet_rows=1000)
        with pytest.raises(Exception, match="Validation mismatches"):
            m.validate_registered_tables.function.__wrapped__(
                registration_result=sample_hms_registration_result,
                spark=mock_spark,
                ti=MagicMock(),
            )

    def test_partition_count_mismatch_raises(self, mock_spark, sample_hms_registration_result):
        self._setup_counts(mock_spark, hms_partitions=1, s3_partitions=2)
        with pytest.raises(Exception, match="Validation mismatches"):
            m.validate_registered_tables.function.__wrapped__(
                registration_result=sample_hms_registration_result,
                spark=mock_spark,
                ti=MagicMock(),
            )

    def test_empty_partition_dirs_count_on_both_sides(self, mock_spark, sample_hms_registration_result):
        """MSCK registers empty key=value dirs; the S3-side directory count must
        include them too, so an empty partition is not a false mismatch."""
        # 3 partition dirs on S3 (one empty), MSCK registered all 3 — rows live
        # in only 2 of them but row counts still agree overall
        self._setup_counts(mock_spark, hms_partitions=3, s3_partitions=3)
        result = m.validate_registered_tables.function.__wrapped__(
            registration_result=sample_hms_registration_result,
            spark=mock_spark,
            ti=MagicMock(),
        )
        assert result["validation_status"] == "COMPLETED"
        assert result["partition_count_match"] is True
        assert result["s3_partition_count"] == 3

    def test_unpartitioned_skips_partition_check(self, mock_spark, sample_hms_registration_result):
        sample_hms_registration_result["partition_columns"] = []
        self._setup_counts(mock_spark)
        result = m.validate_registered_tables.function.__wrapped__(
            registration_result=sample_hms_registration_result,
            spark=mock_spark,
            ti=MagicMock(),
        )
        assert result["validation_status"] == "COMPLETED"
        all_sql = " ".join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert "show partitions" not in all_sql

    def test_skipped_registration_not_validated(self, mock_spark, sample_hms_registration_result):
        sample_hms_registration_result["status"] = "SKIPPED"
        result = m.validate_registered_tables.function.__wrapped__(
            registration_result=sample_hms_registration_result,
            spark=mock_spark,
            ti=MagicMock(),
        )
        assert result["validation_status"] == "SKIPPED"
        mock_spark.read.parquet.assert_not_called()

    def test_invalid_input_returns_empty(self, mock_spark):
        result = m.validate_registered_tables.function.__wrapped__(
            registration_result=None,
            spark=mock_spark,
            ti=MagicMock(),
        )
        assert result == {}


class TestUpdateHmsValidationStatus:
    def _make_validation_result(self, sample, **overrides):
        return {
            **sample,
            "validation_status": "COMPLETED",
            "hms_row_count": 1000,
            "parquet_row_count": 1000,
            "row_count_match": True,
            "hms_partition_count": 2,
            "s3_partition_count": 2,
            "partition_count_match": True,
            "validation_error": None,
            "_task_duration": 4.5,
            **overrides,
        }

    def test_sets_validated_on_match(self, mock_spark, sample_hms_registration_result, mock_iceberg_retry):
        vr = self._make_validation_result(sample_hms_registration_result)
        m.update_hms_validation_status.function(validation_result=vr, spark=mock_spark)
        assert any("'VALIDATED'" in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_sets_validation_failed_on_mismatch(self, mock_spark, sample_hms_registration_result, mock_iceberg_retry):
        vr = self._make_validation_result(
            sample_hms_registration_result,
            row_count_match=False,
            validation_error="row count mismatch: hms=900 parquet=1000",
        )
        m.update_hms_validation_status.function(validation_result=vr, spark=mock_spark)
        assert any("VALIDATION_FAILED" in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_validation_error_status_failed(self, mock_spark, sample_hms_registration_result, mock_iceberg_retry):
        vr = self._make_validation_result(
            sample_hms_registration_result,
            validation_status="FAILED",
            validation_error="boom",
        )
        m.update_hms_validation_status.function(validation_result=vr, spark=mock_spark)
        assert any("VALIDATION_FAILED" in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_skipped_validation_no_update(self, mock_spark, sample_hms_registration_result, mock_iceberg_retry):
        vr = self._make_validation_result(sample_hms_registration_result, validation_status="SKIPPED")
        m.update_hms_validation_status.function(validation_result=vr, spark=mock_spark)
        assert not mock_iceberg_retry.called


class TestGenerateHmsHtmlReport:
    def _make_row(self, **overrides):
        base = dict(
            database_name="sales_data",
            table_name="transactions",
            s3_location="s3a://test-bucket/sales_data/transactions",
            partition_columns="dt",
            status="VALIDATED",
            registration_duration_seconds=12.5,
            validation_duration_seconds=4.5,
            validation_status="COMPLETED",
            hms_row_count=1000,
            parquet_row_count=1000,
            row_count_match=True,
            hms_partition_count=2,
            s3_partition_count=2,
            partition_count_match=True,
            error_message=None,
        )
        base.update(overrides)
        return SimpleNamespace(**base)

    def test_generates_report_and_writes_to_s3(self, mock_spark, sample_hms_run_id):
        df = MagicMock()
        df.collect.return_value = [self._make_row()]
        mock_spark.sql.return_value = df
        mock_spark.sql.side_effect = None
        result = m.generate_hms_html_report.function(run_id=sample_hms_run_id, spark=mock_spark)
        assert result["report_path"].endswith(".html")
        assert sample_hms_run_id in result["report_path"]
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        written = fs_mock.create.return_value.write.call_args[0][0].decode("utf-8")
        assert "transactions" in written
        assert "VALIDATED" in written

    def test_handles_failed_and_skipped_rows(self, mock_spark, sample_hms_run_id):
        df = MagicMock()
        df.collect.return_value = [
            self._make_row(
                status="FAILED",
                validation_status=None,
                error_message="boom",
                hms_row_count=None,
                parquet_row_count=None,
                row_count_match=None,
                hms_partition_count=None,
                s3_partition_count=None,
                partition_count_match=None,
            ),
            self._make_row(
                table_name="orders", status="SKIPPED", validation_status=None, error_message="already exists"
            ),
        ]
        mock_spark.sql.return_value = df
        mock_spark.sql.side_effect = None
        result = m.generate_hms_html_report.function(run_id=sample_hms_run_id, spark=mock_spark)
        assert result["report_path"].endswith(".html")


class TestSendHmsReportEmail:
    def test_skips_when_no_recipients(self, mock_spark, sample_hms_run_id):
        with patch("airflow.models.Variable.get", return_value=""):
            result = m.send_hms_report_email.function(
                report_result={"report_path": "s3a://bucket/report.html"},
                run_id=sample_hms_run_id,
                spark=mock_spark,
            )
        assert result["sent"] is False

    def test_sends_when_recipients_configured(self, mock_spark, sample_hms_run_id):
        reader_mock = MagicMock()
        reader_mock.readLine.side_effect = ["<html>hms report</html>", None]
        mock_spark._jvm.java.io.BufferedReader.return_value = reader_mock

        with (
            patch("airflow.utils.email.send_email"),
            patch("tempfile.NamedTemporaryFile") as mock_tmp,
            patch("os.unlink"),
        ):
            tmp_inst = MagicMock()
            tmp_inst.name = "/tmp/hms_report.html"
            mock_tmp.return_value = tmp_inst
            result = m.send_hms_report_email.function(
                report_result={"report_path": "s3a://bucket/report.html"},
                run_id=sample_hms_run_id,
                spark=mock_spark,
            )
        assert result["sent"] is True


class TestFinalizeHmsRun:
    def _router(self, total=2, successful=2, failed=0, skipped=0):
        stats = MagicMock()
        stats.__getitem__ = lambda self, k: {
            "total": total,
            "successful": successful,
            "failed": failed,
            "skipped": skipped,
        }[k]

        def router(sql):
            df = MagicMock()
            if "count(*)" in sql.lower():
                df.collect.return_value = [stats]
            else:
                df.collect.return_value = []
            return df

        return router

    def test_returns_completed(self, mock_spark, sample_hms_run_id, mock_iceberg_retry):
        mock_spark.sql.side_effect = self._router()
        result = m.finalize_hms_run.function(run_id=sample_hms_run_id, spark=mock_spark)
        assert result["status"] == "COMPLETED"
        assert result["total"] == 2

    def test_returns_completed_with_failures(self, mock_spark, sample_hms_run_id, mock_iceberg_retry):
        mock_spark.sql.side_effect = self._router(total=3, successful=2, failed=1)
        result = m.finalize_hms_run.function(run_id=sample_hms_run_id, spark=mock_spark)
        assert result["status"] == "COMPLETED_WITH_FAILURES"

    def test_no_records_is_failed(self, mock_spark, sample_hms_run_id, mock_iceberg_retry):
        mock_spark.sql.side_effect = self._router(total=0, successful=0, failed=0)
        result = m.finalize_hms_run.function(run_id=sample_hms_run_id, spark=mock_spark)
        assert result["status"] == "FAILED"
