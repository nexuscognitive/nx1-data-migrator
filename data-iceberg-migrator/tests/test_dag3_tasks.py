"""DAG 3 Task Tests: folder_only_data_copy pipeline."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import migration_dag_folder_copy as m
import pandas as pd
import pytest

from .helpers import make_excel_bytes, mock_ssh_stdout, setup_spark_excel

EXPECTED_SCHEMA_ADDITIONS = (
    ('data_copy_status', (
        ('yarn_application_id', 'STRING'),
        ('distcp_started_at', 'TIMESTAMP'),
        ('distcp_completed_at', 'TIMESTAMP'),
        ('distcp_duration_seconds', 'DOUBLE'),
        ('distcp_bytes_copied', 'BIGINT'),
        ('distcp_files_copied', 'BIGINT'),
        ('throughput_mbps', 'DOUBLE'),
    )),
    ('data_copy_runs', (
        ('dag_run_id', 'STRING'),
        ('service_account_user_id', 'STRING'),
        ('service_account_source', 'STRING'),
        ('skipped_folders', 'INT'),
    )),
)


class TestValidatePrerequisitesFolderCopy:

    def test_all_checks_pass(self, mock_ssh_hook):
        hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        responses = [
            (MagicMock(), mock_ssh_stdout(0, b'SSH_TEST_OK'), MagicMock()),
            (MagicMock(), mock_ssh_stdout(0, b'DISTCP_OK'), MagicMock()),
            (MagicMock(), mock_ssh_stdout(0, b'HADOOP_FS_OK'), MagicMock()),
        ]
        for r in responses:
            r[2].read.return_value = b''
        client.exec_command.side_effect = responses

        result = m.validate_prerequisites_folder_copy.function()
        assert result['ssh_connectivity'] is True
        assert result['hadoop_distcp_available'] is True
        assert result['hadoop_fs_available'] is True


class TestInitFolderCopyTrackingTables:

    def test_creates_database_and_tables(self, mock_spark):
        result = m.init_folder_copy_tracking_tables.function(spark=mock_spark)
        assert result == {'status': 'initialized', 'database': 'migration_tracking'}
        sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'data_copy_runs' in sql
        assert 'data_copy_status' in sql
        assert 'using iceberg' in sql

    def test_alter_table_issued_for_every_schema_addition(self, mock_spark):
        m.init_folder_copy_tracking_tables.function(spark=mock_spark)
        sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        for table, columns in EXPECTED_SCHEMA_ADDITIONS:
            for col, col_type in columns:
                assert f"ALTER TABLE migration_tracking.{table} ADD COLUMN {col} {col_type}" in sql

    def test_warns_once_per_table_when_describe_shows_missing_columns(self, mock_spark):
        with patch.object(m.logger, 'warning') as warn:
            m.init_folder_copy_tracking_tables.function(spark=mock_spark)
        messages = [str(c[0][0]) for c in warn.call_args_list]
        assert any('data_copy_status' in msg and 'missing' in msg for msg in messages)
        assert any('data_copy_runs' in msg and 'missing' in msg for msg in messages)
        assert any('throughput_mbps' in msg for msg in messages)

    def test_no_missing_column_warning_when_describe_lists_everything(self, mock_spark):
        expected = {c for _t, cols in EXPECTED_SCHEMA_ADDITIONS for c, _ in cols}

        def _sql(stmt, *a, **kw):
            df = MagicMock()
            if stmt.strip().startswith('DESCRIBE'):
                df.collect.return_value = [{'col_name': c} for c in expected]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = _sql
        with patch.object(m.logger, 'warning') as warn:
            m.init_folder_copy_tracking_tables.function(spark=mock_spark)
        assert not [c for c in warn.call_args_list if 'missing' in str(c[0][0])]

    def test_alter_table_already_exists_error_is_swallowed(self, mock_spark):
        def _sql(stmt, *a, **kw):
            if stmt.strip().startswith('ALTER TABLE'):
                raise Exception("AnalysisException: Cannot add column, name already exists")
            return MagicMock()

        mock_spark.sql.side_effect = _sql
        result = m.init_folder_copy_tracking_tables.function(spark=mock_spark)
        assert result == {'status': 'initialized', 'database': 'migration_tracking'}


class TestCreateDataCopyRun:

    def test_creates_run_with_running_status(self, mock_spark):
        run_id = m.create_data_copy_run.function(
            excel_file_path='s3a://bucket/fc.xlsx', spark=mock_spark,
        )
        assert run_id.startswith('folder_run_')
        assert 'RUNNING' in ' '.join(str(c) for c in mock_spark.sql.call_args_list)


class TestParseFolderCopyExcel:

    def test_basic_row_parsing(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/sales', 'target_bucket': 'my-bucket', 'dest_folder': 'sales_copy'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/fc.xlsx', sample_folder_run_id, spark=mock_spark)
        assert len(configs) == 1
        assert configs[0]['source_path'] == '/data/sales'
        assert configs[0]['dest_bucket'] == 's3a://my-bucket'
        assert configs[0]['dest_folder'] == 'sales_copy'

    @pytest.mark.parametrize("raw_bucket,expected", [
        ('s3://b', 's3a://b'),
        ('s3n://b', 's3a://b'),
        ('b', 's3a://b'),
        ('s3a://b', 's3a://b'),
    ])
    def test_normalizes_s3_bucket_prefix(self, mock_spark, sample_folder_run_id, raw_bucket, expected):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/x', 'target_bucket': raw_bucket, 'dest_folder': 'out'},
        ]))
        result = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert result[0]['dest_bucket'] == expected

    def test_skips_rows_with_missing_target_bucket(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '', 'target_bucket': '', 'dest_folder': ''},
        ]))
        fake_df = pd.DataFrame([
            {'source_path': '/data/no_bucket',   'target_bucket': None,        'dest_folder': 'x'},
            {'source_path': '/data/with_bucket', 'target_bucket': 's3a://bkt', 'dest_folder': 'y'},
        ])
        with patch('pandas.read_excel', return_value=fake_df):
            configs = m.parse_folder_copy_excel.function(
                's3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark,
            )
        assert len(configs) == 1
        assert configs[0]['source_path'] == '/data/with_bucket'

    def test_raises_when_no_valid_rows(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '', 'target_bucket': '', 'dest_folder': ''},
        ]))
        with pytest.raises(ValueError, match="No valid rows"):
            m.parse_folder_copy_excel.function(
                's3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark,
            )

    def test_multiple_rows_all_parsed(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/a', 'target_bucket': 's3a://bkt', 'dest_folder': 'a'},
            {'source_path': '/data/b', 'target_bucket': 's3a://bkt', 'dest_folder': 'b'},
            {'source_path': '/data/c', 'target_bucket': 's3a://bkt', 'dest_folder': 'c'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert len(configs) == 3

    def test_trailing_slash_stripped_from_source_path_basename(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/mydir/', 'target_bucket': 's3a://bkt', 'dest_folder': 'mydir'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert configs[0]['dest_folder'] == 'mydir'

    @pytest.mark.parametrize("empty_val", [None, float('nan')])
    def test_empty_dest_folder_defaults_to_source_basename(self, mock_spark, sample_folder_run_id, empty_val):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/mapr/cluster1/data/raw/sales', 'target_bucket': 's3a://bkt', 'dest_folder': empty_val},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert configs[0]['dest_folder'] == 'sales'

    def test_dest_folder_with_only_whitespace_defaults_to_source_basename(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/raw/reports/', 'target_bucket': 's3a://bkt', 'dest_folder': '   '},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert configs[0]['dest_folder'] == 'reports'

    def test_leading_slash_stripped_from_dest_folder(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/sales', 'target_bucket': 's3a://bkt', 'dest_folder': '/temp'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert configs[0]['dest_folder'] == 'temp'

    def test_endpoint_emitted_when_present(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/sales', 'target_bucket': 's3a://bkt', 'dest_folder': 'sales',
            'endpoint': 'https://s3.tenant-a.example.com'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert configs[0]['dest_endpoint'] == 'https://s3.tenant-a.example.com'

    def test_endpoint_defaults_to_empty_when_absent(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/sales', 'target_bucket': 's3a://bkt', 'dest_folder': 'sales'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert configs[0]['dest_endpoint'] == ''

    def test_each_row_independent_regardless_of_matching_bucket(self, mock_spark, sample_folder_run_id):
        """Two rows with same bucket but different endpoints must both appear as separate jobs."""
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/a', 'target_bucket': 's3a://data-lake',
            'endpoint': 'https://s3.tenant-a.example.com'},
            {'source_path': '/data/b', 'target_bucket': 's3a://data-lake',
            'endpoint': 'https://s3.tenant-b.example.com'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert len(configs) == 2
        endpoints = {c['dest_endpoint'] for c in configs}
        assert endpoints == {'https://s3.tenant-a.example.com', 'https://s3.tenant-b.example.com'}


# ===========================================================================
# run_folder_distcp_ssh
# ===========================================================================
class TestRunFolderDistcpSsh:

    def _success_output(self, incr=False, src_files=20, src_size=52428800,
                         s3_after_files=20, s3_after_size=52428800):
        return '\n'.join([
            f"INCREMENTAL={'true' if incr else 'false'}",
            f"SRC_FILE_COUNT={src_files}", f"SRC_TOTAL_SIZE={src_size}",
            "S3_FILE_COUNT_BEFORE=0", "S3_TOTAL_SIZE_BEFORE=0", "DISTCP_EXIT_CODE=0",
            f"S3_FILE_COUNT_AFTER={s3_after_files}", f"S3_TOTAL_SIZE_AFTER={s3_after_size}",
        ]).encode()

    def test_successful_copy(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._success_output(
            incr=True, src_files=42, src_size=1_000_000,
            s3_after_files=42, s3_after_size=1_000_000,
        )
        stdout_mock.channel.recv_exit_status.return_value = 0

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        assert result['status'] == 'COMPLETED'
        assert result['is_incremental'] is True
        assert result['source_file_count'] == 42
        assert result['file_count_match'] is True

    def test_file_count_mismatch(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._success_output(src_files=20, s3_after_files=18)
        stdout_mock.channel.recv_exit_status.return_value = 0

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        assert result['file_count_match'] is False

    def test_distcp_failure_returns_failed_without_raising(self, mock_ssh_hook, sample_folder_config):
        hook, client, _, _ = mock_ssh_hook
        fail_stderr = MagicMock()
        fail_stderr.read.return_value = b'DistCp failed'
        client.exec_command.return_value = (
            MagicMock(), mock_ssh_stdout(1, b'DISTCP_EXIT_CODE=1\n'), fail_stderr,
        )
        ti = MagicMock()

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config, ti=ti)
        assert result['status'] == 'FAILED'
        assert 'DistCp failed' in result['error']
        for key in ('distcp_duration_seconds', 'distcp_bytes_copied',
                    'distcp_files_copied', 'throughput_mbps'):
            assert key in result
        assert result['distcp_bytes_copied'] == 0
        assert result['distcp_files_copied'] == 0
        assert result['throughput_mbps'] == 0.0
        assert result['yarn_application_id'] is None

    def test_ssh_exception_returns_failed_without_raising(self, mock_ssh_hook, sample_folder_config):
        hook, _, _, _ = mock_ssh_hook
        hook.get_conn.side_effect = Exception("SSH timeout")
        ti = MagicMock()

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config, ti=ti)
        assert result['status'] == 'FAILED'
        assert 'SSH timeout' in result['error']
        assert result['yarn_application_ids'] == []

    def test_yarn_ids_extracted_from_stdout_and_stderr_deduped(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        stdout_mock.read.return_value = (
            b"Submitted application_1700000000000_0001\n"
            b"Retry: application_1700000000000_0001\n"
            b"Submitted application_1700000000000_0002\n"
            + self._success_output()
        )
        stdout_mock.channel.recv_exit_status.return_value = 0
        stderr_mock.read.return_value = b"tracking application_1700000000000_0003\n"

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        assert result['yarn_application_ids'] == [
            'application_1700000000000_0001',
            'application_1700000000000_0002',
            'application_1700000000000_0003',
        ]
        assert result['yarn_application_id'] == 'application_1700000000000_0003'

    def test_yarn_ids_empty_when_none_present(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._success_output()
        stdout_mock.channel.recv_exit_status.return_value = 0

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        assert result['yarn_application_ids'] == []
        assert result['yarn_application_id'] is None

    def test_metrics_block_parsed_and_malformed_value_defaults(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = '\n'.join([
            "noise before block",
            "===DISTCP_METRICS_START===",
            "INCREMENTAL=true",
            "SRC_FILE_COUNT=10",
            "SRC_TOTAL_SIZE=1048576",
            "S3_FILE_COUNT_BEFORE=0",
            "S3_TOTAL_SIZE_BEFORE=0",
            "S3_FILE_COUNT_AFTER=10",
            "S3_TOTAL_SIZE_AFTER=1048576",
            "BYTES_COPIED=not_a_number",
            "FILES_COPIED=10",
            "===DISTCP_METRICS_END===",
        ]).encode()
        stdout_mock.channel.recv_exit_status.return_value = 0

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        assert result['status'] == 'COMPLETED'
        assert result['is_incremental'] is True
        assert result['source_file_count'] == 10
        assert result['dest_size_bytes'] == 1048576
        assert result['distcp_bytes_copied'] == 0
        assert result['distcp_files_copied'] == 10

    def test_source_not_found(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = (
            b"SOURCE_EXISTS=false\nSOURCE_NOT_FOUND=true\n"
            b"===DISTCP_METRICS_START===\nINCREMENTAL=false\n===DISTCP_METRICS_END===\n"
        )
        stdout_mock.channel.recv_exit_status.return_value = 0

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        assert result['status'] == 'SOURCE_NOT_FOUND'
        assert 'does not exist' in result['error']
        assert result['source_file_count'] == 0
        assert result['distcp_bytes_copied'] == 0

    def test_empty_source(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = '\n'.join([
            "SOURCE_EXISTS=true",
            "EMPTY_SOURCE=true",
            "===DISTCP_METRICS_START===",
            "INCREMENTAL=false",
            "SRC_FILE_COUNT=0",
            "SRC_TOTAL_SIZE=0",
            "S3_FILE_COUNT_BEFORE=0",
            "S3_TOTAL_SIZE_BEFORE=0",
            "S3_FILE_COUNT_AFTER=0",
            "S3_TOTAL_SIZE_AFTER=0",
            "BYTES_COPIED=0",
            "FILES_COPIED=0",
            "===DISTCP_METRICS_END===",
        ]).encode()
        stdout_mock.channel.recv_exit_status.return_value = 0

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        assert result['status'] == 'EMPTY_SOURCE'
        assert result['error'] is None
        assert result['files_copied'] == 0

    def test_distcp_log_flag_uses_cluster_setup_log_dir(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._success_output()
        stdout_mock.channel.recv_exit_status.return_value = 0

        m.run_folder_distcp_ssh.function(
            folder_config=sample_folder_config,
            cluster_setup={'distcp_log_dir': '/tmp/logs/run1', 'temp_dir': '/tmp/run1'},
        )
        sent_cmd = client.exec_command.call_args[0][0]
        assert '-log /tmp/logs/run1/distcp_raw.log' in sent_cmd
        assert client.exec_command.call_args[1]['get_pty'] is True

    def test_delete_flag_absent_by_default(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._success_output()
        stdout_mock.channel.recv_exit_status.return_value = 0

        m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        assert ' -delete ' not in client.exec_command.call_args[0][0]

    def test_failed_distcp_retains_parsed_metrics(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        stdout_mock.read.return_value = '\n'.join([
            "===DISTCP_METRICS_START===",
            "INCREMENTAL=false",
            "SRC_FILE_COUNT=10",
            "SRC_TOTAL_SIZE=2097152",
            "S3_FILE_COUNT_BEFORE=0",
            "S3_TOTAL_SIZE_BEFORE=0",
            "S3_FILE_COUNT_AFTER=4",
            "S3_TOTAL_SIZE_AFTER=1048576",
            "BYTES_COPIED=1048576",
            "FILES_COPIED=4",
            "===DISTCP_METRICS_END===",
        ]).encode()
        stdout_mock.channel.recv_exit_status.return_value = 1
        stderr_mock.read.return_value = b'partial failure'

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        assert result['status'] == 'FAILED'
        assert result['distcp_bytes_copied'] == 1048576
        assert result['distcp_files_copied'] == 4
        assert result['source_file_count'] == 10
        assert result['dest_file_count'] == 4
        assert result['bytes_copied'] == 1048576

    def test_throughput_matches_bytes_over_duration(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = '\n'.join([
            "===DISTCP_METRICS_START===",
            "INCREMENTAL=false",
            "SRC_FILE_COUNT=4",
            "SRC_TOTAL_SIZE=4194304",
            "S3_FILE_COUNT_BEFORE=0",
            "S3_TOTAL_SIZE_BEFORE=0",
            "S3_FILE_COUNT_AFTER=4",
            "S3_TOTAL_SIZE_AFTER=4194304",
            "BYTES_COPIED=4194304",
            "FILES_COPIED=4",
            "===DISTCP_METRICS_END===",
        ]).encode()
        stdout_mock.channel.recv_exit_status.return_value = 0

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        duration = result['distcp_duration_seconds']
        expected = (4194304 / 1048576.0 / duration) if duration > 0 else 0.0
        assert result['throughput_mbps'] == pytest.approx(expected)

    def test_throughput_zero_when_no_bytes_copied(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = b"SOURCE_EXISTS=false\nSOURCE_NOT_FOUND=true\n"
        stdout_mock.channel.recv_exit_status.return_value = 0

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        assert result['throughput_mbps'] == 0.0


class TestRecordDataCopyStatus:

    def test_inserts_completed(self, mock_spark, sample_folder_distcp_result, mock_iceberg_retry):
        m.record_data_copy_status.function(distcp_result=sample_folder_distcp_result, spark=mock_spark)
        sql = mock_iceberg_retry.call_args[0][1]
        assert 'COMPLETED' in sql
        assert '/data/sales/raw' in sql

    def test_inserts_failed(self, mock_spark, sample_folder_distcp_result, mock_iceberg_retry):
        failed = {
            **sample_folder_distcp_result,
            'status': 'FAILED', 'error': 'exit 1',
            'source_path': '/bad', 'dest_path': 'x',
        }
        m.record_data_copy_status.function(distcp_result=failed, spark=mock_spark)
        assert 'FAILED' in mock_iceberg_retry.call_args[0][1]

    def test_persists_new_metric_columns(self, mock_spark, sample_folder_distcp_result, mock_iceberg_retry):
        enriched = {
            **sample_folder_distcp_result,
            'yarn_application_ids': ['application_1_1', 'application_1_2'],
            'yarn_application_id': 'application_1_2',
            'distcp_duration_seconds': 12.5,
            'distcp_bytes_copied': 1048576,
            'distcp_files_copied': 7,
            'throughput_mbps': 0.08,
        }
        m.record_data_copy_status.function(distcp_result=enriched, spark=mock_spark)
        sql = mock_iceberg_retry.call_args[0][1]
        assert 'yarn_application_id' in sql
        assert 'application_1_1,application_1_2' in sql
        assert '12.5' in sql
        assert 'throughput_mbps' in sql

    def test_returns_empty_dict_for_invalid_input(self, mock_spark, mock_iceberg_retry):
        assert m.record_data_copy_status.function(distcp_result=None, spark=mock_spark) == {}
        assert m.record_data_copy_status.function(distcp_result={'x': 1}, spark=mock_spark) == {}
        mock_iceberg_retry.assert_not_called()


class TestValidateDataCopy:

    def test_validated_when_counts_match(self, mock_ssh_hook, sample_folder_distcp_result):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = b"DEST_EXISTS=true\nDEST_FILE_COUNT=20\nDEST_TOTAL_SIZE=52428800\n"
        stdout_mock.channel.recv_exit_status.return_value = 0

        result = m.validate_data_copy.function(copy_status=sample_folder_distcp_result)
        assert result['validation_status'] == 'VALIDATED'
        assert result['file_count_match'] is True

    def test_failed_when_dest_missing(self, mock_ssh_hook, sample_folder_distcp_result):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = b"DEST_EXISTS=false\nDEST_FILE_COUNT=0\nDEST_TOTAL_SIZE=0\n"
        stdout_mock.channel.recv_exit_status.return_value = 0
        ti = MagicMock()

        result = m.validate_data_copy.function(copy_status=sample_folder_distcp_result, ti=ti)
        assert result['validation_status'] == 'VALIDATION_FAILED'
        assert ti.xcom_push.call_args[1]['value']['validation_status'] == 'VALIDATION_FAILED'

    def test_failed_on_file_count_mismatch(self, mock_ssh_hook, sample_folder_distcp_result):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = b"DEST_EXISTS=true\nDEST_FILE_COUNT=15\nDEST_TOTAL_SIZE=52428800\n"
        stdout_mock.channel.recv_exit_status.return_value = 0
        ti = MagicMock()

        result = m.validate_data_copy.function(copy_status=sample_folder_distcp_result, ti=ti)
        assert result['validation_status'] == 'VALIDATION_FAILED'
        assert result['file_count_match'] is False
        assert 'file count or size mismatch' in result['validation_error']

    @pytest.mark.parametrize("upstream,expected", [
        ('FAILED', 'VALIDATION_SKIPPED'),
        ('SOURCE_NOT_FOUND', 'SOURCE_NOT_FOUND'),
        ('EMPTY_SOURCE', 'EMPTY_SOURCE'),
        ('SKIPPED', 'SKIPPED'),
    ])
    def test_passthrough_statuses_do_not_raise(self, mock_ssh_hook, sample_folder_distcp_result,
                                               upstream, expected):
        copy_status = {**sample_folder_distcp_result, 'status': upstream}
        result = m.validate_data_copy.function(copy_status=copy_status, ti=MagicMock())
        assert result['validation_status'] == expected

    def test_returns_empty_dict_for_invalid_input(self):
        assert m.validate_data_copy.function(copy_status=None) == {}
        assert m.validate_data_copy.function(copy_status={'source_path': '/x'}) == {}


class TestUpdateDataCopyValidation:

    def test_updates_validated(self, mock_spark, sample_folder_validation_result, mock_iceberg_retry):
        m.update_data_copy_validation.function(
            validation_result=sample_folder_validation_result, spark=mock_spark,
        )
        assert 'VALIDATED' in mock_iceberg_retry.call_args[0][1]

    def test_updates_failed_with_error(self, mock_spark, sample_folder_distcp_result, mock_iceberg_retry):
        failed = {
            **sample_folder_distcp_result,
            'validation_status': 'VALIDATION_FAILED',
            'dest_file_count': 0, 'dest_size_bytes': 0,
            'file_count_match': False, 'size_match': False,
            'validation_error': 'Destination missing',
        }
        m.update_data_copy_validation.function(validation_result=failed, spark=mock_spark)
        sql = mock_iceberg_retry.call_args[0][1]
        assert 'VALIDATION_FAILED' in sql
        assert 'Destination missing' in sql

    def test_returns_empty_dict_for_invalid_input(self, mock_spark, mock_iceberg_retry):
        assert m.update_data_copy_validation.function(validation_result=None, spark=mock_spark) == {}
        mock_iceberg_retry.assert_not_called()


class TestFinalizeDataCopyRun:

    def _make_stats_row(self, total=2, successful=2, failed=0, skipped=0):
        row = MagicMock()
        row.__getitem__ = lambda self, k: {
            'total_folders': total, 'successful_folders': successful,
            'failed_folders': failed, 'skipped_folders': skipped,
        }[k]
        return row

    def test_completed_when_no_failures(self, mock_spark, sample_folder_run_id, mock_iceberg_retry):
        mock_spark.sql.return_value.collect.return_value = [self._make_stats_row(2, 2, 0)]
        result = m.finalize_data_copy_run.function(run_id=sample_folder_run_id, spark=mock_spark)
        assert result['status'] == 'COMPLETED'
        assert 'data_copy_runs' in mock_iceberg_retry.call_args[0][1]

    def test_completed_with_errors_when_failures(self, mock_spark, sample_folder_run_id, mock_iceberg_retry):
        mock_spark.sql.return_value.collect.return_value = [self._make_stats_row(3, 2, 1)]
        result = m.finalize_data_copy_run.function(run_id=sample_folder_run_id, spark=mock_spark)
        assert result['status'] == 'COMPLETED_WITH_ERRORS'
        assert result['failed_folders'] == 1

    def test_counts_new_statuses_and_writes_skipped(self, mock_spark, sample_folder_run_id,
                                                    mock_iceberg_retry):
        row = MagicMock()
        row.__getitem__ = lambda self, k: {
            'total_folders': 5, 'successful_folders': 3,
            'failed_folders': 1, 'skipped_folders': 1,
        }[k]
        mock_spark.sql.return_value.collect.return_value = [row]
        result = m.finalize_data_copy_run.function(run_id=sample_folder_run_id, spark=mock_spark)
        query = mock_spark.sql.call_args[0][0]
        assert 'EMPTY_SOURCE' in query
        assert 'SOURCE_NOT_FOUND' in query
        assert result['skipped_folders'] == 1
        assert 'skipped_folders    = 1' in mock_iceberg_retry.call_args[0][1]

    def test_failed_when_no_folder_records(self, mock_spark, sample_folder_run_id, mock_iceberg_retry):
        mock_spark.sql.return_value.collect.return_value = []
        result = m.finalize_data_copy_run.function(run_id=sample_folder_run_id, spark=mock_spark)
        assert result['status'] == 'FAILED'
        assert result['total_folders'] == 0

    def test_failed_when_total_folders_is_zero(self, mock_spark, sample_folder_run_id,
                                               mock_iceberg_retry):
        mock_spark.sql.return_value.collect.return_value = [self._make_stats_row(0, 0, 0)]
        result = m.finalize_data_copy_run.function(run_id=sample_folder_run_id, spark=mock_spark)
        assert result['status'] == 'FAILED'

    def test_failed_when_stats_query_raises(self, mock_spark, sample_folder_run_id,
                                            mock_iceberg_retry):
        mock_spark.sql.side_effect = Exception("table not found")
        result = m.finalize_data_copy_run.function(run_id=sample_folder_run_id, spark=mock_spark)
        assert result['status'] == 'FAILED'
        assert 'FAILED' in mock_iceberg_retry.call_args[0][1]

    def test_writes_service_account_when_cluster_setup_supplies_one(
        self, mock_spark, sample_folder_run_id, mock_iceberg_retry,
    ):
        mock_spark.sql.return_value.collect.return_value = [self._make_stats_row(2, 2, 0)]
        m.finalize_data_copy_run.function(
            run_id=sample_folder_run_id, spark=mock_spark,
            cluster_setup={
                'service_account_user_id': 'svc_migration',
                'service_account_source': 'active MapR ticket',
            },
        )
        assert mock_iceberg_retry.call_count == 2
        sql = mock_iceberg_retry.call_args_list[1][0][1]
        assert "service_account_user_id = 'svc_migration'" in sql
        assert "service_account_source  = 'active MapR ticket'" in sql

    def test_skips_service_account_update_when_absent(self, mock_spark, sample_folder_run_id,
                                                      mock_iceberg_retry):
        mock_spark.sql.return_value.collect.return_value = [self._make_stats_row(2, 2, 0)]
        m.finalize_data_copy_run.function(
            run_id=sample_folder_run_id, spark=mock_spark, cluster_setup={'temp_dir': '/tmp/x'},
        )
        assert mock_iceberg_retry.call_count == 1


class TestGenerateDataCopyHtmlReport:

    def test_generates_report_and_writes_to_s3(self, mock_spark, sample_folder_run_id, sample_folder_finalize_result):
        run_row = SimpleNamespace(
            run_id=sample_folder_run_id, status='COMPLETED',
            excel_file_path='s3a://bucket/fc.xlsx',
            started_at='2025-01-01 12:00:00', completed_at='2025-01-01 12:10:00',
        )
        folder_row = SimpleNamespace(
            run_id=sample_folder_run_id, source_path='/data/sales/raw',
            dest_bucket='s3a://test-bucket', dest_path='raw',
            status='VALIDATED', is_incremental=False,
            source_file_count=20, dest_file_count=20, file_count_match=True,
            source_size_bytes=52428800, dest_size_bytes=52428800, size_match=True,
            bytes_copied=52428800, files_copied=20, error_message=None,
        )
        mock_spark.sql.side_effect = [
            MagicMock(collect=MagicMock(return_value=[run_row])),
            MagicMock(collect=MagicMock(return_value=[folder_row])),
        ]
        result = m.generate_data_copy_html_report.function(
            run_id=sample_folder_run_id,
            finalize_result=sample_folder_finalize_result,
            spark=mock_spark,
        )
        assert result['report_path'].endswith('.html')
        assert 'data_copy_report' in result['report_path']

    def _report_rows(self, run_id, sa_user='', sa_source='', status='VALIDATED'):
        run_row = SimpleNamespace(
            run_id=run_id, status='COMPLETED',
            excel_file_path='s3a://bucket/fc.xlsx',
            started_at='2025-01-01 12:00:00', completed_at='2025-01-01 12:10:00',
            dag_run_id='manual__2025-01-01T12:00:00',
            service_account_user_id=sa_user, service_account_source=sa_source,
        )
        folder_row = SimpleNamespace(
            run_id=run_id, source_path='/data/sales/raw',
            dest_bucket='s3a://test-bucket', dest_path='raw',
            status=status, is_incremental=False,
            source_file_count=0, dest_file_count=0, file_count_match=False,
            source_size_bytes=0, dest_size_bytes=0, size_match=False,
            bytes_copied=0, files_copied=0, error_message=None,
            distcp_duration_seconds=0.0, throughput_mbps=0.0, yarn_application_id='',
        )
        return run_row, folder_row

    def _written_html(self, mock_spark):
        fs = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        return fs.create.return_value.write.call_args[0][0].decode('utf-8')

    def test_cluster_setup_service_account_wins_over_run_row(
        self, mock_spark, sample_folder_run_id, sample_folder_finalize_result,
    ):
        run_row, folder_row = self._report_rows(
            sample_folder_run_id, sa_user='row_user', sa_source='row_source',
        )
        mock_spark.sql.side_effect = [
            MagicMock(collect=MagicMock(return_value=[run_row])),
            MagicMock(collect=MagicMock(return_value=[folder_row])),
        ]
        m.generate_data_copy_html_report.function(
            run_id=sample_folder_run_id,
            finalize_result=sample_folder_finalize_result,
            spark=mock_spark,
            cluster_setup={
                'service_account_user_id': 'edge_user',
                'service_account_source': 'active MapR ticket',
            },
        )
        html = self._written_html(mock_spark)
        assert 'edge_user' in html
        assert 'active MapR ticket' in html
        assert 'row_user' not in html

    def test_falls_back_to_run_row_service_account(
        self, mock_spark, sample_folder_run_id, sample_folder_finalize_result,
    ):
        run_row, folder_row = self._report_rows(
            sample_folder_run_id, sa_user='row_user', sa_source='row_source',
        )
        mock_spark.sql.side_effect = [
            MagicMock(collect=MagicMock(return_value=[run_row])),
            MagicMock(collect=MagicMock(return_value=[folder_row])),
        ]
        m.generate_data_copy_html_report.function(
            run_id=sample_folder_run_id,
            finalize_result=sample_folder_finalize_result,
            spark=mock_spark,
        )
        html = self._written_html(mock_spark)
        assert 'row_user' in html
        assert 'row_source' in html

    def test_not_configured_when_no_service_account_anywhere(
        self, mock_spark, sample_folder_run_id, sample_folder_finalize_result,
    ):
        run_row, folder_row = self._report_rows(sample_folder_run_id)
        mock_spark.sql.side_effect = [
            MagicMock(collect=MagicMock(return_value=[run_row])),
            MagicMock(collect=MagicMock(return_value=[folder_row])),
        ]
        m.generate_data_copy_html_report.function(
            run_id=sample_folder_run_id,
            finalize_result=sample_folder_finalize_result,
            spark=mock_spark,
        )
        html = self._written_html(mock_spark)
        assert 'not configured' in html
        assert 'unknown' in html

    @pytest.mark.parametrize("status", ['EMPTY_SOURCE', 'SOURCE_NOT_FOUND', 'SKIPPED'])
    def test_neutral_match_cells_for_non_failure_statuses(
        self, mock_spark, sample_folder_run_id, sample_folder_finalize_result, status,
    ):
        run_row, folder_row = self._report_rows(sample_folder_run_id, status=status)
        mock_spark.sql.side_effect = [
            MagicMock(collect=MagicMock(return_value=[run_row])),
            MagicMock(collect=MagicMock(return_value=[folder_row])),
        ]
        m.generate_data_copy_html_report.function(
            run_id=sample_folder_run_id,
            finalize_result=sample_folder_finalize_result,
            spark=mock_spark,
        )
        html = self._written_html(mock_spark)
        assert '<td class="neutral-cell">—</td>' in html
        assert '<td class="fail">✗</td>' not in html


class TestCheckDataCopyRunOutcome:

    def test_returns_dict_when_clean(self, sample_folder_run_id):
        payload = {
            'run_id': sample_folder_run_id, 'status': 'COMPLETED',
            'total_folders': 3, 'successful_folders': 3, 'failed_folders': 0,
        }
        assert m.check_data_copy_run_outcome.function(finalize_result=payload) == payload

    def test_raises_when_folders_failed(self, sample_folder_run_id):
        payload = {
            'run_id': sample_folder_run_id, 'status': 'COMPLETED_WITH_ERRORS',
            'total_folders': 3, 'successful_folders': 1, 'failed_folders': 2,
        }
        with pytest.raises(Exception, match="2/3 folder"):
            m.check_data_copy_run_outcome.function(finalize_result=payload)

    def test_raises_when_no_folder_records(self, sample_folder_run_id):
        payload = {
            'run_id': sample_folder_run_id, 'status': 'FAILED',
            'total_folders': 0, 'successful_folders': 0, 'failed_folders': 0,
        }
        with pytest.raises(Exception, match="no folder records"):
            m.check_data_copy_run_outcome.function(finalize_result=payload)

    def test_returns_empty_dict_for_invalid_input(self):
        assert m.check_data_copy_run_outcome.function(finalize_result=None) == {}
        assert m.check_data_copy_run_outcome.function(finalize_result={'total_folders': 1}) == {}


class TestFolderCopyDagFailureCallback:

    def _context(self, run_id_xcom=None, has_ti=True):
        dag_run = MagicMock()
        dag_run.run_id = 'manual__2025-01-01T12:00:00'
        if has_ti:
            ti = MagicMock()
            ti.xcom_pull.return_value = run_id_xcom
            dag_run.get_task_instance.return_value = ti
        else:
            dag_run.get_task_instance.return_value = None
        return {'dag_run': dag_run, 'task_instance': MagicMock(task_id='run_folder_distcp_ssh')}

    def test_logs_run_id_when_found(self, mock_iceberg_retry):
        ctx = self._context(run_id_xcom='folder_run_20250101_120000_abcd1234')
        with patch.object(m.logger, 'warning') as warn:
            m._folder_copy_dag_failure_callback(ctx)
        messages = [str(c[0][0]) for c in warn.call_args_list]
        assert any('folder_run_20250101_120000_abcd1234' in msg for msg in messages)
        assert any('RUNNING' in msg for msg in messages)
        mock_iceberg_retry.assert_not_called()

    def test_logs_when_run_id_missing(self, mock_iceberg_retry):
        with patch.object(m.logger, 'warning') as warn:
            m._folder_copy_dag_failure_callback(self._context(has_ti=False))
        messages = [str(c[0][0]) for c in warn.call_args_list]
        assert any('no tracking run_id' in msg for msg in messages)
        mock_iceberg_retry.assert_not_called()

    def test_swallows_xcom_lookup_error(self, mock_iceberg_retry):
        dag_run = MagicMock()
        dag_run.get_task_instance.side_effect = Exception("no such task")
        with patch.object(m.logger, 'warning') as warn:
            m._folder_copy_dag_failure_callback(
                {'dag_run': dag_run, 'task_instance': MagicMock()}
            )
        messages = [str(c[0][0]) for c in warn.call_args_list]
        assert any('could not read the tracking run_id XCom' in msg for msg in messages)
        mock_iceberg_retry.assert_not_called()

    def test_swallows_broken_context(self, mock_iceberg_retry):
        m._folder_copy_dag_failure_callback(None)
        mock_iceberg_retry.assert_not_called()


class TestSendDataCopyReportEmail:

    def test_skips_when_no_recipients(self, mock_spark, sample_folder_run_id):
        with patch('migration_dag_folder_copy.get_config') as cfg:
            cfg.return_value = {'smtp_conn_id': 'smtp_default', 'email_recipients': ''}
            result = m.send_data_copy_report_email.function(
                report_result={'report_path': 's3a://b/r.html', 'html_content': '<html/>'},
                run_id=sample_folder_run_id, spark=mock_spark,
            )
        assert result == {'sent': False, 'reason': 'no_recipients'}
