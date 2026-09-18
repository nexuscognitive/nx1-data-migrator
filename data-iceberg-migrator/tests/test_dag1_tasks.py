"""DAG 1 Task Tests: mapr_to_s3_migration pipeline."""

import itertools
import logging
import re
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import migration_dag_mapr_to_s3 as m
import pytest

from .helpers import distcp_call, make_excel_bytes, mock_ssh_stdout, setup_spark_excel


def _make_distcp_stdout(incremental=False):
    return mock_ssh_stdout(0, (
        "===DISTCP_METRICS_START===\n"
        f"INCREMENTAL={'true' if incremental else 'false'}\n"
        "S3_FILE_COUNT_BEFORE=0\nS3_TOTAL_SIZE_BEFORE=0\nDISTCP_EXIT_CODE=0\n"
        "BYTES_COPIED=10485760\nFILES_COPIED=5\n"
        "S3_FILE_COUNT_AFTER=5\nS3_TOTAL_SIZE_AFTER=10485760\n"
        "S3_FILES_TRANSFERRED=5\nS3_BYTES_TRANSFERRED=10485760\n"
        "===DISTCP_METRICS_END===\n"
    ).encode())


def _partitioned_discovery(sample_discovery, partitions=('d=2024', 'd=2025')):
    """One table on the per-partition path: filtered, preserve_delete default."""
    base = sample_discovery['tables'][0]
    return {
        **sample_discovery,
        'tables': [{
            **base,
            'partition_filter_active': True,
            'partition_filter': 'd>=2024',
            'filtered_partitions': list(partitions),
            'partition_file_counts': {p: 5 for p in partitions},
            'filtered_file_count': 5 * len(partitions),
            'filtered_source_size_bytes': 1024 ** 3,
        }],
    }


def assert_each_overall_status_case_preserves_skippable(calls):
    """Every statement rewriting `overall_status` through a CASE must carry the
    preserve guard, expanded rather than left as a literal placeholder.

    Asserted per statement, not on the joined SQL: several of these UPDATEs fire in
    one task, so a joined assertion stays green if any *one* keeps the guard.
    Returns the matched task labels so callers can pin which UPDATEs they hit."""
    guarded = [
        (c.kwargs.get('task_label'), c.args[1])
        for c in calls
        if 'overall_status = CASE' in c.args[1]
    ]
    assert guarded, 'no overall_status CASE statement was emitted'
    for label, sql in guarded:
        assert m._PRESERVE_SKIPPABLE_STATUS_SQL in sql, f'guard missing from {label}'
        assert '_PRESERVE_SKIPPABLE_STATUS_SQL' not in sql, f'unexpanded in {label}'
    return [label for label, _ in guarded]


class TestValidatePrerequisites:

    def test_all_checks_pass(self, mock_ssh_hook):
        hook, client, stdout_mock, _ = mock_ssh_hook
        responses = [
            (MagicMock(), mock_ssh_stdout(0, b'SSH_TEST_OK'), MagicMock()),
            (MagicMock(), mock_ssh_stdout(0, b'CLUSTER_AUTH_OK'), MagicMock()),
            (MagicMock(), mock_ssh_stdout(0, b'PYSPARK_HIVE_OK'), MagicMock()),
            (MagicMock(), mock_ssh_stdout(0, b'HADOOP_FS_OK'), MagicMock()),
        ]
        for r in responses:
            r[2].read.return_value = b''
        client.exec_command.side_effect = responses

        result = m.validate_prerequisites.function(run_id='test_run')
        assert result['ssh_connectivity'] is True
        assert result['cluster_auth'] is True
        assert result['pyspark_available'] is True
        assert result['hive_available'] is True
        assert result['hadoop_fs_available'] is True
        assert result['errors'] == []

    def test_ssh_failure_raises(self, mock_ssh_hook):
        hook, client, _, _ = mock_ssh_hook
        hook.get_conn.side_effect = Exception("Connection refused")

        with pytest.raises(Exception, match="Pre-DAG validation failed"):
            m.validate_prerequisites.function(run_id='test_run')


class TestInitTrackingTables:

    def test_creates_database_and_all_tables(self, mock_spark):
        result = m.init_tracking_tables.function(spark=mock_spark)
        assert result == {'status': 'initialized', 'database': 'migration_tracking'}
        assert mock_spark.sql.call_count >= 3
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        for table in ['migration_runs', 'migration_table_status']:
            assert table in all_sql


class TestCreateMigrationRun:

    def test_creates_run_with_running_status(self, mock_spark):
        run_id = m.create_migration_run.function(
            excel_file_path='s3a://bucket/file.xlsx',
            dag_run_id='dag_run_123',
            spark=mock_spark,
        )
        assert run_id.startswith('run_') and len(run_id) > 10
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'INSERT INTO' in all_sql
        assert 'RUNNING' in all_sql


class TestParseExcel:

    def test_basic_parse(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'sales', 'table': '*', 'dest database': 'sales_s3', 'bucket': 's3a://mybucket'},
        ]))
        result = m.parse_excel.function('s3a://bucket/file.xlsx', 'run_test', spark=mock_spark)
        assert len(result) == 1
        assert result[0]['source_database'] == 'sales'
        assert result[0]['dest_database'] == 'sales_s3'
        assert result[0]['run_id'] == 'run_test'

    @pytest.mark.parametrize("raw_bucket,expected_prefix", [
        ('s3://mybucket', 's3a://'),
        ('s3n://mybucket', 's3a://'),
        ('mybucket', 's3a://'),
        ('s3a://mybucket', 's3a://'),
    ])
    def test_normalizes_bucket_prefix(self, mock_spark, raw_bucket, expected_prefix):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'db', 'table': '*', 'dest database': '', 'bucket': raw_bucket},
        ]))
        result = m.parse_excel.function('s3a://b/f.xlsx', 'run_test', spark=mock_spark)
        assert result[0]['dest_bucket'].startswith(expected_prefix)

    def test_defaults_and_wildcard_handling(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'mydb', 'table': 'tbl_a', 'dest database': None, 'bucket': None},
            {'database': 'mydb', 'table': '*', 'dest database': None, 'bucket': None},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert result[0]['table_tokens'] == ['*']
        assert result[0]['dest_database'] == 'mydb'

    def test_comma_separated_tables_tokenized(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'db1', 'table': 'tbl_a,tbl_b,tbl_c', 'dest database': '', 'bucket': ''},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert set(result[0]['table_tokens']) == {'tbl_a', 'tbl_b', 'tbl_c'}

    def test_s3_bucket_normalized(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'mydb', 'table': '*', 'dest database': '', 'bucket': 's3://plain-bucket'},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert result[0]['dest_bucket'].startswith('s3a://')

    def test_dest_database_defaults_to_source(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'sourcedb', 'table': '*', 'dest database': None, 'bucket': None},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert result[0]['dest_database'] == 'sourcedb'

    def test_wildcard_overrides_other_tokens(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'db', 'table': 'tbl_a', 'dest database': '', 'bucket': ''},
            {'database': 'db', 'table': '*', 'dest database': '', 'bucket': ''},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        # When * is present, tokens should collapse to ['*']
        assert result[0]['table_tokens'] == ['*']

    def test_run_id_embedded_in_each_config(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'db1', 'table': '*', 'dest database': '', 'bucket': ''},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_xyz', spark=mock_spark)
        assert result[0]['run_id'] == 'run_xyz'

    @patch('migration_dag_mapr_to_s3.validate_bucket_endpoint_pairs')
    def test_dest_endpoint_emitted_when_present(self, _mock_validate, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'db1', 'table': '*', 'dest database': '', 'bucket': 's3a://bkt',
            'endpoint': 'https://s3.tenant-a.example.com'},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_xyz', spark=mock_spark)
        assert result[0]['dest_endpoint'] == 'https://s3.tenant-a.example.com'

    def test_dest_endpoint_defaults_to_empty_when_absent(self, mock_spark):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'db1', 'table': '*', 'dest database': '', 'bucket': 's3a://bkt'},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert result[0]['dest_endpoint'] == ''

    @patch('migration_dag_mapr_to_s3.validate_bucket_endpoint_pairs')
    def test_same_bucket_different_endpoint_produces_two_configs(self, _mock_validate, mock_spark):
        """Same (src_db, dest_db, bucket) but different endpoints must not be merged."""
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'db1', 'table': 'tbl_a', 'dest database': 'db1_s3', 'bucket': 's3a://data-lake',
            'endpoint': 'https://s3.tenant-a.example.com'},
            {'database': 'db1', 'table': 'tbl_b', 'dest database': 'db1_s3', 'bucket': 's3a://data-lake',
            'endpoint': 'https://s3.tenant-b.example.com'},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert len(result) == 2
        endpoints = {r['dest_endpoint'] for r in result}
        assert endpoints == {'https://s3.tenant-a.example.com', 'https://s3.tenant-b.example.com'}

    @patch('migration_dag_mapr_to_s3.validate_bucket_endpoint_pairs')
    def test_same_bucket_same_endpoint_merged_into_one_config(self, _mock_validate, mock_spark):
        """Same (src_db, dest_db, bucket, endpoint) on two rows must merge tokens."""
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'db1', 'table': 'tbl_a', 'dest database': 'db1_s3', 'bucket': 's3a://data-lake',
            'endpoint': 'https://s3.tenant-a.example.com'},
            {'database': 'db1', 'table': 'tbl_b', 'dest database': 'db1_s3', 'bucket': 's3a://data-lake',
            'endpoint': 'https://s3.tenant-a.example.com'},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert len(result) == 1
        assert set(result[0]['table_tokens']) == {'tbl_a', 'tbl_b'}

    def test_partition_filter_emitted_in_config(self, mock_spark):
        """partition_filter column is parsed and passed through to the config."""
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'db1', 'table': '*', 'dest database': '',
             'bucket': 's3a://bkt', 'partition_filter': 'dt>=2024-01-01'},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert result[0]['partition_filter'] == 'dt>=2024-01-01'

    def test_partition_filter_defaults_to_none_when_absent(self, mock_spark):
        """Rows without a partition_filter column emit None."""
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'db1', 'table': '*', 'dest database': '', 'bucket': 's3a://bkt'},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert result[0]['partition_filter'] is None

    def test_same_db_different_partition_filter_produces_two_configs(self, mock_spark):
        """Two rows for the same db/bucket but different partition_filter values
        must NOT be merged — partition_filter is part of the grouping key."""
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'db1', 'table': 'tbl_a', 'dest database': 'db1_s3',
             'bucket': 's3a://data-lake', 'partition_filter': 'dt>=2024-01-01'},
            {'database': 'db1', 'table': 'tbl_b', 'dest database': 'db1_s3',
             'bucket': 's3a://data-lake', 'partition_filter': 'dt>=2024-06-01'},
        ]))
        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert len(result) == 2
        filters = {r['partition_filter'] for r in result}
        assert filters == {'dt>=2024-01-01', 'dt>=2024-06-01'}


# ---------------------------------------------------------------------------
# cluster_login_setup
# ---------------------------------------------------------------------------
class TestClusterLoginSetup:

    def test_success_returns_temp_dir(self, mock_ssh_hook):
        hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        stdout_mock.read.return_value = b'CLUSTER_LOGIN_SUCCESS\nTEMP_DIR=/tmp/migration/run_test'
        stderr_mock.read.return_value = b''

        result = m.cluster_login_setup.function(run_id='run_test')
        assert 'temp_dir' in result
        assert result['run_id'] == 'run_test'

    def test_returns_distcp_log_dir_and_service_account(self, mock_ssh_hook):
        """Regression: the resolved SA and cluster-FS log dir must reach the caller."""
        hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        stdout_mock.read.return_value = (
            b'CLUSTER_LOGIN_SUCCESS\n'
            b'MAPR_EFFECTIVE_USER=ETL_CIHA_CU\n'
            b'SERVICE_ACCOUNT_SOURCE=config:service_account_user_id\n'
            b'TEMP_DIR=/tmp/migration/run_test\n'
            b'DISTCP_LOG_DIR=/tmp/ETL_CIHA_CU/distcp_logs/run_test\n'
        )
        stderr_mock.read.return_value = b''

        result = m.cluster_login_setup.function(run_id='run_test')
        assert result['distcp_log_dir'] == '/tmp/ETL_CIHA_CU/distcp_logs/run_test'
        assert result['service_account_user_id'] == 'ETL_CIHA_CU'
        assert result['service_account_source'] == 'config:service_account_user_id'
        # DistCp logs must NOT be written under the local edge temp dir.
        assert not result['distcp_log_dir'].startswith(result['temp_dir'])

    def test_configured_service_account_is_not_clobbered_by_ticket(self, mock_ssh_hook):
        """Regression: the generated script must not overwrite a configured SA user."""
        hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        stdout_mock.read.return_value = b'CLUSTER_LOGIN_SUCCESS\nTEMP_DIR=/tmp/migration/run_test\n'
        stderr_mock.read.return_value = b''

        m.cluster_login_setup.function(run_id='run_test')
        script = client.exec_command.call_args[0][0]
        # The maprlogin-derived value may only be applied when nothing is configured.
        assert 'if [ -z "$CONFIGURED_SA_USER" ]; then' in script
        ticket_idx = script.index('TICKET_USER=$(maprlogin print')
        guard_idx = script.index('if [ -z "$CONFIGURED_SA_USER" ]; then')
        assert guard_idx < ticket_idx

    def test_nonzero_exit_raises(self, mock_ssh_hook):
        hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        stdout_mock.channel.recv_exit_status.return_value = 1
        stdout_mock.read.return_value = b'ERROR'
        stderr_mock.read.return_value = b'auth error'

        with pytest.raises(Exception, match="Cluster login setup failed"):
            m.cluster_login_setup.function(run_id='run_test')

    def test_missing_success_marker_raises(self, mock_ssh_hook):
        hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        stdout_mock.channel.recv_exit_status.return_value = 0
        stdout_mock.read.return_value = b'Something happened but no marker'
        stderr_mock.read.return_value = b''

        with pytest.raises(Exception, match="success marker not found"):
            m.cluster_login_setup.function(run_id='run_test')


class TestClassifyDiscoveryError:
    """Skip the table and migrate its siblings, or abort the whole database? Decided
    on the remote fs.exists(), never on the wording of the exception."""

    def test_missing_root_location_is_skippable(self):
        assert m._classify_discovery_error(
            "u'Path does not exist: maprfs:/datalake/sales/orders;'", False
        ) == 'SOURCE_PATH_NOT_FOUND'

    def test_intact_root_location_is_not_skippable(self):
        # A leaf file vanishing mid-listing (compaction, or one missing partition)
        # is indistinguishable from a gone root in the text; only fs.exists() tells
        # them apart, and this table is still migratable on retry.
        assert m._classify_discovery_error(
            'java.io.FileNotFoundException: File does not exist: '
            'maprfs:/datalake/sales/orders/part-00000-abc.snappy.parquet', True
        ) == 'FAILED'

    def test_root_gone_wins_over_an_unrelated_error(self):
        # Unmigratable whatever else failed on the way; the raw error still lands
        # in tracking.
        assert m._classify_discovery_error(
            'java.lang.OutOfMemoryError: GC overhead limit exceeded', False
        ) == 'SOURCE_PATH_NOT_FOUND'

    def test_permission_error_is_not_skippable_even_when_root_reads_as_absent(self):
        # An ACL problem normally throws, leaving None; pins the guard in case a
        # filesystem returns False instead.
        assert m._classify_discovery_error(
            'org.apache.hadoop.security.AccessControlException: Permission denied: '
            'user=svc_migration, path="maprfs:/datalake/sales/orders"',
            False,
        ) == 'FAILED'

    def test_unknown_existence_is_not_skippable(self):
        """No ground truth — abort rather than guess."""
        assert m._classify_discovery_error(
            'Path does not exist: maprfs:/datalake/sales/orders', None
        ) == 'FAILED'


class TestDiscoverTablesViaSshSpark:

    def _make_discovery_output(self, metadata_json):
        return f"some pyspark log output\n===JSON_START===\n{metadata_json}\n===JSON_END===\nmore output".encode()

    def test_successful_discovery(self, mock_ssh_hook, sample_run_id):
        hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        import json
        metadata = [{
            'source_database': 'sales', 'source_table': 'orders',
            'dest_database': 'sales_s3', 'dest_bucket': 's3a://bucket',
            'source_location': 'maprfs:///data/sales/orders',
            's3_location': 's3a://bucket/sales_s3/orders',
            'file_format': 'PARQUET', 'schema': [{'name': 'id', 'type': 'bigint'}],
            'partitions': [], 'partition_columns': '', 'partition_count': 0,
            'row_count': 500, 'is_partitioned': False,
            'unregistered_partitions': False, 'table_type': 'EXTERNAL',
            'source_total_size_bytes': 1024, 'source_file_count': 1,
            'serde_properties': {},
            'partition_filter': None, 'filtered_partitions': [],
            'partition_filter_active': False, 'filtered_row_count': 500,
            'filtered_source_size_bytes': 1024, 'filtered_file_count': 1,
            'full_table_row_count': 500, 'full_table_partition_count': 0,
        }]
        # First exec_command: mkdir, second: pyspark
        mkdir_stdout = mock_ssh_stdout(0, b'')
        pyspark_stdout = mock_ssh_stdout(0, self._make_discovery_output(json.dumps(metadata)))
        client.exec_command.side_effect = [
            (MagicMock(), mkdir_stdout, MagicMock()),
            (MagicMock(), pyspark_stdout, MagicMock()),
        ]

        db_config = {
            'run_id': sample_run_id, 'source_database': 'sales',
            'table_tokens': ['orders'], 'dest_database': 'sales_s3',
            'dest_bucket': 's3a://bucket',
        }
        result = m.discover_tables_via_spark_ssh.function.__wrapped__(db_config=db_config)
        assert result['source_database'] == 'sales'
        assert len(result['tables']) == 1
        assert result['tables'][0]['source_table'] == 'orders'

    def test_spark_nonzero_exit_raises(self, mock_ssh_hook, sample_run_id):
        hook, client, _, _ = mock_ssh_hook
        mkdir_stdout = mock_ssh_stdout(0, b'')
        pyspark_stdout = mock_ssh_stdout(1, b'Error: some spark failure')
        pyspark_stderr = MagicMock()
        pyspark_stderr.read.return_value = b'spark error details'
        client.exec_command.side_effect = [
            (MagicMock(), mkdir_stdout, MagicMock()),
            (MagicMock(), pyspark_stdout, pyspark_stderr),
        ]

        db_config = {
            'run_id': sample_run_id, 'source_database': 'sales',
            'table_tokens': ['*'], 'dest_database': 'sales_s3',
            'dest_bucket': 's3a://bucket',
        }
        with pytest.raises(Exception, match="Table discovery Spark job failed"):
            m.discover_tables_via_spark_ssh.function.__wrapped__(db_config=db_config)

    def test_missing_json_markers_raises(self, mock_ssh_hook, sample_run_id):
        hook, client, _, _ = mock_ssh_hook
        mkdir_stdout = mock_ssh_stdout(0, b'')
        pyspark_stdout = mock_ssh_stdout(0, b'output without json markers')
        client.exec_command.side_effect = [
            (MagicMock(), mkdir_stdout, MagicMock()),
            (MagicMock(), pyspark_stdout, MagicMock()),
        ]

        db_config = {
            'run_id': sample_run_id, 'source_database': 'sales',
            'table_tokens': ['*'], 'dest_database': 'sales_s3',
            'dest_bucket': 's3a://bucket',
        }
        with pytest.raises(Exception, match="Could not find JSON markers"):
            m.discover_tables_via_spark_ssh.function.__wrapped__(db_config=db_config)

    def test_table_with_error_raises(self, mock_ssh_hook, sample_run_id):
        hook, client, _, _ = mock_ssh_hook
        import json
        metadata = [{
            'source_database': 'sales', 'source_table': 'broken',
            'dest_database': 'sales_s3', 'dest_bucket': 's3a://bucket',
            'source_location': '', 's3_location': 's3a://bucket/sales_s3/broken',
            'file_format': 'PARQUET', 'schema': [], 'partitions': [],
            'partition_columns': '', 'partition_count': 0, 'row_count': 0,
            'is_partitioned': False, 'unregistered_partitions': False,
            'table_type': 'UNKNOWN', 'source_total_size_bytes': 0,
            'source_file_count': 0, 'serde_properties': {},
            'partition_filter': None, 'filtered_partitions': [],
            'partition_filter_active': False, 'filtered_row_count': 0,
            'filtered_source_size_bytes': 0, 'filtered_file_count': 0,
            'full_table_row_count': 0, 'full_table_partition_count': 0,
            'error': 'Table not found',
        }]
        mkdir_stdout = mock_ssh_stdout(0, b'')
        pyspark_stdout = mock_ssh_stdout(0, self._make_discovery_output(json.dumps(metadata)))
        client.exec_command.side_effect = [
            (MagicMock(), mkdir_stdout, MagicMock()),
            (MagicMock(), pyspark_stdout, MagicMock()),
        ]

        db_config = {
            'run_id': sample_run_id, 'source_database': 'sales',
            'table_tokens': ['broken'], 'dest_database': 'sales_s3',
            'dest_bucket': 's3a://bucket',
        }
        with pytest.raises(Exception, match="Discovery failed for"):
            m.discover_tables_via_spark_ssh.function.__wrapped__(db_config=db_config)

    def test_table_not_found_does_not_raise(self, mock_ssh_hook, sample_run_id):
        hook, client, _, _ = mock_ssh_hook
        import json
        metadata = [{
            'source_database': 'sales', 'source_table': 'ghost',
            'dest_database': 'sales_s3', 'dest_bucket': 's3a://bucket',
            'source_location': '', 's3_location': 's3a://bucket/sales_s3/ghost',
            'file_format': 'UNKNOWN', 'schema': [], 'partitions': [],
            'partition_columns': '', 'partition_count': 0, 'row_count': 0,
            'is_partitioned': False, 'unregistered_partitions': False,
            'table_type': 'UNKNOWN', 'source_total_size_bytes': 0,
            'source_file_count': 0, 'serde_properties': {},
            'partition_filter': None, 'filtered_partitions': [],
            'partition_filter_active': False, 'filtered_row_count': 0,
            'filtered_source_size_bytes': 0, 'filtered_file_count': 0,
            'full_table_row_count': 0, 'full_table_partition_count': 0,
            'error': 'Table or view not found: sales.ghost',
            'error_type': 'TABLE_NOT_FOUND',
        }]
        mkdir_stdout = mock_ssh_stdout(0, b'')
        pyspark_stdout = mock_ssh_stdout(0, self._make_discovery_output(json.dumps(metadata)))
        client.exec_command.side_effect = [
            (MagicMock(), mkdir_stdout, MagicMock()),
            (MagicMock(), pyspark_stdout, MagicMock()),
        ]

        db_config = {
            'run_id': sample_run_id, 'source_database': 'sales',
            'table_tokens': ['ghost'], 'dest_database': 'sales_s3',
            'dest_bucket': 's3a://bucket',
        }
        result = m.discover_tables_via_spark_ssh.function.__wrapped__(db_config=db_config)
        assert len(result['tables']) == 1
        assert result['tables'][0]['error_type'] == 'TABLE_NOT_FOUND'

    def test_database_not_found_does_not_raise(self, mock_ssh_hook, sample_run_id):
        hook, client, _, _ = mock_ssh_hook
        import json
        metadata = [{
            'source_database': 'phantom_db', 'source_table': 'orders',
            'dest_database': 'sales_s3', 'dest_bucket': 's3a://bucket',
            'source_location': '', 's3_location': 's3a://bucket/sales_s3/orders',
            'file_format': 'UNKNOWN', 'schema': [], 'partitions': [],
            'partition_columns': '', 'partition_count': 0, 'row_count': 0,
            'is_partitioned': False, 'unregistered_partitions': False,
            'table_type': 'UNKNOWN', 'source_total_size_bytes': 0,
            'source_file_count': 0, 'serde_properties': {},
            'partition_filter': None, 'filtered_partitions': [],
            'partition_filter_active': False, 'filtered_row_count': 0,
            'filtered_source_size_bytes': 0, 'filtered_file_count': 0,
            'full_table_row_count': 0, 'full_table_partition_count': 0,
            'error': "Source database 'phantom_db' does not exist on the cluster",
            'error_type': 'DATABASE_NOT_FOUND',
        }]
        mkdir_stdout = mock_ssh_stdout(0, b'')
        pyspark_stdout = mock_ssh_stdout(0, self._make_discovery_output(json.dumps(metadata)))
        client.exec_command.side_effect = [
            (MagicMock(), mkdir_stdout, MagicMock()),
            (MagicMock(), pyspark_stdout, MagicMock()),
        ]

        db_config = {
            'run_id': sample_run_id, 'source_database': 'phantom_db',
            'table_tokens': ['orders'], 'dest_database': 'sales_s3',
            'dest_bucket': 's3a://bucket',
        }
        result = m.discover_tables_via_spark_ssh.function.__wrapped__(db_config=db_config)
        assert len(result['tables']) == 1
        assert result['tables'][0]['error_type'] == 'DATABASE_NOT_FOUND'

    def _make_metadata_record(self, table, **overrides):
        record = {
            'source_database': 'sales', 'source_table': table,
            'dest_database': 'sales_s3', 'dest_bucket': 's3a://bucket',
            'source_location': f'maprfs:/datalake/sales/{table}',
            's3_location': f's3a://bucket/sales_s3/{table}',
            'file_format': 'PARQUET', 'schema': [], 'partitions': [],
            'partition_columns': '', 'partition_count': 0, 'row_count': 0,
            'is_partitioned': False, 'unregistered_partitions': False,
            'table_type': 'EXTERNAL', 'source_total_size_bytes': 0,
            'source_file_count': 0, 'serde_properties': {},
            'partition_filter': None, 'filtered_partitions': [],
            'partition_filter_active': False, 'filtered_row_count': 0,
            'filtered_source_size_bytes': 0, 'filtered_file_count': 0,
            'full_table_row_count': 0, 'full_table_partition_count': 0,
            'source_path_exists': True,
        }
        record.update(overrides)
        return record

    def _run_discovery(self, client, metadata, sample_run_id, tokens):
        import json
        client.exec_command.side_effect = [
            (MagicMock(), mock_ssh_stdout(0, b''), MagicMock()),
            (MagicMock(), mock_ssh_stdout(0, self._make_discovery_output(json.dumps(metadata))), MagicMock()),
        ]
        return m.discover_tables_via_spark_ssh.function.__wrapped__(db_config={
            'run_id': sample_run_id, 'source_database': 'sales',
            'table_tokens': tokens, 'dest_database': 'sales_s3',
            'dest_bucket': 's3a://bucket',
        })

    def test_generated_remote_script_is_valid_python(self, mock_ssh_hook, sample_run_id):
        """The script only ever runs on the edge node, where a syntax error surfaces
        as an opaque Spark failure. Compile it here instead."""
        _, client, _, _ = mock_ssh_hook
        self._run_discovery(
            client, [self._make_metadata_record('orders')], sample_run_id, ['orders'],
        )
        sftp_file = client.open_sftp.return_value.file.return_value
        script = sftp_file.__enter__.return_value.write.call_args[0][0]
        compile(script, 'discover_tables.py', 'exec')
        # Guard the two fields the driver-side classifier depends on.
        probe = 'source_path_exists = bool(fs.exists(path))'
        assert probe in script
        assert '"source_path_exists": source_path_exists' in script
        # Load-bearing ordering, ~150 lines apart in one template: reversed, the
        # blanket handler only ever sees None and every orphan aborts its database
        # again — WF-343 restored with the rest of this suite green.
        assert script.index(probe) < script.index('spark.table("{0}.{1}"')

    def test_orphaned_metastore_entry_does_not_abort_sibling_tables(
        self, mock_ssh_hook, sample_run_id
    ):
        """WF-343: an orphaned metastore entry is a source-data condition, not a
        discovery failure — skip it and migrate its healthy siblings."""
        _, client, _, _ = mock_ssh_hook
        metadata = [
            self._make_metadata_record('good_one'),
            self._make_metadata_record(
                't_ce_phonepref_ref',
                error="u'Path does not exist: maprfs:/datalake/sales/t_ce_phonepref_ref;'",
                error_type='FAILED',
                source_path_exists=False,
            ),
            self._make_metadata_record('good_two'),
        ]

        result = self._run_discovery(
            client, metadata, sample_run_id,
            ['good_one', 't_ce_phonepref_ref', 'good_two'],
        )

        by_table = {t['source_table']: t for t in result['tables']}
        assert set(by_table) == {'good_one', 't_ce_phonepref_ref', 'good_two'}
        assert by_table['t_ce_phonepref_ref']['error_type'] == 'SOURCE_PATH_NOT_FOUND'
        assert by_table['good_one'].get('error_type') is None
        assert by_table['good_two'].get('error_type') is None

    def test_orphan_that_never_threw_is_still_classified(self, mock_ssh_hook, sample_run_id):
        """A TEXTFILE/Avro orphan never throws — its schema comes from the catalog —
        so it emits as a success record and would reach DistCp with 0 files and be
        reported EMPTY_SOURCE, which finalize_run counts as successful."""
        _, client, _, _ = mock_ssh_hook
        metadata = [
            self._make_metadata_record('good_one'),
            self._make_metadata_record(
                'textfile_orphan', file_format='TEXTFILE', source_path_exists=False,
            ),
        ]

        result = self._run_discovery(
            client, metadata, sample_run_id, ['good_one', 'textfile_orphan'],
        )

        by_table = {t['source_table']: t for t in result['tables']}
        assert by_table['textfile_orphan']['error_type'] == 'SOURCE_PATH_NOT_FOUND'
        # The reason must be concrete: it is what lands in tracking and the report.
        assert 'maprfs:/datalake/sales/textfile_orphan' in by_table['textfile_orphan']['error']
        assert by_table['good_one'].get('error_type') is None

    def test_genuine_discovery_failure_still_aborts_batch(self, mock_ssh_hook, sample_run_id):
        """The abort remains when the metadata is unreliable: proceeding would
        silently under-migrate."""
        _, client, _, _ = mock_ssh_hook
        metadata = [
            self._make_metadata_record('good_one'),
            self._make_metadata_record(
                'flaky',
                error='java.lang.OutOfMemoryError: GC overhead limit exceeded',
                error_type='FAILED',
            ),
        ]

        with pytest.raises(Exception, match="Discovery failed for"):
            self._run_discovery(client, metadata, sample_run_id, ['good_one', 'flaky'])


class TestRecordDiscoveredTables:

    def _setup_count(self, mock_spark, count):
        row = MagicMock()
        row.__getitem__ = lambda self, k: count
        df = MagicMock()
        df.collect.return_value = [row]
        mock_spark.sql.return_value = df

    def test_inserts_new_record(self, mock_spark, sample_discovery, mock_iceberg_retry):
        self._setup_count(mock_spark, 0)
        result = m.record_discovered_tables.function(
            discovery=sample_discovery, spark=mock_spark, ti=MagicMock(map_index=0)
        )
        assert any('INSERT INTO' in str(c) for c in mock_iceberg_retry.call_args_list)
        assert result['run_id'] == sample_discovery['run_id']

    def test_insert_covers_partition_schema_columns(self, mock_spark, sample_discovery, mock_iceberg_retry):
        """The tracking table has partition_schema_* columns, and the INSERT uses an
        explicit column list — the new columns MUST be listed, otherwise Iceberg
        rejects the write with INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA."""
        self._setup_count(mock_spark, 0)
        sample_discovery['tables'][0]['partition_schema'] = [{'name': 'dt', 'type': 'date'}]
        m.record_discovered_tables.function(
            discovery=sample_discovery, spark=mock_spark, ti=MagicMock(map_index=0)
        )
        insert_sql = next(
            c.args[1] for c in mock_iceberg_retry.call_args_list if 'INSERT INTO' in c.args[1]
        )
        for col in ('partition_schema_json', 'partition_schema_match', 'partition_schema_differences'):
            assert col in insert_sql
        # The captured source partition schema is persisted as JSON
        assert '"name": "dt"' in insert_sql and '"type": "date"' in insert_sql

    def test_insert_covers_empty_partition_names_column(self, mock_spark, sample_discovery, mock_iceberg_retry):
        """Regression: empty_partition_names was added to the tracking table's
        CREATE TABLE DDL but initially forgotten from this explicit-column-list
        INSERT, which broke record_discovered_tables in production with
        INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA."""
        self._setup_count(mock_spark, 0)
        m.record_discovered_tables.function(
            discovery=sample_discovery, spark=mock_spark, ti=MagicMock(map_index=0)
        )
        insert_sql = next(
            c.args[1] for c in mock_iceberg_retry.call_args_list if 'INSERT INTO' in c.args[1]
        )
        assert 'empty_partition_names' in insert_sql

    def test_insert_column_list_matches_values_count(self, mock_spark, sample_discovery, mock_iceberg_retry):
        """Generic guard: the explicit column list and the VALUES tuple in the
        discovery INSERT must always have the same number of entries, or Iceberg
        rejects the write. This catches any future column added to one but not
        the other, not just empty_partition_names specifically."""
        import re as _re

        def _split_top_level(s):
            """Split a SQL VALUES body on top-level commas, respecting single-quoted
            string literals (where '' is an escaped quote) and nested parens —
            values here include JSON blobs full of internal commas."""
            parts, depth, in_str, buf = [], 0, False, []
            i = 0
            while i < len(s):
                ch = s[i]
                if in_str:
                    if ch == "'" and s[i:i + 2] == "''":
                        buf.append("''")
                        i += 2
                        continue
                    if ch == "'":
                        in_str = False
                    buf.append(ch)
                elif ch == "'":
                    in_str = True
                    buf.append(ch)
                elif ch == '(':
                    depth += 1
                    buf.append(ch)
                elif ch == ')':
                    depth -= 1
                    buf.append(ch)
                elif ch == ',' and depth == 0:
                    parts.append(''.join(buf))
                    buf = []
                else:
                    buf.append(ch)
                i += 1
            if buf:
                parts.append(''.join(buf))
            return [p for p in parts if p.strip()]

        self._setup_count(mock_spark, 0)
        m.record_discovered_tables.function(
            discovery=sample_discovery, spark=mock_spark, ti=MagicMock(map_index=0)
        )
        insert_sql = next(
            c.args[1] for c in mock_iceberg_retry.call_args_list if 'INSERT INTO' in c.args[1]
        )
        cols_block = _re.search(r'INSERT INTO [\w.]+ \((.*?)\)\s*VALUES\s*\(', insert_sql, _re.S).group(1)
        values_block = _re.search(r'VALUES\s*\((.*)\)\s*$', insert_sql.strip(), _re.S).group(1)
        col_count = len([c for c in cols_block.split(',') if c.strip()])
        value_count = len(_split_top_level(values_block))
        assert col_count == value_count, (
            f"column list has {col_count} entries but VALUES has {value_count} — "
            f"they must match or Iceberg will reject the insert"
        )

    def test_updates_existing_record(self, mock_spark, sample_discovery, mock_iceberg_retry):
        self._setup_count(mock_spark, 1)
        m.record_discovered_tables.function(
            discovery=sample_discovery, spark=mock_spark, ti=MagicMock(map_index=0)
        )
        assert any('UPDATE' in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_writes_table_not_found_status(self, mock_spark, sample_discovery, mock_iceberg_retry):
        self._setup_count(mock_spark, 0)
        discovery = {
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'error': 'Table or view not found: sales_data.transactions',
                'error_type': 'TABLE_NOT_FOUND',
            }],
        }
        m.record_discovered_tables.function(
            discovery=discovery, spark=mock_spark, ti=MagicMock(map_index=0)
        )
        all_sql = ' '.join(c.args[1] for c in mock_iceberg_retry.call_args_list)
        assert "'TABLE_NOT_FOUND'" in all_sql
        assert 'Table or view not found' in all_sql

    def test_writes_database_not_found_status(self, mock_spark, sample_discovery, mock_iceberg_retry):
        self._setup_count(mock_spark, 0)
        discovery = {
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'error': "Source database 'phantom_db' does not exist on the cluster",
                'error_type': 'DATABASE_NOT_FOUND',
            }],
        }
        m.record_discovered_tables.function(
            discovery=discovery, spark=mock_spark, ti=MagicMock(map_index=0)
        )
        all_sql = ' '.join(c.args[1] for c in mock_iceberg_retry.call_args_list)
        assert "'DATABASE_NOT_FOUND'" in all_sql
        assert 'does not exist' in all_sql

    def test_writes_source_path_not_found_status(self, mock_spark, sample_discovery, mock_iceberg_retry):
        self._setup_count(mock_spark, 0)
        discovery = {
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'source_location': "maprfs:/data/sales_data/o'brien",
                'error': "Path does not exist: maprfs:/data/sales_data/o'brien",
                'error_type': 'SOURCE_PATH_NOT_FOUND',
            }],
        }
        m.record_discovered_tables.function(
            discovery=discovery, spark=mock_spark, ti=MagicMock(map_index=0)
        )
        all_sql = ' '.join(c.args[1] for c in mock_iceberg_retry.call_args_list)
        assert "'SOURCE_PATH_NOT_FOUND'" in all_sql
        assert 'Path does not exist' in all_sql
        # A quote in the location must not break out of the SQL literal. Failure rows
        # only started carrying a location in this change, so nothing pinned this.
        assert "o''brien" in all_sql
        assert "o'brien'" not in all_sql

    def test_stamps_its_map_index_for_batch_resolution(self, mock_spark, sample_discovery, mock_iceberg_retry):
        """Batches resolve their group by this index, never by list position."""
        self._setup_count(mock_spark, 0)
        ti = MagicMock()
        ti.map_index = 7
        result = m.record_discovered_tables.function(
            discovery=sample_discovery, spark=mock_spark, ti=ti
        )
        assert result['_map_index'] == 7

    def test_failed_upstream_input_returns_empty_without_an_index(self, mock_spark):
        result = m.record_discovered_tables.function(
            discovery={}, spark=mock_spark, ti=MagicMock(map_index=0)
        )
        assert result == {}


class TestRunDistcpSsh:

    def test_successful_copy_detects_incremental(self, mock_ssh_hook, sample_discovery):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (MagicMock(), _make_distcp_stdout(incremental=True), stderr)

        result = m.run_distcp_ssh.function.__wrapped__(**distcp_call(sample_discovery))
        assert result['distcp_results'][0]['status'] == 'COMPLETED'
        assert result['distcp_results'][0]['bytes_copied'] == 10485760
        assert result['distcp_results'][0]['is_incremental'] is True

    def test_skips_table_not_found_without_ssh(self, mock_ssh_hook, sample_discovery):
        hook, client, _, _ = mock_ssh_hook
        discovery = {
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'error': 'Table or view not found: sales_data.transactions',
                'error_type': 'TABLE_NOT_FOUND',
            }],
        }
        result = m.run_distcp_ssh.function.__wrapped__(**distcp_call(discovery))
        assert result['distcp_results'][0]['status'] == 'TABLE_NOT_FOUND'
        client.exec_command.assert_not_called()

    def test_skips_source_path_not_found_without_ssh(self, mock_ssh_hook, sample_discovery):
        hook, client, _, _ = mock_ssh_hook
        discovery = {
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'error': 'Path does not exist: maprfs:/data/sales_data/transactions',
                'error_type': 'SOURCE_PATH_NOT_FOUND',
            }],
        }
        result = m.run_distcp_ssh.function.__wrapped__(**distcp_call(discovery))
        assert result['distcp_results'][0]['status'] == 'SOURCE_PATH_NOT_FOUND'
        client.exec_command.assert_not_called()

    def test_skips_database_not_found_without_ssh(self, mock_ssh_hook, sample_discovery):
        hook, client, _, _ = mock_ssh_hook
        discovery = {
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'error': "Source database 'phantom_db' does not exist on the cluster",
                'error_type': 'DATABASE_NOT_FOUND',
            }],
        }
        result = m.run_distcp_ssh.function.__wrapped__(**distcp_call(discovery))
        assert result['distcp_results'][0]['status'] == 'DATABASE_NOT_FOUND'
        client.exec_command.assert_not_called()

    def test_distcp_failure_raises(self, mock_ssh_hook, sample_discovery):
        hook, client, _, _ = mock_ssh_hook
        fail_stderr = MagicMock()
        fail_stderr.read.return_value = b'DistCp failed: timeout'
        client.exec_command.return_value = (
            MagicMock(), mock_ssh_stdout(1, b'DISTCP_EXIT_CODE=1\n'), fail_stderr,
        )

        with pytest.raises(Exception, match="DistCp failed"):
            m.run_distcp_ssh.function.__wrapped__(**distcp_call(sample_discovery))

    def test_partition_filter_active_uses_per_partition_distcp(self, mock_ssh_hook, sample_discovery):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(incremental=False), stderr
        )

        filtered_discovery = {
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'partition_filter': 'dt>=2024-01-01',
                'filtered_partitions': ['dt=2024-01-01', 'dt=2024-01-02'],
                'partition_filter_active': True,
                'filtered_row_count': 500,
                'filtered_source_size_bytes': 5 * 1024 * 1024,
                'filtered_file_count': 2,
                'full_table_row_count': 1000,
                'full_table_partition_count': 2,
                'serde_properties': {},
            }],
        }
        result = m.run_distcp_ssh.function.__wrapped__(**distcp_call(filtered_discovery))
        assert result['distcp_results'][0]['status'] == 'COMPLETED'
        assert result['distcp_results'][0]['partition_filter_active'] is True

        ssh_cmd = client.exec_command.call_args[0][0]

        assert 'PATHLIST' not in ssh_cmd

        source_loc = sample_discovery['tables'][0]['source_location']
        s3_loc = sample_discovery['tables'][0]['s3_location']
        for part in ['dt=2024-01-01', 'dt=2024-01-02']:
            expected_src = f'"{source_loc}/{part}"'
            expected_dst = f'"{s3_loc}/{part}"'
            assert expected_src in ssh_cmd, f"Expected source partition path {expected_src} not found in SSH command"
            assert expected_dst in ssh_cmd, f"Expected dest partition path {expected_dst} not found in SSH command"

        assert s3_loc in ssh_cmd
        assert 'PARTITIONS_REQUESTED=2' in ssh_cmd

    def test_filtered_partitions_can_use_path_list_when_delete_not_preserved(self, mock_ssh_hook, sample_discovery):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(incremental=False), stderr
        )

        filtered_discovery = {
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'partition_filter': 'dt>=2024-01-01',
                'filtered_partitions': ['dt=2024-01-01', 'dt=2024-01-02'],
                'partition_filter_active': True,
                'filtered_row_count': 500,
                'filtered_source_size_bytes': 5 * 1024 * 1024,
                'filtered_file_count': 2,
                'full_table_row_count': 1000,
                'full_table_partition_count': 2,
                'serde_properties': {},
            }],
        }

        cfg = {**m.get_config(), 'distcp_preserve_delete': False}
        with patch.object(m, 'get_config', return_value=cfg):
            result = m.run_distcp_ssh.function.__wrapped__(**distcp_call(filtered_discovery))

        assert result['distcp_results'][0]['status'] == 'COMPLETED'

        ssh_cmd = client.exec_command.call_args[0][0]
        assert 'PATHLIST=' in ssh_cmd
        assert '-f "$PATHLIST"' in ssh_cmd
        assert '-delete' not in ssh_cmd
        assert 'dt=2024-01-01' in ssh_cmd
        assert 'dt=2024-01-02' in ssh_cmd

    def test_zero_filtered_partitions_skips_table(self, mock_ssh_hook, sample_discovery):
        """If partition_filter_active=True but filtered_partitions=[], table must be SKIPPED
        without calling SSH at all."""
        hook, client, _, _ = mock_ssh_hook

        empty_filter_discovery = {
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'partition_filter': 'dt>=2099-01-01',
                'filtered_partitions': [],
                'partition_filter_active': True,
                'filtered_row_count': 0,
                'filtered_source_size_bytes': 0,
                'filtered_file_count': 0,
                'full_table_row_count': 1000,
                'full_table_partition_count': 2,
                'serde_properties': {},
            }],
        }
        result = m.run_distcp_ssh.function.__wrapped__(**distcp_call(empty_filter_discovery))
        assert result['distcp_results'][0]['status'] == 'SKIPPED'
        client.exec_command.assert_not_called()

    def _run_with_partition_filter(self, mock_ssh_hook, sample_discovery, preserve_delete):
        """Helper: invoke run_distcp_ssh with a partition filter and return the SSH command."""
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(incremental=False), stderr
        )
        filtered_discovery = {
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'partition_filter': 'dt>=2024-01-01',
                'filtered_partitions': ['dt=2024-01-01', 'dt=2024-01-02'],
                'partition_filter_active': True,
                'filtered_row_count': 500,
                'filtered_source_size_bytes': 5 * 1024 * 1024,
                'filtered_file_count': 2,
                'full_table_row_count': 1000,
                'full_table_partition_count': 2,
                'serde_properties': {},
            }],
        }
        cfg = {**m.get_config(), 'distcp_preserve_delete': preserve_delete}
        with patch.object(m, 'get_config', return_value=cfg):
            m.run_distcp_ssh.function.__wrapped__(**distcp_call(filtered_discovery))
        return client.exec_command.call_args[0][0]

    def _assert_metrics_scoped_to_partitions(self, ssh_cmd, s3_loc, partitions):
        """Regression check (WF-201): BEFORE/AFTER S3 metrics must be summed only over the
        filtered partition paths, not the whole table location."""
        assert 'DEST_PART_PATHS=(' in ssh_cmd
        for part in partitions:
            assert f"{s3_loc}/{part}" in ssh_cmd
        assert 'sum_s3_metrics_over_paths()' in ssh_cmd
        assert 'S3_BEFORE=$(sum_s3_metrics_over_paths "${DEST_PART_PATHS[@]}")' in ssh_cmd
        assert 'S3_AFTER=$(sum_s3_metrics_over_paths "${DEST_PART_PATHS[@]}")' in ssh_cmd
        assert f'S3_BEFORE=$(calculate_s3_metrics_hadoop "{s3_loc}")' not in ssh_cmd
        assert f'S3_AFTER=$(calculate_s3_metrics_hadoop "{s3_loc}")' not in ssh_cmd

    def test_partition_filter_per_partition_scopes_s3_metrics(self, mock_ssh_hook, sample_discovery):
        ssh_cmd = self._run_with_partition_filter(
            mock_ssh_hook, sample_discovery, preserve_delete=True,
        )
        s3_loc = sample_discovery['tables'][0]['s3_location']
        self._assert_metrics_scoped_to_partitions(
            ssh_cmd, s3_loc, ['dt=2024-01-01', 'dt=2024-01-02'],
        )

    def test_partition_filter_pathlist_scopes_s3_metrics(self, mock_ssh_hook, sample_discovery):
        ssh_cmd = self._run_with_partition_filter(
            mock_ssh_hook, sample_discovery, preserve_delete=False,
        )
        s3_loc = sample_discovery['tables'][0]['s3_location']
        assert 'PATHLIST=' in ssh_cmd
        self._assert_metrics_scoped_to_partitions(
            ssh_cmd, s3_loc, ['dt=2024-01-01', 'dt=2024-01-02'],
        )

    def _run_per_partition_sized(self, mock_ssh_hook, sample_discovery,
                                 config_overrides=None):
        """Helper: per-partition copy of a 40 GiB / 300-file table across two partitions."""
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(incremental=False), stderr
        )
        filtered_discovery = {
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'partition_filter': 'dt>=2024-01-01',
                'filtered_partitions': ['dt=2024-01-01', 'dt=2024-01-02'],
                'partition_filter_active': True,
                'filtered_source_size_bytes': 40 * 1024 ** 3,
                'filtered_file_count': 300,
                'partition_file_counts': {'dt=2024-01-01': 100, 'dt=2024-01-02': 200},
                'serde_properties': {},
            }],
        }
        cfg = {**m.get_config(), 'distcp_preserve_delete': True}
        cfg.update(config_overrides or {})
        with patch.object(m, 'get_config', return_value=cfg):
            m.run_distcp_ssh.function.__wrapped__(**distcp_call(filtered_discovery))
        return client.exec_command.call_args[0][0]

    def test_per_partition_mappers_scale_with_partition_file_share(self, mock_ssh_hook,
                                                                   sample_discovery):
        ssh_cmd = self._run_per_partition_sized(
            mock_ssh_hook, sample_discovery,
            config_overrides={'distcp_mappers': '', 'distcp_bandwidth': ''},
        )
        pairs = re.findall(r'-m (\d+) -bandwidth (\d+)', ssh_cmd)
        assert len(pairs) == 2
        mapper_counts = [int(m_) for m_, _ in pairs]
        assert len(set(mapper_counts)) == 2, f"expected distinct -m values, got {pairs}"
        # dt=2024-01-01 holds 100 of the 300 files, dt=2024-01-02 holds 200, and
        # the partitions are emitted in filtered_partitions order.
        assert mapper_counts[0] < mapper_counts[1]
        assert mapper_counts == [7, 14]

    def test_per_partition_override_forces_same_values_everywhere(self, mock_ssh_hook,
                                                                 sample_discovery):
        ssh_cmd = self._run_per_partition_sized(
            mock_ssh_hook, sample_discovery,
            config_overrides={'distcp_mappers': '50', 'distcp_bandwidth': '100'},
        )
        pairs = re.findall(r'-m (\d+) -bandwidth (\d+)', ssh_cmd)
        assert len(pairs) == 2
        assert set(pairs) == {('50', '100')}

    def test_strategy_with_shell_metacharacter_is_quoted(self, mock_ssh_hook,
                                                         sample_discovery):
        ssh_cmd = self._run_per_partition_sized(
            mock_ssh_hook, sample_discovery,
            config_overrides={'distcp_strategy': 'dynamic; rm -rf /'},
        )
        assert "-strategy 'dynamic; rm -rf /'" in ssh_cmd
        assert '-strategy dynamic; rm' not in ssh_cmd

    def test_default_strategy_emits_no_quotes(self, mock_ssh_hook, sample_discovery):
        ssh_cmd = self._run_per_partition_sized(mock_ssh_hook, sample_discovery)
        assert '-strategy dynamic ' in ssh_cmd
        assert "-strategy 'dynamic'" not in ssh_cmd

    def test_empty_source_skips_distcp_and_sets_empty_source_status(self, mock_ssh_hook, sample_discovery):
        """If source has 0 files, distcp must be skipped and status set to EMPTY_SOURCE."""
        hook, client, _, _ = mock_ssh_hook

        empty_source_discovery = {
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'source_file_count': 0,
                'partition_filter_active': False,
                'filtered_file_count': 0,
            }],
        }
        result = m.run_distcp_ssh.function.__wrapped__(**distcp_call(empty_source_discovery))
        assert result['distcp_results'][0]['status'] == 'EMPTY_SOURCE'
        if client.exec_command.called:
            cmd = client.exec_command.call_args[0][0]
            assert 'distcp' not in cmd.lower(), "distcp should not be called for empty source"


class TestDistcpShellHardening:

    def _script(self, mock_ssh_hook, discovery):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(), stderr
        )
        m.run_distcp_ssh.function.__wrapped__(**distcp_call(discovery))
        return client.exec_command.call_args[0][0]

    def test_every_copy_is_wrapped_in_timeout(self, mock_ssh_hook, sample_discovery):
        script = self._script(mock_ssh_hook, sample_discovery)
        assert 'run_distcp_with_retry' in script
        assert 'timeout -k 60s' in script

    def test_timeout_is_guarded_by_command_v(self, mock_ssh_hook, sample_discovery):
        """An edge node without coreutils must degrade, not fail every copy."""
        script = self._script(mock_ssh_hook, sample_discovery)
        assert 'command -v timeout' in script

    def test_retry_helper_never_retries_a_timeout(self, mock_ssh_hook, sample_discovery):
        script = self._script(mock_ssh_hook, sample_discovery)
        assert 'rc -eq 124' in script
        assert 'not retrying' in script

    def test_metrics_helper_is_defined_once(self, mock_ssh_hook, sample_discovery):
        script = self._script(mock_ssh_hook, sample_discovery)
        assert script.count('calculate_s3_metrics_hadoop() {') == 1

    def test_call_timeout_scales_with_the_estimate(self):
        cfg = {'distcp_call_timeout_max_seconds': 21600}
        assert m._call_timeout_seconds(10.0, cfg) == 1800       # floor
        assert m._call_timeout_seconds(3000.0, cfg) == 9000     # 3x estimate
        assert m._call_timeout_seconds(100000.0, cfg) == 21600  # ceiling

    def test_the_retry_banner_never_echoes_the_command(self, mock_ssh_hook,
                                                       sample_discovery):
        """The command carries -Dfs.s3a.secret.key and the task logs the
        script's output verbatim, so no echo may interpolate it."""
        script = self._script(mock_ssh_hook, sample_discovery)
        echoes = [ln for ln in script.splitlines() if ln.strip().startswith('echo')]
        assert echoes
        assert not [ln for ln in echoes if '$*' in ln or '"$@"' in ln]

    def test_ssh_read_window_outlasts_every_retry_the_shell_may_make(self):
        """Tighter than the blanket 24h, but never tighter than the shell's own
        bound: giving up first abandons a live DistCp whose YARN id was never
        read, so nothing kills the orphan before the retry starts a second
        writer."""
        assert m._ssh_read_timeout(1800) == 1800 * 3 + 1800
        assert m._ssh_read_timeout(1800) < m.SSH_COMMAND_TIMEOUT
        assert m._ssh_read_timeout(1800) > 1800 * m._DISTCP_RETRY_ATTEMPTS
        # The blanket constant stays the ceiling, so the override only tightens.
        assert m._ssh_read_timeout(10 ** 9) == m.SSH_COMMAND_TIMEOUT

    def test_the_shell_makes_exactly_the_attempts_the_window_allows_for(
        self, mock_ssh_hook, sample_discovery
    ):
        script = self._script(mock_ssh_hook, sample_discovery)
        assert f'max_attempts={m._DISTCP_RETRY_ATTEMPTS}' in script

    def test_the_copy_gets_a_bounded_read_window_not_the_blanket_one(
        self, mock_ssh_hook, sample_discovery
    ):
        hook, client, _, _ = mock_ssh_hook
        self._script(mock_ssh_hook, sample_discovery)
        passed = client.exec_command.call_args.kwargs['timeout']
        assert passed != m.SSH_COMMAND_TIMEOUT
        assert passed == m._ssh_read_timeout(m._call_timeout_seconds(
            m.estimate_distcp_cost(sample_discovery['tables'][0], m.get_config()),
            m.get_config(),
        ))

    def test_the_empty_source_mkdir_gets_a_bounded_read_window_too(
        self, mock_ssh_hook, sample_discovery
    ):
        hook, client, _, _ = mock_ssh_hook
        m.run_distcp_ssh.function.__wrapped__(**distcp_call({
            **sample_discovery,
            'tables': [{
                **sample_discovery['tables'][0],
                'source_file_count': 0,
                'partition_filter_active': False,
                'filtered_file_count': 0,
            }],
        }))
        passed = client.exec_command.call_args.kwargs['timeout']
        assert passed != m.SSH_COMMAND_TIMEOUT
        assert 0 < passed <= m._ssh_read_timeout(m._CALL_TIMEOUT_FLOOR_SECONDS)

    def test_sizing_uses_the_shared_effective_metrics_helper(
        self, mock_ssh_hook, sample_discovery, caplog
    ):
        """Pins the (size, files) unpacking order — a swap would size every
        filtered copy from its file count."""
        d = _partitioned_discovery(sample_discovery)
        d['tables'][0]['filtered_source_size_bytes'] = 2 * 1024 * 1024
        d['tables'][0]['filtered_file_count'] = 20
        with caplog.at_level(logging.INFO):
            self._script(mock_ssh_hook, d)
        assert '2097152 bytes / 20 files' in caplog.text

    def test_timed_out_copy_kills_its_yarn_application(
        self, mock_ssh_hook, sample_discovery
    ):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        stdout = mock_ssh_stdout(124, (
            'Submitted application application_1700000000000_0042\n'
            '  [DistCp] TIMED OUT after 1800s — not retrying\n'
        ).encode())
        client.exec_command.return_value = (MagicMock(), stdout, stderr)

        # A single failing table makes run_distcp_ssh raise after recording the
        # per-table result (see test_distcp_failure_raises) — the result dict
        # itself was already pushed to XCom by then, so read it back from there.
        kwargs = distcp_call(sample_discovery)
        with pytest.raises(Exception, match="DistCp failed"):
            m.run_distcp_ssh.function.__wrapped__(**kwargs)

        result = kwargs['ti'].xcom_push.call_args.kwargs['value']
        assert result['distcp_results'][0]['status'] == 'FAILED'
        kill_calls = [
            c for c in client.exec_command.call_args_list
            if 'application -kill' in str(c)
        ]
        assert kill_calls, 'expected a yarn application -kill after a timeout'
        assert 'application_1700000000000_0042' in str(kill_calls[0])

    def test_non_timeout_failure_does_not_kill_the_application(
        self, mock_ssh_hook, sample_discovery
    ):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        stdout = mock_ssh_stdout(1, b'Submitted application application_1_0001\nboom\n')
        client.exec_command.return_value = (MagicMock(), stdout, stderr)

        with pytest.raises(Exception, match="DistCp failed"):
            m.run_distcp_ssh.function.__wrapped__(**distcp_call(sample_discovery))

        assert not [
            c for c in client.exec_command.call_args_list
            if 'application -kill' in str(c)
        ]

    def _kill(self, exit_code, output=b'', error=b''):
        ssh = MagicMock()
        client = MagicMock()
        ssh.get_conn.return_value.__enter__ = MagicMock(return_value=client)
        ssh.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        stdout = mock_ssh_stdout(exit_code, output)
        stderr = MagicMock()
        stderr.read.return_value = error
        client.exec_command.return_value = (MagicMock(), stdout, stderr)
        m._kill_yarn_apps(ssh, ['application_1_0001'], {}, 'db.tbl')
        return client

    def test_a_failed_kill_is_logged_as_an_error(self, caplog):
        """The only observability on the one failure mode with data-safety
        stakes: an orphan writing under the same prefix as the retry."""
        with caplog.at_level(logging.DEBUG):
            self._kill(1, error=b'Permission denied: user=airflow')
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert errors
        assert 'application_1_0001' in errors[0].message
        assert 'Permission denied' in errors[0].message

    def test_an_already_finished_app_is_not_reported_as_a_failed_kill(self, caplog):
        """Every scraped id is passed, so this happens routinely — it must not
        read like the orphan case."""
        with caplog.at_level(logging.DEBUG):
            self._kill(255, output=b'Application application_1_0001 has already finished')
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert 'already finished' in caplog.text

    def test_a_successful_kill_logs_no_error(self, caplog):
        with caplog.at_level(logging.DEBUG):
            self._kill(0)
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert 'killed YARN app(s)' in caplog.text

    def test_a_kill_that_raises_never_propagates(self, caplog):
        """It runs on a path that is already failing."""
        ssh = MagicMock()
        ssh.get_conn.side_effect = OSError('socket closed')
        with caplog.at_level(logging.DEBUG):
            m._kill_yarn_apps(ssh, ['application_1_0001'], {}, 'db.tbl')
        assert [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_logs_estimate_against_actual(self, mock_ssh_hook, sample_discovery, caplog):
        """The client guide tells users to grep for this line."""
        with caplog.at_level(logging.INFO):
            self._script(mock_ssh_hook, sample_discovery)
        assert 'cost estimate' in caplog.text
        assert 'vs actual' in caplog.text

    def test_the_hard_stop_is_measured_from_the_batch_start(
        self, mock_ssh_hook, sample_discovery
    ):
        offset = _hard_stop_offset(self._script(mock_ssh_hook, sample_discovery))
        assert _HARD_STOP - 60 < offset <= _HARD_STOP
        # Past the longest timeout any copy can have, so an early copy is never cut short.
        assert offset > int(m.get_config()['distcp_call_timeout_max_seconds'])

    def test_a_table_started_late_gets_only_what_is_left_of_the_window(
        self, mock_ssh_hook, sample_discovery
    ):
        seven_hours = 7 * 3600
        starts = iter([0.0])  # batch_started_at, then every later reading is 7h on
        clock = SimpleNamespace(time=m.time.time,
                                monotonic=lambda: next(starts, seven_hours))
        with patch.object(m, 'time', clock):
            script = self._script(mock_ssh_hook, sample_discovery)
        assert _hard_stop_offset(script) == int(_HARD_STOP - seven_hours)


_HAS_SHELL = bool(shutil.which('bash') and shutil.which('timeout'))
# Every copy attempt must have ended by here: execution_timeout less the
# allowance for the post-copy scans and reporting.
_HARD_STOP = m._DISTCP_EXECUTION_TIMEOUT.total_seconds() - m._SSH_READ_SLACK_SECONDS


def _hard_stop_offset(script):
    return int(re.search(
        r'DISTCP_HARD_STOP_EPOCH=\$\(\( \$\(date \+%s\) \+ (-?\d+) \)\)', script
    )[1])


def _run_retry_helper(elapsed_secs, command, delay=None):
    """Run the rendered run_distcp_with_retry against a stand-in for hadoop distcp."""
    prelude = m._distcp_shell_prelude('', 0, elapsed_secs=elapsed_secs)
    if delay is not None:
        prelude = prelude.replace('local delay=30', f'local delay={delay}')
    proc = subprocess.run(
        ['bash', '-c', f'set -e\n{prelude}\nrun_distcp_with_retry 30 {command}'],
        capture_output=True, text=True, timeout=30,
    )
    return proc.returncode, proc.stdout


@pytest.mark.skipif(not _HAS_SHELL, reason='needs bash and coreutils timeout')
class TestHardStopBeforeExecutionTimeout:
    """A copy started late in a batch must end before execution_timeout, whose
    kill discards the batch's XCom and leaves the copy's YARN job writing while
    the retry starts a second one under -update -delete."""

    def test_an_attempt_is_cut_off_at_the_hard_stop(self):
        rc, out = _run_retry_helper(_HARD_STOP - 2, 'sleep 20')
        assert rc == 124, out

    def test_no_attempt_starts_once_the_hard_stop_has_passed(self, tmp_path):
        marker = tmp_path / 'ran'
        rc, out = _run_retry_helper(_HARD_STOP + 5, f'touch {marker}')
        assert rc == 124
        assert not marker.exists()
        assert 'not starting' in out

    def test_a_retry_gets_only_what_is_left_before_the_hard_stop(self):
        # A genuine failure, not a timeout, so it would normally be retried twice.
        rc, out = _run_retry_helper(_HARD_STOP - 3, "sh -c 'sleep 2; exit 1'", delay=0)
        assert rc == 124, out
        assert 'Attempt 3/' not in out

    def test_a_copy_started_early_keeps_its_full_timeout(self):
        rc, out = _run_retry_helper(0, 'true')
        assert rc == 0
        assert 'Attempt 1/3 (timeout 30s)' in out


class TestDistcpBatchResolution:

    def test_copies_only_the_tables_in_its_batch(self, mock_ssh_hook, sample_discovery):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(), stderr
        )
        two = {
            **sample_discovery,
            'tables': [
                sample_discovery['tables'][0],
                {**sample_discovery['tables'][0], 'source_table': 'other'},
            ],
        }
        result = m.run_distcp_ssh.function.__wrapped__(
            **distcp_call(two, table_names=['other'])
        )
        assert [t['source_table'] for t in result['tables']] == ['other']
        assert [r['source_table'] for r in result['distcp_results']] == ['other']

    def test_pulls_its_group_by_map_index(self, mock_ssh_hook, sample_discovery):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(), stderr
        )
        kwargs = distcp_call({**sample_discovery, '_map_index': 5})
        m.run_distcp_ssh.function.__wrapped__(**kwargs)
        kwargs['ti'].xcom_pull.assert_called_once_with(
            task_ids='record_discovered_tables', map_indexes=5
        )

    def test_raises_when_the_pulled_group_is_a_different_database(
        self, mock_ssh_hook, sample_discovery
    ):
        """A shifted map index must fail loudly, not copy the wrong tables."""
        other = {**sample_discovery, 'source_database': 'someone_else'}
        with pytest.raises(ValueError, match='refusing to copy'):
            m.run_distcp_ssh.function.__wrapped__(
                **distcp_call(sample_discovery, pulled=other)
            )

    def test_raises_when_the_group_xcom_is_missing(self, mock_ssh_hook, sample_discovery):
        with pytest.raises(ValueError, match='could not read group discovery'):
            m.run_distcp_ssh.function.__wrapped__(
                **distcp_call(sample_discovery, pulled={})
            )

    def test_invalid_descriptor_is_skipped_not_raised(self, mock_ssh_hook):
        assert m.run_distcp_ssh.function.__wrapped__(
            batch={}, cluster_setup={'temp_dir': '/tmp', 'run_id': 'r'},
            source_task_id='record_discovered_tables', ti=MagicMock(),
        ) == {}

    def test_matches_keys_when_partition_filter_is_none(
        self, mock_ssh_hook, sample_discovery
    ):
        """Discovery rows carry None; descriptors carry ''. They must still match."""
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(), stderr
        )
        d = {
            **sample_discovery,
            'tables': [{**sample_discovery['tables'][0], 'partition_filter': None}],
        }
        result = m.run_distcp_ssh.function.__wrapped__(**distcp_call(d))
        assert len(result['tables']) == 1

    def test_does_not_cross_match_two_slices_of_one_table(
        self, mock_ssh_hook, sample_discovery
    ):
        """Multi-slice runs hold several rows per table, differing only by filter."""
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(), stderr
        )
        base = sample_discovery['tables'][0]
        d = {
            **sample_discovery,
            'tables': [
                {**base, 'partition_filter': 'd=2023'},
                {**base, 'partition_filter': 'd=2024'},
            ],
        }
        kwargs = distcp_call(d)
        kwargs['batch']['table_keys'] = [[base['source_table'], 'd=2024']]
        result = m.run_distcp_ssh.function.__wrapped__(**kwargs)
        assert len(result['tables']) == 1
        assert result['tables'][0]['partition_filter'] == 'd=2024'


class TestDistcpSoftDeadline:

    def test_tables_not_started_before_the_budget_expires_are_failed(
        self, mock_ssh_hook, sample_discovery, monkeypatch
    ):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(), stderr
        )
        base = sample_discovery['tables'][0]
        d = {
            **sample_discovery,
            'tables': [
                base,
                {**base, 'source_table': 'second'},
                {**base, 'source_table': 'third'},
            ],
        }
        # Clock jumps past the budget after the first table. Never sleep:
        # pytest.ini sets --timeout=60.
        ticks = iter([0.0, 0.0, 10_000.0, 10_000.0, 10_000.0, 10_000.0])
        monkeypatch.setattr(m.time, 'monotonic', lambda: next(ticks, 10_000.0))

        # A budget skip is still a FAILED result, so run_distcp_ssh raises
        # after the XCom push — same as any other per-table failure — so the
        # final attempt's tracking is complete and Airflow retries the batch
        # (see test_timed_out_copy_kills_its_yarn_application). Read the
        # result back from the push rather than a normal return value.
        kwargs = distcp_call(d, budget=100.0)
        with pytest.raises(Exception, match="DistCp failed"):
            m.run_distcp_ssh.function.__wrapped__(**kwargs)

        result = kwargs['ti'].xcom_push.call_args.kwargs['value']
        by_table = {r['source_table']: r for r in result['distcp_results']}
        assert by_table[base['source_table']]['status'] == 'COMPLETED'
        assert by_table['second']['status'] == 'FAILED'
        assert 'budget exhausted' in by_table['second']['error']
        assert by_table['third']['status'] == 'FAILED'

    def test_every_remaining_table_still_gets_a_result_row(
        self, mock_ssh_hook, sample_discovery, monkeypatch
    ):
        """Rows are what make the failure visible in tracking."""
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(), stderr
        )
        base = sample_discovery['tables'][0]
        d = {
            **sample_discovery,
            'tables': [{**base, 'source_table': f't{i}'} for i in range(4)],
        }
        # Monotonically increasing rather than a fixed value: a truly frozen
        # clock can never show elapsed time exceeding the budget. Never sleep:
        # pytest.ini sets --timeout=60.
        ticks = itertools.count(10_000.0)
        monkeypatch.setattr(m.time, 'monotonic', lambda: next(ticks))

        # All 4 are budget skips, still FAILED results, so run_distcp_ssh
        # raises after the XCom push (see
        # test_tables_not_started_before_the_budget_expires_are_failed) — read
        # the result back from the push rather than a normal return value.
        kwargs = distcp_call(d, budget=1.0)
        with pytest.raises(Exception, match="DistCp failed"):
            m.run_distcp_ssh.function.__wrapped__(**kwargs)

        result = kwargs['ti'].xcom_push.call_args.kwargs['value']
        assert len(result['distcp_results']) == 4
        assert result['_has_failures'] is True

    def test_zero_budget_disables_the_deadline(
        self, mock_ssh_hook, sample_discovery
    ):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(), stderr
        )
        result = m.run_distcp_ssh.function.__wrapped__(
            **distcp_call(sample_discovery, budget=0.0)
        )
        assert result['distcp_results'][0]['status'] == 'COMPLETED'

    def test_partition_copies_are_guarded_by_the_in_shell_deadline(
        self, mock_ssh_hook, sample_discovery
    ):
        """A 300-partition table must honour the budget between partitions."""
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(), stderr
        )
        base = sample_discovery['tables'][0]
        d = {
            **sample_discovery,
            'tables': [{
                **base,
                'partition_filter_active': True,
                'partition_filter': 'd>=2024',
                'filtered_partitions': ['d=2024', 'd=2025'],
                'partition_file_counts': {'d=2024': 5, 'd=2025': 5},
                'filtered_file_count': 10,
                'filtered_source_size_bytes': 1024 ** 3,
            }],
        }
        m.run_distcp_ssh.function.__wrapped__(**distcp_call(d, budget=3600.0))
        script = client.exec_command.call_args[0][0]
        assert 'deadline_ok' in script
        assert script.count('if deadline_ok; then') == 2

    def test_deadline_epoch_is_zero_when_there_is_no_budget(
        self, mock_ssh_hook, sample_discovery
    ):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(), stderr
        )
        m.run_distcp_ssh.function.__wrapped__(
            **distcp_call(sample_discovery, budget=0.0)
        )
        assert 'DISTCP_DEADLINE_EPOCH=0' in client.exec_command.call_args[0][0]

    def test_skipped_partitions_are_counted_and_reported_back(
        self, mock_ssh_hook, sample_discovery
    ):
        """The in-shell deadline must be visible to Python.

        The per-partition script exits 0 whether or not the deadline cut the
        copy short, so without this counter an incomplete table is recorded
        COMPLETED.
        """
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), _make_distcp_stdout(), stderr
        )
        m.run_distcp_ssh.function.__wrapped__(
            **distcp_call(_partitioned_discovery(sample_discovery), budget=3600.0)
        )
        script = client.exec_command.call_args[0][0]

        assert 'DEADLINE_SKIPPED=0' in script
        assert script.count('DEADLINE_SKIPPED=$((DEADLINE_SKIPPED + 1))') == 2

        metrics = script.split('===DISTCP_METRICS_START===')[1].split(
            '===DISTCP_METRICS_END==='
        )[0]
        assert 'DEADLINE_SKIPPED_PARTITIONS=$DEADLINE_SKIPPED' in metrics

    def test_a_deadline_skipped_partition_fails_the_table(
        self, mock_ssh_hook, sample_discovery, caplog
    ):
        """A partial copy must not be recorded COMPLETED.

        FAILED is what makes has_failures true, which raises after the XCom
        push so Airflow retries the batch with a fresh budget.
        """
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        stdout = mock_ssh_stdout(0, (
            "===DISTCP_METRICS_START===\n"
            "INCREMENTAL=false\n"
            "BYTES_COPIED=0\nFILES_COPIED=0\n"
            "S3_FILE_COUNT_BEFORE=0\nS3_TOTAL_SIZE_BEFORE=0\n"
            "S3_FILE_COUNT_AFTER=5\nS3_TOTAL_SIZE_AFTER=10485760\n"
            "S3_FILES_TRANSFERRED=5\nS3_BYTES_TRANSFERRED=10485760\n"
            "DEADLINE_SKIPPED_PARTITIONS=1\n"
            "===DISTCP_METRICS_END===\n"
        ).encode())
        client.exec_command.return_value = (MagicMock(), stdout, stderr)

        # A FAILED table makes run_distcp_ssh raise after pushing its result
        # (see test_timed_out_copy_kills_its_yarn_application) — read the
        # result back from the push.
        kwargs = distcp_call(_partitioned_discovery(sample_discovery), budget=3600.0)
        with caplog.at_level(logging.WARNING), \
             pytest.raises(Exception, match="DistCp failed"):
            m.run_distcp_ssh.function.__wrapped__(**kwargs)

        row = kwargs['ti'].xcom_push.call_args.kwargs['value']['distcp_results'][0]
        assert row['status'] == 'FAILED'
        assert row['error'] == m._BUDGET_PARTIAL_ERROR
        # The counts the copy did manage are still accurate and worth keeping.
        assert row['s3_files_transferred'] == 5
        assert row['distcp_started_at']
        assert 'transactions' in caplog.text
        assert '1 partition' in caplog.text

    def test_zero_skipped_partitions_still_completes(
        self, mock_ssh_hook, sample_discovery
    ):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        stdout = mock_ssh_stdout(0, (
            "===DISTCP_METRICS_START===\n"
            "INCREMENTAL=false\nBYTES_COPIED=0\nFILES_COPIED=0\n"
            "S3_FILE_COUNT_BEFORE=0\nS3_TOTAL_SIZE_BEFORE=0\n"
            "S3_FILE_COUNT_AFTER=5\nS3_TOTAL_SIZE_AFTER=10485760\n"
            "S3_FILES_TRANSFERRED=5\nS3_BYTES_TRANSFERRED=10485760\n"
            "DEADLINE_SKIPPED_PARTITIONS=0\n"
            "===DISTCP_METRICS_END===\n"
        ).encode())
        client.exec_command.return_value = (MagicMock(), stdout, stderr)

        result = m.run_distcp_ssh.function.__wrapped__(
            **distcp_call(_partitioned_discovery(sample_discovery), budget=3600.0)
        )
        assert result['distcp_results'][0]['status'] == 'COMPLETED'
        assert result['distcp_results'][0]['error'] is None


class TestUpdateDistcpStatus:

    def test_sets_copied_on_success(self, mock_spark, sample_distcp_result, mock_iceberg_retry):
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        assert any('COPIED' in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_sets_failed_on_error(self, mock_spark, sample_distcp_result, mock_iceberg_retry):
        sample_distcp_result['distcp_results'][0]['status'] = 'FAILED'
        sample_distcp_result['distcp_results'][0]['error'] = 'Network error'
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        assert any('FAILED' in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_budget_skip_row_survives_the_tracking_update(
        self, mock_spark, sample_distcp_result, mock_iceberg_retry
    ):
        """A budget-skip row (run_distcp_ssh's soft-deadline guard) is FAILED,
        not SKIPPED, so it is not in update_distcp_status's skip-list and
        reaches code that indexes several fields directly rather than via
        .get(). A row missing any of them must not reach here — this pins the
        exact shape the guard is required to produce."""
        sample_distcp_result['distcp_results'][0] = {
            'source_database': 'sales_data', 'source_table': 'transactions',
            'dest_database': 'sales_data_s3',
            'status': 'FAILED',
            'distcp_started_at': '2025-01-01 12:00:00',
            'distcp_completed_at': '2025-01-01 12:00:00',
            'distcp_duration_secs': 0.0,
            'is_incremental': False,
            'bytes_copied': 0, 'files_copied': 0,
            's3_total_size_bytes_before': 0, 's3_file_count_before': 0,
            's3_total_size_bytes_after': 0, 's3_file_count_after': 0,
            's3_bytes_transferred': 0, 's3_files_transferred': 0,
            'partition_filter_active': False, 'partitions_requested': None,
            'empty_partitions': [],
            'error': m._BUDGET_EXHAUSTED_ERROR,
            'yarn_application_id': None, 'yarn_application_ids': [],
            'partition_filter': None,
        }
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        sql_calls = ' '.join(str(c) for c in mock_iceberg_retry.call_args_list)
        assert "distcp_status = 'FAILED'" in sql_calls
        assert 'budget exhausted' in sql_calls

    def test_empty_partition_names_written_to_tracking_update(
        self, mock_spark, sample_distcp_result, mock_iceberg_retry
    ):
        """Empty partition names captured by run_distcp_ssh must be persisted
        into the tracking table's empty_partition_names column."""
        sample_distcp_result['distcp_results'][0]['empty_partitions'] = [
            'dt=2024-01-02', 'dt=2024-01-03',
        ]
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        sql_calls = ' '.join(str(c) for c in mock_iceberg_retry.call_args_list)
        assert 'dt=2024-01-02' in sql_calls and 'dt=2024-01-03' in sql_calls
        assert 'empty_partition_names' in sql_calls

    def test_no_empty_partitions_writes_null_to_tracking_update(
        self, mock_spark, sample_distcp_result, mock_iceberg_retry
    ):
        """When nothing is empty, empty_partition_names must be set to NULL."""
        sample_distcp_result['distcp_results'][0]['empty_partitions'] = []
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        sql_calls = [str(c) for c in mock_iceberg_retry.call_args_list]
        assert any('empty_partition_names = NULL' in c for c in sql_calls)

    def test_skipped_row_gets_no_per_table_update(
        self, mock_spark, sample_distcp_result, mock_iceberg_retry
    ):
        """A table skipped at discovery already has its final status; the per-table
        UPDATE must not run for it and overwrite that with COPIED/FAILED."""
        sample_distcp_result['distcp_results'][0]['status'] = 'SOURCE_PATH_NOT_FOUND'
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        # Key on the per-table UPDATE's own task_label. Matching on the emitted
        # distcp_status would be vacuous: the UPDATE writes r['status'] verbatim, so
        # a skipped row that slipped through would write SOURCE_PATH_NOT_FOUND, not
        # COPIED/FAILED, and any such filter matches nothing either way.
        labels = [c.kwargs.get('task_label') for c in mock_iceberg_retry.call_args_list]
        assert 'update_distcp_status:transactions' not in labels

    def test_failure_update_preserves_skippable_statuses(
        self, mock_spark, sample_distcp_result, mock_iceberg_retry
    ):
        """The guard is f-string interpolated, so assert it reaches the statement
        that needs it."""
        sample_distcp_result['distcp_results'][0]['status'] = 'FAILED'
        sample_distcp_result['distcp_results'][0]['error'] = 'Network error'
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        labels = assert_each_overall_status_case_preserves_skippable(
            mock_iceberg_retry.call_args_list
        )
        assert 'update_distcp_status:failure_patch:transactions' in labels

    def test_catchall_preserves_skippable_statuses(
        self, mock_spark, sample_distcp_result, mock_iceberg_retry
    ):
        """The catchall for tables distcp never reported on carries its own copy of
        the guard, and the failure-patch test above cannot reach it."""
        sample_distcp_result['distcp_results'] = []
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        labels = assert_each_overall_status_case_preserves_skippable(
            mock_iceberg_retry.call_args_list
        )
        assert any(label.startswith('update_distcp:catchall:') for label in labels)

    def test_normalizes_zero_row_completed_to_empty_source(
        self, mock_spark, sample_distcp_result, mock_iceberg_retry
    ):
        """A COMPLETED distcp that moved zero bytes/files against a source with
        zero rows must be flipped to EMPTY_SOURCE so the report converges on a
        single tracking state regardless of stray marker files."""
        sample_distcp_result['tables'][0]['row_count'] = 0
        sample_distcp_result['distcp_results'][0].update({
            'status': 'COMPLETED', 'bytes_copied': 0, 'files_copied': 0,
            's3_file_count_after': 0, 's3_total_size_bytes_after': 0,
        })
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        sql_calls = ' '.join(str(c) for c in mock_iceberg_retry.call_args_list)
        assert "distcp_status = 'EMPTY_SOURCE'" in sql_calls
        assert "overall_status = 'EMPTY_SOURCE'" in sql_calls

    def test_does_not_normalize_when_source_has_rows(
        self, mock_spark, sample_distcp_result, mock_iceberg_retry
    ):
        """A COMPLETED distcp with zero bytes/files but non-zero source row
        count is NOT EMPTY_SOURCE — it's an incremental no-op against a real
        table. Reporting it as EMPTY_SOURCE would lose information."""
        sample_distcp_result['tables'][0]['row_count'] = 1000
        sample_distcp_result['distcp_results'][0].update({
            'status': 'COMPLETED', 'bytes_copied': 0, 'files_copied': 0,
        })
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        sql_calls = ' '.join(str(c) for c in mock_iceberg_retry.call_args_list)
        assert "distcp_status = 'EMPTY_SOURCE'" not in sql_calls
        # The main per-row UPDATE must still have fired with the original status.
        assert "distcp_status = 'COMPLETED'" in sql_calls

    def test_normalization_keys_match_per_partition_filter_slice(
        self, mock_spark, sample_discovery, mock_iceberg_retry
    ):
        """Two discovery slices for the same (src,table,dest) differing only by
        partition_filter must each be matched to their own distcp result —
        the key includes partition_filter."""
        base_t = sample_discovery['tables'][0]
        empty_slice = dict(base_t)
        empty_slice.update({
            'partition_filter': "dt='2025-01-02'",
            'partition_filter_active': True,
            'filtered_partitions': ['dt=2025-01-02'],
            'filtered_row_count': 0,
            'row_count': 0,
        })
        nonempty_slice = dict(base_t)
        nonempty_slice.update({
            'partition_filter': "dt='2025-01-01'",
            'partition_filter_active': True,
            'filtered_partitions': ['dt=2025-01-01'],
            'filtered_row_count': 1000,
            'row_count': 1000,
        })
        distcp_result = {
            **sample_discovery,
            'tables': [empty_slice, nonempty_slice],
            'distcp_results': [
                {
                    'source_database': 'sales_data', 'source_table': 'transactions',
                    'dest_database': 'sales_data_s3',
                    'partition_filter': "dt='2025-01-02'",
                    'status': 'COMPLETED',
                    'distcp_started_at': '2025-01-01 12:00:00',
                    'distcp_completed_at': '2025-01-01 12:00:10',
                    'distcp_duration_secs': 10.0, 'is_incremental': True,
                    'bytes_copied': 0, 'files_copied': 0,
                    's3_total_size_bytes_before': 0, 's3_file_count_before': 0,
                    's3_total_size_bytes_after': 0, 's3_file_count_after': 0,
                    's3_bytes_transferred': 0, 's3_files_transferred': 0,
                    'partition_filter_active': True, 'partitions_requested': 1,
                    'error': None,
                },
                {
                    'source_database': 'sales_data', 'source_table': 'transactions',
                    'dest_database': 'sales_data_s3',
                    'partition_filter': "dt='2025-01-01'",
                    'status': 'COMPLETED',
                    'distcp_started_at': '2025-01-01 12:00:00',
                    'distcp_completed_at': '2025-01-01 12:05:00',
                    'distcp_duration_secs': 300.0, 'is_incremental': False,
                    'bytes_copied': 10 * 1024 * 1024, 'files_copied': 5,
                    's3_total_size_bytes_before': 0, 's3_file_count_before': 0,
                    's3_total_size_bytes_after': 10 * 1024 * 1024, 's3_file_count_after': 5,
                    's3_bytes_transferred': 10 * 1024 * 1024, 's3_files_transferred': 5,
                    'partition_filter_active': True, 'partitions_requested': 1,
                    'error': None,
                },
            ],
            '_task_duration': 310.0,
        }
        m.update_distcp_status.function(distcp_result=distcp_result, spark=mock_spark)
        # Only the empty slice should be flipped to EMPTY_SOURCE; the
        # non-empty slice's UPDATE must still write distcp_status='COMPLETED'.
        # Without partition_filter in the key, the dict comprehension would
        # collapse both slices onto whichever entry came last, and both
        # distcp_results would see the same row_count.
        sql_calls = ' '.join(str(c) for c in mock_iceberg_retry.call_args_list)
        assert "distcp_status = 'EMPTY_SOURCE'" in sql_calls
        assert "distcp_status = 'COMPLETED'" in sql_calls

    def test_catchall_preserves_empty_source_overall_status(
        self, mock_spark, sample_distcp_result, mock_iceberg_retry
    ):
        """The catchall UPDATE that fires for tables not processed by distcp
        must NOT overwrite overall_status='EMPTY_SOURCE' with 'FAILED'."""
        # Empty distcp_results so the catchall fires for every table in the slot.
        sample_distcp_result['distcp_results'] = []
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        catchall_sqls = [
            str(c) for c in mock_iceberg_retry.call_args_list
            if "S3 copy task did not process this table" in str(c)
        ]
        assert catchall_sqls, "catchall UPDATE should have fired"
        joined = ' '.join(catchall_sqls)
        assert "overall_status='FAILED'" not in joined
        assert "WHEN overall_status = 'EMPTY_SOURCE'" in joined


class TestReconcileUnprocessedTables:

    def test_marks_tables_no_batch_ever_processed(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [{'cnt': 3}]
        result = m.reconcile_unprocessed_tables.function(
            run_id='run-1', spark=mock_spark
        )
        assert result == {'run_id': 'run-1', 'unprocessed': 3}
        statements = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'distcp_status IS NULL' in statements

    def test_is_a_no_op_when_every_table_has_a_status(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [{'cnt': 0}]
        result = m.reconcile_unprocessed_tables.function(
            run_id='run-1', spark=mock_spark
        )
        assert result['unprocessed'] == 0
        # Only the counting SELECT ran — no UPDATE.
        assert mock_spark.sql.call_count == 1

    def test_preserves_skippable_and_empty_source_statuses(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [{'cnt': 1}]
        m.reconcile_unprocessed_tables.function(run_id='run-1', spark=mock_spark)
        statements = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'EMPTY_SOURCE' in statements
        assert 'TABLE_NOT_FOUND' in statements

    def test_only_touches_tables_that_finished_discovery(self, mock_spark):
        mock_spark.sql.return_value.collect.return_value = [{'cnt': 1}]
        m.reconcile_unprocessed_tables.function(run_id='run-1', spark=mock_spark)
        statements = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert "discovery_status = 'COMPLETED'" in statements


class TestCreateHiveTables:

    def test_creates_new_table(self, mock_spark, sample_distcp_result):
        mock_spark.sql.side_effect = [None, Exception("Not found"), None, None, None]
        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=sample_distcp_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['table_results'][0]['status'] == 'COMPLETED'
        assert result['table_results'][0]['existed'] is False

    def test_a_concurrent_database_create_is_tolerated(self, mock_spark,
                                                       sample_distcp_result):
        """Several batches from one source database now run at once, so they
        race on CREATE DATABASE IF NOT EXISTS."""
        mock_spark.sql.side_effect = [
            Exception("AlreadyExistsException(message:Database sales_data_s3 "
                      "already exists)"),
            Exception("Not found"), None, None, None,
        ]
        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=sample_distcp_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['table_results'][0]['status'] == 'COMPLETED'

    def test_a_real_database_create_failure_still_propagates(self, mock_spark,
                                                             sample_distcp_result):
        mock_spark.sql.side_effect = Exception("Permission denied: user=airflow")
        with pytest.raises(Exception, match="Permission denied"):
            m.create_hive_tables.function.__wrapped__(
                distcp_result=sample_distcp_result, spark=mock_spark, ti=MagicMock(),
            )

    def test_skips_source_path_not_found(self, mock_spark, sample_distcp_result):
        sample_distcp_result['tables'][0].update({
            'error': 'Path does not exist: maprfs:/data/sales_data/transactions',
            'error_type': 'SOURCE_PATH_NOT_FOUND',
        })
        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=sample_distcp_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['table_results'][0]['status'] == 'SOURCE_PATH_NOT_FOUND'
        assert result['table_results'][0]['action'] == 'skipped_not_found'
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).upper()
        assert 'CREATE EXTERNAL TABLE' not in all_sql

    def test_partition_column_type_from_partition_schema(self, mock_spark, sample_distcp_result):
        """A date partition column must be emitted as `dt` date in PARTITIONED BY,
        not defaulted to STRING. Regression test: partition-column types were never
        captured in discovery, so create_hive_tables hardcoded every partition
        column to STRING (source date -> dest string)."""
        sample_distcp_result['tables'][0]['partition_schema'] = [
            {'name': 'dt', 'type': 'date'},
        ]

        sql_calls = []

        def recording_sql(sql):
            sql_calls.append(sql)
            if sql.strip().upper().startswith('DESCRIBE') and 'FORMATTED' not in sql.upper():
                raise Exception("Table not found")
            df = MagicMock()
            df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = recording_sql

        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=sample_distcp_result, spark=mock_spark, ti=MagicMock(),
        )

        assert result['table_results'][0]['status'] == 'COMPLETED'
        create_ddl = next(s for s in sql_calls if 'CREATE EXTERNAL TABLE' in s.upper())
        assert '`dt` date' in create_ddl
        assert '`dt` STRING' not in create_ddl

    def test_partition_column_defaults_to_string_without_type(self, mock_spark, sample_distcp_result):
        """Fallback safety: if no partition_schema/schema type is available for a
        partition column, it still defaults to STRING (unchanged behavior)."""
        sample_distcp_result['tables'][0].pop('partition_schema', None)

        sql_calls = []

        def recording_sql(sql):
            sql_calls.append(sql)
            if sql.strip().upper().startswith('DESCRIBE') and 'FORMATTED' not in sql.upper():
                raise Exception("Table not found")
            df = MagicMock()
            df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = recording_sql

        m.create_hive_tables.function.__wrapped__(
            distcp_result=sample_distcp_result, spark=mock_spark, ti=MagicMock(),
        )

        create_ddl = next(s for s in sql_calls if 'CREATE EXTERNAL TABLE' in s.upper())
        assert '`dt` STRING' in create_ddl

    def test_repairs_existing_table(self, mock_spark, sample_distcp_result):
        df = MagicMock()
        df.collect.return_value = []
        mock_spark.sql.return_value = df
        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=sample_distcp_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['table_results'][0]['existed'] is True

    def test_recreate_tables_drops_and_recreates_existing(self, mock_spark, sample_distcp_result):
        """migration_recreate_tables=true must DROP an existing destination table
        (metadata only — EXTERNAL keeps S3 data) and recreate it from scratch
        rather than just running MSCK REPAIR. This is how a user fixes tables
        created by an older DAG with wrongly-typed partition columns."""
        sql_calls = []

        def recording_sql(sql):
            sql_calls.append(sql)
            df = MagicMock()
            df.collect.return_value = []
            return df  # DESCRIBE succeeds → table exists

        mock_spark.sql.side_effect = recording_sql

        with patch.object(m, 'get_config', return_value={'recreate_tables': True}):
            result = m.create_hive_tables.function.__wrapped__(
                distcp_result=sample_distcp_result, spark=mock_spark, ti=MagicMock(),
            )

        r = result['table_results'][0]
        assert r['status'] == 'COMPLETED'
        assert r['action'] == 'recreated'
        assert r['existed'] is True
        all_sql = ' '.join(sql_calls).upper()
        assert 'DROP TABLE IF EXISTS' in all_sql
        assert 'CREATE EXTERNAL TABLE' in all_sql

    def test_recreate_refuses_to_drop_iceberg_table(self, mock_spark, sample_distcp_result):
        """SAFETY: with recreate=true, an existing ICEBERG table (e.g. from a DAG 2
        in-place migration under the same name) must NOT be dropped — it would
        destroy the Iceberg table. The table is marked FAILED with a message
        pointing to the remediation script; no DROP is issued."""
        sql_calls = []

        def recording_sql(sql):
            sql_calls.append(sql)
            df = MagicMock()
            if sql.strip().upper().startswith('DESCRIBE FORMATTED'):
                prov = MagicMock()
                prov.col_name = 'Provider'
                prov.data_type = 'iceberg'
                df.collect.return_value = [prov]
            else:
                df.collect.return_value = []  # DESCRIBE (exists check) succeeds
            return df

        mock_spark.sql.side_effect = recording_sql

        ti = MagicMock()
        with patch.object(m, 'get_config', return_value={'recreate_tables': True}), \
             pytest.raises(Exception, match="Hive table creation failed"):
            m.create_hive_tables.function.__wrapped__(
                distcp_result=sample_distcp_result, spark=mock_spark, ti=ti,
            )

        # No DROP was issued against the Iceberg table
        assert 'DROP TABLE' not in ' '.join(sql_calls).upper()
        # Result recorded as FAILED / skipped_iceberg with a remediation hint
        pushed = ti.xcom_push.call_args.kwargs['value']
        r = pushed['table_results'][0]
        assert r['status'] == 'FAILED'
        assert r['action'] == 'skipped_iceberg'
        assert 'iceberg_cleanup' in r['error']

    def test_recreate_tables_false_does_not_drop(self, mock_spark, sample_distcp_result):
        """Default (migration_recreate_tables=false) must NOT drop an existing
        table — existing incremental-repair behavior is preserved."""
        sql_calls = []

        def recording_sql(sql):
            sql_calls.append(sql)
            df = MagicMock()
            df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = recording_sql

        with patch.object(m, 'get_config', return_value={'recreate_tables': False}):
            result = m.create_hive_tables.function.__wrapped__(
                distcp_result=sample_distcp_result, spark=mock_spark, ti=MagicMock(),
            )
        assert result['table_results'][0]['existed'] is True
        assert 'DROP TABLE' not in ' '.join(sql_calls).upper()

    def test_partition_filter_active_uses_add_partition_not_msck(self, mock_spark, sample_distcp_result):
        """When partition_filter_active=True, table creation must use
        ALTER TABLE ADD PARTITION per filtered partition, not MSCK REPAIR."""
        # Inject a filtered table into the distcp result
        sample_distcp_result['tables'][0].update({
            'partition_filter': 'dt>=2024-01-01',
            'filtered_partitions': ['dt=2024-01-01'],
            'partition_filter_active': True,
        })
        sample_distcp_result['distcp_results'][0]['status'] = 'COMPLETED'

        sql_calls = []
        def recording_sql(sql):
            sql_calls.append(sql)
            # Make DESCRIBE raise to simulate table not existing yet
            if sql.strip().upper().startswith('DESCRIBE') and 'FORMATTED' not in sql.upper():
                raise Exception("Table not found")
            df = MagicMock()
            df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = recording_sql

        m.create_hive_tables.function.__wrapped__(
            distcp_result=sample_distcp_result, spark=mock_spark, ti=MagicMock(),
        )

        all_sql = ' '.join(sql_calls).upper()
        assert 'ADD IF NOT EXISTS' in all_sql or 'ADD PARTITION' in all_sql
        assert 'MSCK REPAIR' not in all_sql

    def test_struct_types_use_space_notation_in_ddl(
        self, mock_spark, sample_run_id, sample_table_metadata_with_structs
    ):
        """Colon-separated struct types from Hive catalogString() must be converted
        to space-separated notation before embedding in CREATE EXTERNAL TABLE DDL.
        Regression test for Bug #2: PARSE_SYNTAX_ERROR on struct<field:type>."""
        distcp_result = {
            'run_id': sample_run_id,
            'source_database': 'sales_data',
            'dest_database': 'sales_data_s3',
            'dest_bucket': 's3a://test-bucket',
            'tables': sample_table_metadata_with_structs,
            'distcp_results': [{
                'source_database': 'sales_data',
                'source_table': 'flex_rules_result',
                'dest_database': 'sales_data_s3',
                'status': 'COMPLETED',
                'partition_filter': None,
                'error': None,
            }],
        }

        sql_calls = []
        def recording_sql(sql):
            sql_calls.append(sql)
            if sql.strip().upper().startswith('DESCRIBE') and 'FORMATTED' not in sql.upper():
                raise Exception("Table not found")
            df = MagicMock()
            df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = recording_sql

        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=distcp_result, spark=mock_spark, ti=MagicMock(),
        )

        assert result['table_results'][0]['status'] == 'COMPLETED'
        create_ddl = next(s for s in sql_calls if 'CREATE EXTERNAL TABLE' in s.upper())
        # Space-separated notation must be present
        assert 'FIELD_0 string' in create_ddl
        assert 'category string' in create_ddl
        # Colon-separated notation must NOT appear inside type bodies
        assert 'FIELD_0:string' not in create_ddl
        assert 'category:string' not in create_ddl

    def test_wide_struct_all_fields_present_in_ddl(
        self, mock_spark, sample_run_id, sample_table_metadata_with_structs
    ):
        """All 30 struct fields must appear in the DDL — no truncation to
        '... N more fields'. Regression test for Bug #1: DESCRIBE truncation."""
        distcp_result = {
            'run_id': sample_run_id,
            'source_database': 'sales_data',
            'dest_database': 'sales_data_s3',
            'dest_bucket': 's3a://test-bucket',
            'tables': sample_table_metadata_with_structs,
            'distcp_results': [{
                'source_database': 'sales_data',
                'source_table': 'flex_rules_result',
                'dest_database': 'sales_data_s3',
                'status': 'COMPLETED',
                'partition_filter': None,
                'error': None,
            }],
        }

        sql_calls = []
        def recording_sql(sql):
            sql_calls.append(sql)
            if sql.strip().upper().startswith('DESCRIBE') and 'FORMATTED' not in sql.upper():
                raise Exception("Table not found")
            df = MagicMock()
            df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = recording_sql

        m.create_hive_tables.function.__wrapped__(
            distcp_result=distcp_result, spark=mock_spark, ti=MagicMock(),
        )

        create_ddl = next(s for s in sql_calls if 'CREATE EXTERNAL TABLE' in s.upper())
        # All 30 fields must be present — none truncated
        for i in range(30):
            assert f'FIELD_{i} string' in create_ddl
        assert 'more fields' not in create_ddl


class TestUpdateTableCreateStatus:

    def test_sets_table_created_status(self, mock_spark, sample_table_result, mock_iceberg_retry):
        m.update_table_create_status.function(table_result=sample_table_result, spark=mock_spark)
        assert any('TABLE_CREATED' in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_skipped_row_gets_no_per_table_update(
        self, mock_spark, sample_table_result, mock_iceberg_retry
    ):
        sample_table_result['table_results'][0]['status'] = 'SOURCE_PATH_NOT_FOUND'
        m.update_table_create_status.function(table_result=sample_table_result, spark=mock_spark)
        per_table = [
            c.args[1] for c in mock_iceberg_retry.call_args_list
            if 'table_create_status =' in c.args[1] and 'source_table =' in c.args[1]
        ]
        assert per_table == []

    def test_preserves_skippable_statuses(self, mock_spark, sample_table_result, mock_iceberg_retry):
        sample_table_result['table_results'][0].update({
            'status': 'FAILED', 'error': 'forced failure for test',
        })
        m.update_table_create_status.function(table_result=sample_table_result, spark=mock_spark)
        labels = assert_each_overall_status_case_preserves_skippable(
            mock_iceberg_retry.call_args_list
        )
        # Both the per-table UPDATE and its failure patch rewrite overall_status.
        assert 'update_table_create_status:transactions' in labels
        assert 'update_table_create_status:failure_patch:transactions' in labels

    def test_catchall_preserves_skippable_statuses(
        self, mock_spark, sample_table_result, mock_iceberg_retry
    ):
        sample_table_result['table_results'] = []
        m.update_table_create_status.function(table_result=sample_table_result, spark=mock_spark)
        labels = assert_each_overall_status_case_preserves_skippable(
            mock_iceberg_retry.call_args_list
        )
        assert any(
            label.startswith('update_table_create_status:catchall:') for label in labels
        )

    def test_catchall_preserves_empty_source_overall_status(
        self, mock_spark, sample_table_result, mock_iceberg_retry
    ):
        """The catchall UPDATE for tables not processed by create_hive_tables
        must NOT overwrite overall_status='EMPTY_SOURCE' with 'FAILED'."""
        sample_table_result['table_results'] = []
        m.update_table_create_status.function(table_result=sample_table_result, spark=mock_spark)
        catchall_sqls = [
            str(c) for c in mock_iceberg_retry.call_args_list
            if "Table creation task did not process this table" in str(c)
        ]
        assert catchall_sqls, "catchall UPDATE should have fired"
        joined = ' '.join(catchall_sqls)
        assert "overall_status = 'FAILED'," not in joined
        assert "WHEN overall_status = 'EMPTY_SOURCE'" in joined

    def test_failure_patch_preserves_empty_source_overall_status(
        self, mock_spark, sample_table_result, mock_iceberg_retry
    ):
        """The per-table failure-patch UPDATE must also preserve EMPTY_SOURCE."""
        sample_table_result['table_results'][0].update({
            'status': 'FAILED', 'error': 'forced failure for test',
        })
        m.update_table_create_status.function(table_result=sample_table_result, spark=mock_spark)
        patch_sqls = [
            str(c) for c in mock_iceberg_retry.call_args_list
            if 'failure_patch' in str(c)
        ]
        assert patch_sqls, "failure-patch UPDATE should have fired"
        joined = ' '.join(patch_sqls)
        assert "WHEN overall_status = 'EMPTY_SOURCE'" in joined


class TestValidateDestinationTables:

    def _make_tracking_row(self):
        row = MagicMock()
        row.__getitem__ = lambda self, k: {
            'distcp_status': 'COMPLETED', 'table_create_status': 'COMPLETED',
            'overall_status': 'TABLE_CREATED', 'error_message': None,
        }[k]
        return row

    def _make_dest_field(self, name, dtype):
        f = MagicMock()
        f.name = name
        f.dataType.simpleString.return_value = dtype
        return f

    def _setup_dest_schema(self, mock_spark):
        # Validation reads dest schema via spark.table(dest_tbl).schema.fields.
        # Source schema (from fixture) excludes the 'dt' partition col; dest mock
        # includes it so the validation code's partition-col filter is exercised.
        table_mock = MagicMock()
        table_mock.schema.fields = [
            self._make_dest_field('id', 'bigint'),
            self._make_dest_field('amount', 'double'),
            self._make_dest_field('dt', 'string'),
        ]
        mock_spark.table.return_value = table_mock

    def _make_router(self, dest_count, partition_filter=None):
        _pf = partition_filter
        _filtered_parts = ['dt=2024-01-01', 'dt=2024-01-02']

        def sql_router(sql):
            df = MagicMock()
            sql_lower = sql.strip().lower()
            if 'distcp_status' in sql_lower:
                df.collect.return_value = [self._make_tracking_row()]
            elif 'source_row_count' in sql_lower:
                r = MagicMock()
                r.__getitem__ = lambda self, k: {
                    'source_row_count': 1000,
                    'source_partition_count': len(_filtered_parts) if _pf else 2,
                    'partition_filter': _pf,
                }[k]
                df.collect.return_value = [r]
            elif 'count(*)' in sql_lower and 'as c' in sql_lower:
                r = MagicMock()
                r.__getitem__ = lambda self, k: dest_count
                df.collect.return_value = [r]
            elif 'show partitions' in sql_lower:
                df.count.return_value = 2
                df.collect.return_value = [MagicMock(), MagicMock()]
            else:
                df.collect.return_value = []
            return df
        return sql_router

    def test_passes_with_matching_counts(self, mock_spark, sample_table_result):
        mock_spark.sql.side_effect = self._make_router(1000)
        self._setup_dest_schema(mock_spark)
        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['row_count_match'] is True
        assert result['validation_results'][0]['schema_match'] is True

    def test_skips_source_path_not_found(self, mock_spark, sample_table_result):
        mock_spark.sql.side_effect = self._make_router(1000)
        self._setup_dest_schema(mock_spark)
        sample_table_result['tables'][0].update({
            'error': 'Path does not exist: maprfs:/data/sales_data/transactions',
            'error_type': 'SOURCE_PATH_NOT_FOUND',
        })
        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['status'] == 'SOURCE_PATH_NOT_FOUND'
        assert 'Path does not exist' in result['validation_results'][0]['error']

    def test_detects_row_count_mismatch(self, mock_spark, sample_table_result):
        mock_spark.sql.side_effect = self._make_router(500)
        self._setup_dest_schema(mock_spark)
        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['row_count_match'] is False

    def test_partition_schema_mismatch_detected(self, mock_spark, sample_table_result):
        """A source date partition column landing as string on the destination
        must be flagged. The non-partition schema comparison excludes partition
        columns, so without a dedicated check this would silently pass."""
        sample_table_result['tables'][0]['partition_schema'] = [
            {'name': 'dt', 'type': 'date'},
        ]
        mock_spark.sql.side_effect = self._make_router(1000)
        self._setup_dest_schema(mock_spark)  # dest 'dt' is string
        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result, spark=mock_spark, ti=MagicMock(),
        )
        v = result['validation_results'][0]
        assert v['partition_schema_match'] is False
        assert 'dt' in v['partition_schema_differences'].lower()
        # Non-partition schema still matches; only the partition schema differs
        assert v['schema_match'] is True

    def test_partition_schema_match_when_types_align(self, mock_spark, sample_table_result):
        sample_table_result['tables'][0]['partition_schema'] = [
            {'name': 'dt', 'type': 'date'},
        ]
        mock_spark.sql.side_effect = self._make_router(1000)
        table_mock = MagicMock()
        table_mock.schema.fields = [
            self._make_dest_field('id', 'bigint'),
            self._make_dest_field('amount', 'double'),
            self._make_dest_field('dt', 'date'),
        ]
        mock_spark.table.return_value = table_mock
        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result, spark=mock_spark, ti=MagicMock(),
        )
        v = result['validation_results'][0]
        assert v['partition_schema_match'] is True
        assert v['partition_schema_differences'] == ''

    def test_partition_schema_not_compared_when_source_absent(self, mock_spark, sample_table_result):
        """Discovery from an older DAG version has no partition_schema. Comparison
        must be a no-op (match=True) so re-runs don't produce false failures."""
        sample_table_result['tables'][0].pop('partition_schema', None)
        mock_spark.sql.side_effect = self._make_router(1000)
        self._setup_dest_schema(mock_spark)
        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['partition_schema_match'] is True

    def test_validates_scoped_to_filtered_partitions_when_filter_active(
        self, mock_spark, sample_table_result
    ):
        """When partition_filter is active, the validation COUNT(*) must use a
        WHERE clause scoped to the filtered partitions, not a full-table scan."""
        sample_table_result['tables'][0].update({
            'partition_filter': 'dt>=2024-01-01',
            'filtered_partitions': ['dt=2024-01-01', 'dt=2024-01-02'],
            'partition_filter_active': True,
            'full_table_row_count': 1000,
            'full_table_partition_count': 2,
            'serde_properties': {},
        })
        mock_spark.sql.side_effect = self._make_router(1000, partition_filter='dt>=2024-01-01')
        self._setup_dest_schema(mock_spark)
        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result, spark=mock_spark, ti=MagicMock(),
        )
        # Confirm the scoped COUNT(*) with WHERE was issued
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        count_calls = [c for c in sql_calls if 'count(*)' in c.lower() and 'as c' in c.lower()]
        assert any('WHERE' in c for c in count_calls), (
            "Expected a scoped COUNT(*) with WHERE clause for filtered partitions"
        )
        assert result['validation_results'][0]['row_count_match'] is True

    def _make_upstream_only_router(self, upstream_values):
        """spark.sql router that returns just one upstream-tracking row.
        Used by EMPTY_SOURCE-shortcut tests where validation should never
        reach the COUNT(*) / SHOW PARTITIONS queries."""
        row = MagicMock()
        row.__getitem__ = lambda self, k: upstream_values[k]

        def sql_router(sql):
            df = MagicMock()
            if 'distcp_status' in sql.lower():
                df.collect.return_value = [row]
            else:
                df.collect.return_value = []
            return df
        return sql_router

    def test_empty_source_shortcut_taken_when_table_create_succeeded(
        self, mock_spark, sample_table_result
    ):
        """distcp_status='EMPTY_SOURCE' + table_create_status='COMPLETED' →
        validation takes the all-zeros / all-matches shortcut."""
        import copy
        empty_source_input = copy.deepcopy(sample_table_result)
        for t in empty_source_input['tables']:
            t['partition_count'] = 0
            t['partitions'] = []
            t['is_partitioned'] = False
            t['full_table_partition_count'] = 0
        src_schema = empty_source_input['tables'][0]['schema']
        mock_fields = []
        for col in src_schema:
            f = MagicMock()
            f.name = col['name']
            f.dataType.simpleString.return_value = col['type']
            mock_fields.append(f)
        mock_spark.table.return_value.schema.fields = mock_fields

        mock_spark.sql.side_effect = self._make_upstream_only_router({
            'distcp_status': 'EMPTY_SOURCE',
            'table_create_status': 'COMPLETED',
            'overall_status': 'EMPTY_SOURCE',
            'error_message': None,
        })
        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=empty_source_input, spark=mock_spark, ti=MagicMock(),
        )
        v = result['validation_results'][0]
        assert v['status'] == 'COMPLETED'
        assert v['row_count_match'] is True
        assert v['partition_count_match'] is True
        assert v['schema_match'] is True

    def test_empty_source_shortcut_wins_over_stale_failed_overall_status(
        self, mock_spark, sample_table_result
    ):
        """If a catchall left overall_status='FAILED' but distcp_status is
        still 'EMPTY_SOURCE' and the dest table got created, the shortcut
        must still fire — that's the run-2-style flake we're fixing."""
        mock_spark.sql.side_effect = self._make_upstream_only_router({
            'distcp_status': 'EMPTY_SOURCE',
            'table_create_status': 'COMPLETED',
            'overall_status': 'FAILED',
            'error_message': 'stale catchall message',
        })
        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['status'] == 'COMPLETED'

    def test_empty_source_skipped_when_table_create_failed(
        self, mock_spark, sample_table_result
    ):
        """distcp_status='EMPTY_SOURCE' but table_create_status='FAILED' means
        the destination shell was never built — validation must SKIP rather
        than falsely report schema_match=True."""
        mock_spark.sql.side_effect = self._make_upstream_only_router({
            'distcp_status': 'EMPTY_SOURCE',
            'table_create_status': 'FAILED',
            'overall_status': 'FAILED',
            'error_message': 'table create crashed',
        })
        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['status'] == 'SKIPPED'

    def test_empty_source_skipped_when_table_create_skipped(
        self, mock_spark, sample_table_result
    ):
        """Same as above for table_create_status='SKIPPED' (e.g.
        skipped_no_schema branch in create_hive_tables)."""
        mock_spark.sql.side_effect = self._make_upstream_only_router({
            'distcp_status': 'EMPTY_SOURCE',
            'table_create_status': 'SKIPPED',
            'overall_status': 'EMPTY_SOURCE',
            'error_message': None,
        })
        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['status'] == 'SKIPPED'


class TestUpdateValidationStatus:

    def test_sets_validated_on_match(self, mock_spark, sample_validation_result, mock_iceberg_retry):
        m.update_validation_status.function(validation_result=sample_validation_result, spark=mock_spark)
        assert any('VALIDATED' in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_preserves_skippable_statuses(self, mock_spark, sample_validation_result, mock_iceberg_retry):
        m.update_validation_status.function(validation_result=sample_validation_result, spark=mock_spark)
        labels = assert_each_overall_status_case_preserves_skippable(
            mock_iceberg_retry.call_args_list
        )
        assert 'update_validation_status:transactions' in labels
        # This task alone also guards error_message, so a skipped table keeps the
        # reason it was skipped instead of picking up a validation verdict.
        per_table = next(
            c.args[1] for c in mock_iceberg_retry.call_args_list
            if c.kwargs.get('task_label') == 'update_validation_status:transactions'
        )
        assert (
            f"WHEN overall_status IN ({m._SKIPPABLE_STATUS_SQL_IN}) THEN error_message"
            in per_table
        )
        assert '_SKIPPABLE_STATUS_SQL_IN' not in per_table

    def test_catchall_preserves_skippable_statuses(
        self, mock_spark, sample_validation_result, mock_iceberg_retry
    ):
        sample_validation_result['validation_results'] = []
        m.update_validation_status.function(validation_result=sample_validation_result, spark=mock_spark)
        labels = assert_each_overall_status_case_preserves_skippable(
            mock_iceberg_retry.call_args_list
        )
        assert any(
            label.startswith('update_validation_status:catchall:') for label in labels
        )

    def test_sets_validation_failed_on_mismatch(self, mock_spark, sample_validation_result, mock_iceberg_retry):
        sample_validation_result['validation_results'][0]['row_count_match'] = False
        m.update_validation_status.function(validation_result=sample_validation_result, spark=mock_spark)
        assert any('VALIDATION_FAILED' in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_partition_schema_mismatch_sets_validation_failed(
        self, mock_spark, sample_validation_result, mock_iceberg_retry
    ):
        """Row/partition counts and non-partition schema all match, but a partition
        schema mismatch alone must still fail validation."""
        sample_validation_result['validation_results'][0].update({
            'partition_schema_match': False,
            'partition_schema_differences': 'Partition type mismatch for dt: date vs string',
        })
        m.update_validation_status.function(validation_result=sample_validation_result, spark=mock_spark)
        assert any('VALIDATION_FAILED' in str(c) for c in mock_iceberg_retry.call_args_list)
        assert not any('VALIDATED,' in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_persists_partition_schema_columns(
        self, mock_spark, sample_validation_result, mock_iceberg_retry
    ):
        m.update_validation_status.function(validation_result=sample_validation_result, spark=mock_spark)
        joined = ' '.join(str(c) for c in mock_iceberg_retry.call_args_list)
        assert 'partition_schema_match' in joined
        assert 'partition_schema_differences' in joined


class TestGenerateHtmlReport:

    def test_generates_report_and_writes_to_s3(self, mock_spark, sample_run_id):
        run_row = SimpleNamespace(dag_run_id='dag_run_test')
        tbl_row = SimpleNamespace(
            source_database='sales_data', source_table='transactions',
            overall_status='VALIDATED', discovery_duration_seconds=12.0,
            distcp_duration_seconds=300.0, distcp_bytes_copied=10 * 1024 * 1024,
            distcp_files_copied=5, distcp_is_incremental=False,
            table_create_duration_seconds=8.0, validation_duration_seconds=5.0,
            validation_status='COMPLETED', row_count_match=True,
            partition_count_match=True, schema_match=True,
            source_row_count=1000, dest_hive_row_count=1000,
            source_partition_count=2, dest_partition_count=2,
            source_total_size_bytes=10 * 1024 * 1024, s3_total_size_bytes_before=0,
            s3_total_size_bytes_after=10 * 1024 * 1024, s3_bytes_transferred=10 * 1024 * 1024,
            file_size_match=True, source_file_count=5,
            s3_file_count_before=0, s3_file_count_after=5,
            s3_files_transferred=5, file_count_match=True,
            distcp_status='COMPLETED',
            file_format='PARQUET',
            partition_filter=None,
            filtered_partition_count=None,
        )
        vs_row = MagicMock()
        vs_row.__getitem__ = lambda self, k: 1
        vs_row.total_tables_validated = 1
        vs_row.tables_passed_validation = 1
        vs_row.tables_failed_validation = 0
        vs_row.total_row_count_mismatches = 0
        vs_row.total_partition_count_mismatches = 0
        vs_row.total_schema_mismatches = 0

        def sql_router(sql):
            df = MagicMock()
            sl = sql.lower()
            if 'migration_runs' in sl and 'where' in sl:
                df.collect.return_value = [run_row]
            elif 'order by' in sl:
                df.collect.return_value = [tbl_row]
            elif 'sum(case when row_count_match' in sl:
                df.collect.return_value = [vs_row]
            elif 'sum(case when file_size_match' in sl:
                fm_row = MagicMock()
                fm_row.tables_size_match = 1
                fm_row.tables_size_mismatch = 0
                fm_row.tables_file_count_match = 1
                fm_row.tables_file_count_mismatch = 0
                fm_row.total_source_bytes = 10 * 1024 * 1024
                fm_row.total_dest_bytes = 10 * 1024 * 1024
                df.collect.return_value = [fm_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router
        result = m.generate_html_report.function(run_id=sample_run_id, spark=mock_spark)
        assert result['report_path'].endswith('.html')
        assert sample_run_id in result['report_path']
        assert mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value.create.called

    def test_html_report_shows_empty_partition_names(self, mock_spark, sample_run_id):
        """Empty partition names persisted in the tracking table must render
        somewhere in the generated HTML report."""
        run_row = SimpleNamespace(dag_run_id='dag_run_test')
        tbl_row = SimpleNamespace(
            source_database='sales_data', source_table='transactions',
            overall_status='VALIDATED', discovery_duration_seconds=12.0,
            distcp_duration_seconds=300.0, distcp_bytes_copied=10 * 1024 * 1024,
            distcp_files_copied=5, distcp_is_incremental=False,
            table_create_duration_seconds=8.0, validation_duration_seconds=5.0,
            validation_status='COMPLETED', row_count_match=True,
            partition_count_match=True, schema_match=True,
            source_row_count=1000, dest_hive_row_count=1000,
            source_partition_count=2, dest_partition_count=2,
            source_total_size_bytes=10 * 1024 * 1024, s3_total_size_bytes_before=0,
            s3_total_size_bytes_after=10 * 1024 * 1024, s3_bytes_transferred=10 * 1024 * 1024,
            file_size_match=True, source_file_count=5,
            s3_file_count_before=0, s3_file_count_after=5,
            s3_files_transferred=5, file_count_match=True,
            distcp_status='COMPLETED',
            file_format='PARQUET',
            partition_filter="dt>='2024-01-01'",
            filtered_partition_count=3,
            empty_partition_names='dt=2024-01-02, dt=2024-01-03',
        )
        vs_row = MagicMock()
        vs_row.__getitem__ = lambda self, k: 1
        vs_row.total_tables_validated = 1
        vs_row.tables_passed_validation = 1
        vs_row.tables_failed_validation = 0
        vs_row.total_row_count_mismatches = 0
        vs_row.total_partition_count_mismatches = 0
        vs_row.total_schema_mismatches = 0

        def sql_router(sql):
            df = MagicMock()
            sl = sql.lower()
            if 'migration_runs' in sl and 'where' in sl:
                df.collect.return_value = [run_row]
            elif 'order by' in sl:
                df.collect.return_value = [tbl_row]
            elif 'sum(case when row_count_match' in sl:
                df.collect.return_value = [vs_row]
            elif 'sum(case when file_size_match' in sl:
                fm_row = MagicMock()
                fm_row.tables_size_match = 1
                fm_row.tables_size_mismatch = 0
                fm_row.tables_file_count_match = 1
                fm_row.tables_file_count_mismatch = 0
                fm_row.total_source_bytes = 10 * 1024 * 1024
                fm_row.total_dest_bytes = 10 * 1024 * 1024
                df.collect.return_value = [fm_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router
        m.generate_html_report.function(run_id=sample_run_id, spark=mock_spark)

        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        written_bytes = fs_mock.create.return_value.write.call_args[0][0]
        html = written_bytes.decode('utf-8')
        assert 'dt=2024-01-02' in html
        assert 'dt=2024-01-03' in html


    def test_source_path_not_found_gets_its_own_card_and_badge(self, mock_spark, sample_run_id):
        """An orphaned metastore entry must be visible in the report as its own
        condition, not folded into TABLE_NOT_FOUND — ops needs to know the source
        needs cleanup rather than that the table was never there."""
        run_row = SimpleNamespace(dag_run_id='dag_run_test')
        tbl_row = SimpleNamespace(
            source_database='cap_curated_mktg', source_table='t_ce_phonepref_ref',
            overall_status='SOURCE_PATH_NOT_FOUND', discovery_duration_seconds=1.0,
            distcp_duration_seconds=None, distcp_bytes_copied=0,
            distcp_files_copied=0, distcp_is_incremental=False,
            table_create_duration_seconds=None, validation_duration_seconds=None,
            validation_status=None, row_count_match=None,
            partition_count_match=None, schema_match=None,
            source_row_count=0, dest_hive_row_count=0,
            source_partition_count=0, dest_partition_count=0,
            source_total_size_bytes=0, s3_total_size_bytes_before=0,
            s3_total_size_bytes_after=0, s3_bytes_transferred=0,
            file_size_match=None, source_file_count=0,
            s3_file_count_before=0, s3_file_count_after=0,
            s3_files_transferred=0, file_count_match=None,
            distcp_status=None, file_format='UNKNOWN',
            partition_filter=None, filtered_partition_count=None,
            source_location='maprfs:/datalake/cap/curated/mktg/target/<t_ce_phonepref_ref>',
            error_message="u'Path does not exist;' It is possible the "
                          'underlying files have been updated',
        )

        def sql_router(sql):
            df = MagicMock()
            sl = sql.lower()
            if 'migration_runs' in sl and 'where' in sl:
                df.collect.return_value = [run_row]
            elif 'order by' in sl:
                df.collect.return_value = [tbl_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router
        m.generate_html_report.function(run_id=sample_run_id, spark=mock_spark)

        stream = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value.create.return_value
        html = stream.write.call_args[0][0].decode('utf-8')
        assert 'SOURCE PATH MISSING' in html
        assert 'status-path-not-found' in html
        assert 'SOURCE_PATH_NOT_FOUND' in html
        # The location is rendered in place of the full Spark exception, and escaped —
        # the metacharacters live in source_location because that is the field shown.
        assert 'maprfs:/datalake/cap/curated/mktg/target/&lt;t_ce_phonepref_ref&gt;' in html
        assert '<t_ce_phonepref_ref>' not in html
        assert 'It is possible the underlying files' not in html


class TestSendMigrationReportEmail:

    def test_skips_when_no_recipients(self, mock_spark, sample_run_id):
        with patch('airflow.models.Variable.get', return_value=''):
            result = m.send_migration_report_email.function(
                report_result={'report_path': 's3a://bucket/report.html'},
                run_id=sample_run_id, spark=mock_spark,
            )
        assert result['sent'] is False
        assert result['reason'] == 'no_recipients'

    def test_sends_when_recipients_configured(self, mock_spark, sample_run_id):
        reader_mock = MagicMock()
        reader_mock.readLine.side_effect = ['<html>report</html>', None]
        mock_spark._jvm.java.io.BufferedReader.return_value = reader_mock

        with patch('airflow.utils.email.send_email'), \
             patch('tempfile.NamedTemporaryFile') as mock_tmp, \
             patch('os.unlink'):
            tmp_inst = MagicMock()
            tmp_inst.name = '/tmp/report.html'
            mock_tmp.return_value = tmp_inst
            result = m.send_migration_report_email.function(
                report_result={'report_path': 's3a://bucket/report.html'},
                run_id=sample_run_id, spark=mock_spark,
            )
        assert result['sent'] is True
        assert 'user@example.com' in result['recipients']


class TestFinalizeRun:

    def test_updates_migration_runs_with_completed(self, mock_spark, sample_run_id):
        stats_row = MagicMock()
        stats_row.__getitem__ = lambda self, k: 5
        df = MagicMock()
        df.collect.return_value = [stats_row]
        mock_spark.sql.return_value = df

        m.finalize_run.function(run_id=sample_run_id, spark=mock_spark)
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert any('COMPLETED' in c for c in sql_calls)

    def _run_with_stats(self, mock_spark, run_id, **counts):
        """Returns the raw SQL strings. `c.args[0]`, not `str(c)`: a mock call's repr
        escapes newlines, so splitlines() yields one line and per-line assertions
        silently degrade to matching the whole statement."""
        stats_row = MagicMock()
        stats_row.__getitem__ = lambda self, k: counts[k]
        df = MagicMock()
        df.collect.return_value = [stats_row]
        mock_spark.sql.return_value = df
        m.finalize_run.function(run_id=run_id, spark=mock_spark)
        return [c.args[0] for c in mock_spark.sql.call_args_list if c.args]

    def test_not_found_rollup_counts_every_skippable_status(self, mock_spark, sample_run_id):
        """Pin the `not_found` rollup specifically: asserting the names appear anywhere
        is vacuous, since the `successful` rollup above satisfies that on its own."""
        sqls = self._run_with_stats(
            mock_spark, sample_run_id, total=3, successful=3, failed=0, not_found=1
        )
        not_found_expr = next(
            line for sql in sqls for line in sql.splitlines() if 'as not_found' in line
        )
        assert '_SKIPPABLE_STATUS_SQL_IN' not in not_found_expr
        for status in m.SKIPPABLE_DISCOVERY_ERRORS:
            assert status in not_found_expr

    def test_skipped_table_downgrades_run_to_completed_with_missing(
        self, mock_spark, sample_run_id
    ):
        """A run whose only anomaly is a skipped table must not finish bare COMPLETED.
        failed=0 matters: any other value short-circuits to COMPLETED_WITH_FAILURES
        and the branch under test never runs."""
        sqls = ' '.join(self._run_with_stats(
            mock_spark, sample_run_id, total=3, successful=3, failed=0, not_found=1
        ))
        assert "status = 'COMPLETED_WITH_MISSING'" in sqls

    def test_clean_run_still_completes(self, mock_spark, sample_run_id):
        sqls = ' '.join(self._run_with_stats(
            mock_spark, sample_run_id, total=3, successful=3, failed=0, not_found=0
        ))
        assert "status = 'COMPLETED'" in sqls
        assert 'COMPLETED_WITH_MISSING' not in sqls


class TestFlattenAndBatch:

    def _group(self, map_index, db, table_names, **table_overrides):
        tables = []
        for name in table_names:
            t = {
                'source_database': db, 'source_table': name,
                'dest_database': f'{db}_s3', 'dest_bucket': 's3a://test-bucket',
                'source_total_size_bytes': 10 * 1024 ** 3,
                'source_file_count': 100, 'partition_filter_active': False,
            }
            t.update(table_overrides)
            tables.append(t)
        return {
            'run_id': 'run-1', 'source_database': db, 'dest_database': f'{db}_s3',
            'dest_bucket': 's3a://test-bucket', 'tables': tables,
            '_map_index': map_index,
        }

    def test_emits_one_descriptor_per_bin_covering_every_table(self):
        groups = [self._group(0, 'db_a', ['t1', 't2']), self._group(1, 'db_b', ['t3'])]
        batches = m.flatten_and_batch.function(discoveries=groups)
        keys = [tuple(k) for b in batches for k in b['table_keys']]
        assert sorted(keys) == [('t1', ''), ('t2', ''), ('t3', '')]

    def test_batches_never_mix_source_databases(self):
        groups = [self._group(0, 'db_a', ['t1']), self._group(1, 'db_b', ['t2'])]
        batches = m.flatten_and_batch.function(discoveries=groups)
        assert {b['source_database'] for b in batches} == {'db_a', 'db_b'}
        assert all(len({b['source_database']}) == 1 for b in batches)

    def test_addresses_groups_by_map_index_not_list_position(self):
        """A dead record_discovered_tables instance leaves a gap in the sequence."""
        groups = [
            self._group(0, 'db_a', ['t1']),
            self._group(1, 'db_b', ['t2']),
            self._group(3, 'db_d', ['t4']),
        ]
        batches = m.flatten_and_batch.function(discoveries=groups)
        assert sorted(b['group_map_index'] for b in batches) == [0, 1, 3]
        assert 2 not in {b['group_map_index'] for b in batches}

    def test_skips_failed_group_discoveries(self):
        groups = [{}, self._group(1, 'db_b', ['t2']), None]
        batches = m.flatten_and_batch.function(discoveries=groups)
        assert len(batches) == 1
        assert batches[0]['group_map_index'] == 1

    def test_raises_when_a_group_has_no_map_index(self):
        stale = self._group(0, 'db_a', ['t1'])
        del stale['_map_index']
        with pytest.raises(ValueError, match='_map_index'):
            m.flatten_and_batch.function(discoveries=[stale])

    def test_descriptors_are_ordered_by_cost_descending(self):
        groups = [
            self._group(0, 'db_a', ['small'], source_total_size_bytes=1024,
                        source_file_count=1),
            self._group(1, 'db_b', ['huge'],
                        source_total_size_bytes=5000 * 1024 ** 3,
                        source_file_count=100000),
        ]
        batches = m.flatten_and_batch.function(discoveries=groups)
        assert batches[0]['source_database'] == 'db_b'
        costs = [b['batch_cost_secs'] for b in batches]
        assert costs == sorted(costs, reverse=True)

    def test_batch_index_is_contiguous_from_zero(self):
        groups = [self._group(0, 'db_a', [f't{i}' for i in range(30)])]
        batches = m.flatten_and_batch.function(discoveries=groups)
        assert [b['batch_index'] for b in batches] == list(range(len(batches)))

    def test_descriptors_carry_no_table_metadata(self):
        """The whole point: every batch TI deserializes this list in full."""
        groups = [self._group(0, 'db_a', ['t1'], schema=[{'name': 'c'}],
                              partitions=['d=1'], partition_file_counts={'d=1': 3})]
        batches = m.flatten_and_batch.function(discoveries=groups)
        for key in ('schema', 'partitions', 'partition_file_counts', 'tables'):
            assert key not in batches[0]

    def test_a_normal_batch_budget_still_covers_its_own_cost(self):
        """The clamp must not starve ordinary batches: if a batch's budget fell
        below its estimate, a healthy copy would be failed by its own deadline."""
        groups = [self._group(0, 'db_a', ['t1', 't2', 't3', 't4'])]
        batches = m.flatten_and_batch.function(discoveries=groups)
        for b in batches:
            assert b['batch_budget_secs'] >= b['batch_cost_secs']

    def test_an_over_clamp_batch_is_budgeted_below_its_cost_on_purpose(self):
        """Replaces test_budget_covers_an_over_cap_monster_batch, which asserted
        the bug: a ~19.5 TiB table used to get a budget matching its own cost
        (170790s), far past the 28800s execution_timeout, so Airflow SIGKILLed
        the task before the soft budget could fire and the XCom — and every
        FAILED row in it — was discarded. Being under-budgeted is the fix: the
        batch now fails cleanly and -update lets the retry resume."""
        backstop = m._DISTCP_EXECUTION_TIMEOUT.total_seconds()
        groups = [self._group(0, 'db_a', ['huge'],
                              source_total_size_bytes=20000 * 1024 ** 3,
                              source_file_count=10)]
        batches = m.flatten_and_batch.function(discoveries=groups)
        assert batches[0]['batch_budget_secs'] == pytest.approx(
            m._BATCH_BUDGET_FRACTION * backstop)
        assert batches[0]['batch_budget_secs'] < batches[0]['batch_cost_secs']

    def test_partition_filter_is_part_of_the_table_key(self):
        group = self._group(0, 'db_a', ['t1'])
        group['tables'].append({**group['tables'][0], 'partition_filter': 'd=2024'})
        batches = m.flatten_and_batch.function(discoveries=[group])
        keys = sorted(tuple(k) for b in batches for k in b['table_keys'])
        assert keys == [('t1', ''), ('t1', 'd=2024')]

    def test_empty_input_returns_an_empty_plan(self):
        assert m.flatten_and_batch.function(discoveries=[]) == []

    def test_no_batch_budget_can_exceed_the_execution_timeout(self):
        """The property the clamp exists for: a budget above the backstop can
        never fire, so the batch is SIGKILLed and its XCom discarded instead of
        writing FAILED rows."""
        backstop = m._DISTCP_EXECUTION_TIMEOUT.total_seconds()
        # 40 TiB in one table: estimated far above any per-batch budget.
        groups = [self._group(
            0, 'db_a', ['huge'],
            source_total_size_bytes=40 * 1024 ** 4, source_file_count=100_000)]
        batches = m.flatten_and_batch.function(discoveries=groups)
        assert batches
        for b in batches:
            assert b['batch_budget_secs'] <= backstop

    def test_an_over_budget_batch_is_warned_about_by_table_name(self, caplog):
        groups = [self._group(
            0, 'db_a', ['huge'],
            source_total_size_bytes=40 * 1024 ** 4, source_file_count=100_000)]
        with caplog.at_level(logging.WARNING):
            m.flatten_and_batch.function(discoveries=groups)
        assert 'huge' in caplog.text
        assert 'more than one attempt' in caplog.text
        # The retired knobs must not be named as the fix.
        assert 'max_batches' not in caplog.text
        assert 'batch_target_cost_seconds' not in caplog.text

    def test_a_normal_batch_is_not_warned_about(self, caplog):
        groups = [self._group(
            0, 'db_a', ['t1', 't2', 't3', 't4'],
            source_total_size_bytes=1024 ** 3, source_file_count=10)]
        with caplog.at_level(logging.WARNING):
            m.flatten_and_batch.function(discoveries=groups)
        assert 'more than one attempt' not in caplog.text

    def test_batch_count_follows_the_cap_not_the_table_count(
            self, monkeypatch):
        """The plan's central behaviour, and nothing else pins it. Fixture
        tables cost ~167s each, so four of them give a cap of 670/(3*2) = 112s
        and no two share a bin. Under the retired seconds-target cap (1800s)
        all four landed in ONE batch, and no other test in this class would
        notice the difference."""
        monkeypatch.delenv('MIGRATION_DISTCP_COPY_MAX_CONCURRENT',
                           raising=False)
        groups = [self._group(0, 'db_a', ['t1', 't2', 't3', 't4'])]
        batches = m.flatten_and_batch.function(discoveries=groups)
        assert len(batches) == 4
        assert all(len(b['table_keys']) == 1 for b in batches)

    def test_a_small_run_is_spread_across_the_lanes(self, monkeypatch):
        """One table is one batch — nothing to spread. Two tables must not
        collapse into a single task while lanes sit idle."""
        monkeypatch.delenv('MIGRATION_DISTCP_COPY_MAX_CONCURRENT',
                           raising=False)
        one = m.flatten_and_batch.function(
            discoveries=[self._group(0, 'db_a', ['solo'])])
        two = m.flatten_and_batch.function(
            discoveries=[self._group(0, 'db_a', ['a', 'b'])])
        assert len(one) == 1
        assert len(two) == 2
