"""DAG 1 Task Tests: mapr_to_s3_migration pipeline."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import migration_dag_mapr_to_s3 as m
import pytest

from .helpers import make_excel_bytes, mock_ssh_stdout, setup_spark_excel


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


class TestRecordDiscoveredTables:

    def _setup_count(self, mock_spark, count):
        row = MagicMock()
        row.__getitem__ = lambda self, k: count
        df = MagicMock()
        df.collect.return_value = [row]
        mock_spark.sql.return_value = df

    def test_inserts_new_record(self, mock_spark, sample_discovery, mock_iceberg_retry):
        self._setup_count(mock_spark, 0)
        result = m.record_discovered_tables.function(discovery=sample_discovery, spark=mock_spark)
        assert any('INSERT INTO' in str(c) for c in mock_iceberg_retry.call_args_list)
        assert result['run_id'] == sample_discovery['run_id']

    def test_insert_covers_partition_schema_columns(self, mock_spark, sample_discovery, mock_iceberg_retry):
        """The tracking table has partition_schema_* columns, and the INSERT uses an
        explicit column list — the new columns MUST be listed, otherwise Iceberg
        rejects the write with INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA."""
        self._setup_count(mock_spark, 0)
        sample_discovery['tables'][0]['partition_schema'] = [{'name': 'dt', 'type': 'date'}]
        m.record_discovered_tables.function(discovery=sample_discovery, spark=mock_spark)
        insert_sql = next(
            c.args[1] for c in mock_iceberg_retry.call_args_list if 'INSERT INTO' in c.args[1]
        )
        for col in ('partition_schema_json', 'partition_schema_match', 'partition_schema_differences'):
            assert col in insert_sql
        # The captured source partition schema is persisted as JSON
        assert '"name": "dt"' in insert_sql and '"type": "date"' in insert_sql

    def test_updates_existing_record(self, mock_spark, sample_discovery, mock_iceberg_retry):
        self._setup_count(mock_spark, 1)
        m.record_discovered_tables.function(discovery=sample_discovery, spark=mock_spark)
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
        m.record_discovered_tables.function(discovery=discovery, spark=mock_spark)
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
        m.record_discovered_tables.function(discovery=discovery, spark=mock_spark)
        all_sql = ' '.join(c.args[1] for c in mock_iceberg_retry.call_args_list)
        assert "'DATABASE_NOT_FOUND'" in all_sql
        assert 'does not exist' in all_sql


class TestRunDistcpSsh:

    def _make_distcp_stdout(self, incremental=False):
        return mock_ssh_stdout(0, (
            "===DISTCP_METRICS_START===\n"
            f"INCREMENTAL={'true' if incremental else 'false'}\n"
            "S3_FILE_COUNT_BEFORE=0\nS3_TOTAL_SIZE_BEFORE=0\nDISTCP_EXIT_CODE=0\n"
            "BYTES_COPIED=10485760\nFILES_COPIED=5\n"
            "S3_FILE_COUNT_AFTER=5\nS3_TOTAL_SIZE_AFTER=10485760\n"
            "S3_FILES_TRANSFERRED=5\nS3_BYTES_TRANSFERRED=10485760\n"
            "===DISTCP_METRICS_END===\n"
        ).encode())

    def test_successful_copy_detects_incremental(self, mock_ssh_hook, sample_discovery):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (MagicMock(), self._make_distcp_stdout(incremental=True), stderr)

        result = m.run_distcp_ssh.function.__wrapped__(
            discovery=sample_discovery,
            cluster_setup={'temp_dir': '/tmp/test', 'run_id': 'r'},
            ti=MagicMock(),
        )
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
        result = m.run_distcp_ssh.function.__wrapped__(
            discovery=discovery,
            cluster_setup={'temp_dir': '/tmp/test', 'run_id': 'r'},
            ti=MagicMock(),
        )
        assert result['distcp_results'][0]['status'] == 'TABLE_NOT_FOUND'
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
        result = m.run_distcp_ssh.function.__wrapped__(
            discovery=discovery,
            cluster_setup={'temp_dir': '/tmp/test', 'run_id': 'r'},
            ti=MagicMock(),
        )
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
            m.run_distcp_ssh.function.__wrapped__(
                discovery=sample_discovery,
                cluster_setup={'temp_dir': '/tmp/test', 'run_id': 'r'},
                ti=MagicMock(),
            )

    def test_partition_filter_active_uses_per_partition_distcp(self, mock_ssh_hook, sample_discovery):
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), self._make_distcp_stdout(incremental=False), stderr
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
        result = m.run_distcp_ssh.function.__wrapped__(
            discovery=filtered_discovery,
            cluster_setup={'temp_dir': '/tmp/test', 'run_id': 'r'},
            ti=MagicMock(),
        )
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
            MagicMock(), self._make_distcp_stdout(incremental=False), stderr
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
            result = m.run_distcp_ssh.function.__wrapped__(
                discovery=filtered_discovery,
                cluster_setup={'temp_dir': '/tmp/test', 'run_id': 'r'},
                ti=MagicMock(),
            )

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
        result = m.run_distcp_ssh.function.__wrapped__(
            discovery=empty_filter_discovery,
            cluster_setup={'temp_dir': '/tmp/test', 'run_id': 'r'},
            ti=MagicMock(),
        )
        assert result['distcp_results'][0]['status'] == 'SKIPPED'
        client.exec_command.assert_not_called()

    def _run_with_partition_filter(self, mock_ssh_hook, sample_discovery, preserve_delete):
        """Helper: invoke run_distcp_ssh with a partition filter and return the SSH command."""
        hook, client, _, _ = mock_ssh_hook
        stderr = MagicMock()
        stderr.read.return_value = b''
        client.exec_command.return_value = (
            MagicMock(), self._make_distcp_stdout(incremental=False), stderr
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
            m.run_distcp_ssh.function.__wrapped__(
                discovery=filtered_discovery,
                cluster_setup={'temp_dir': '/tmp/test', 'run_id': 'r'},
                ti=MagicMock(),
            )
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
        result = m.run_distcp_ssh.function.__wrapped__(
            discovery=empty_source_discovery,
            cluster_setup={'temp_dir': '/tmp/test', 'run_id': 'r'},
            ti=MagicMock(),
        )
        assert result['distcp_results'][0]['status'] == 'EMPTY_SOURCE'
        if client.exec_command.called:
            cmd = client.exec_command.call_args[0][0]
            assert 'distcp' not in cmd.lower(), "distcp should not be called for empty source"


class TestUpdateDistcpStatus:

    def test_sets_copied_on_success(self, mock_spark, sample_distcp_result, mock_iceberg_retry):
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        assert any('COPIED' in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_sets_failed_on_error(self, mock_spark, sample_distcp_result, mock_iceberg_retry):
        sample_distcp_result['distcp_results'][0]['status'] = 'FAILED'
        sample_distcp_result['distcp_results'][0]['error'] = 'Network error'
        m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        assert any('FAILED' in str(c) for c in mock_iceberg_retry.call_args_list)

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


class TestCreateHiveTables:

    def test_creates_new_table(self, mock_spark, sample_distcp_result):
        mock_spark.sql.side_effect = [None, Exception("Not found"), None, None, None]
        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=sample_distcp_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['table_results'][0]['status'] == 'COMPLETED'
        assert result['table_results'][0]['existed'] is False

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
