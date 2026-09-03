"""DAG 2 Task Tests: iceberg_migration pipeline."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import migration_dag_iceberg as m
import pytest

from .helpers import make_excel_bytes, setup_spark_excel


def _keyed_row(value):
    """A Row-like mock returning `value` for any key or index."""
    row = MagicMock()
    row.__getitem__ = lambda self, k, _v=value: _v
    return row


class TestInitIcebergTrackingTables:

    def test_creates_database_and_tables(self, mock_spark):
        result = m.init_iceberg_tracking_tables.function(spark=mock_spark)
        assert result == {'status': 'initialized', 'database': 'migration_tracking'}
        sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'iceberg_migration_runs' in sql
        assert 'iceberg_migration_table_status' in sql


class TestCreateIcebergMigrationRun:

    def test_creates_run_with_running_status(self, mock_spark):
        run_id = m.create_iceberg_migration_run.function(
            excel_file_path='s3a://bucket/ice.xlsx',
            dag_run_id='dag_test',
            spark=mock_spark,
        )
        assert run_id.startswith('iceberg_run_')
        assert 'RUNNING' in ' '.join(str(c) for c in mock_spark.sql.call_args_list)

class TestIsPermanentError:
    def test_matches_known_marker(self):
        assert m.is_permanent_error("iceberg_migrate", Exception("Corrupt Parquet file")) is True

    def test_no_match_returns_false(self):
        assert m.is_permanent_error("iceberg_migrate", Exception("connection timed out")) is False

    def test_unknown_category_returns_false(self):
        assert m.is_permanent_error("nonexistent_category", Exception("anything")) is False

    def test_case_insensitive_match(self):
        assert m.is_permanent_error("send_email", Exception("SMTPAuthenticationError: bad creds")) is True

class TestPermanentFail:
    def test_raises_airflow_fail_exception(self):
        from airflow.exceptions import AirflowFailException
        with pytest.raises(AirflowFailException, match="failed permanently"):
            m.permanent_fail("my_task", ValueError("bad input"))

class TestParseIcebergExcel:

    def test_snapshot_migration_by_default(self, mock_spark, sample_iceberg_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'sales_s3', 'table': '*', 'inplace_migration': 'F', 'destination_iceberg_database': ''},
        ]))
        result = m.parse_iceberg_excel.function('s3a://b/f.xlsx', sample_iceberg_run_id, spark=mock_spark)
        assert result[0]['inplace_migration'] is False
        assert result[0]['destination_iceberg_database'] == 'sales_s3_iceberg'

    def test_inplace_uses_same_database(self, mock_spark, sample_iceberg_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'sales_s3', 'table': '*', 'inplace_migration': 'T', 'destination_iceberg_database': ''},
        ]))
        result = m.parse_iceberg_excel.function('s3a://b/f.xlsx', sample_iceberg_run_id, spark=mock_spark)
        assert result[0]['destination_iceberg_database'] == 'sales_s3'

    def test_custom_dest_database(self, mock_spark, sample_iceberg_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'sales_s3', 'table': '*', 'inplace_migration': 'F', 'destination_iceberg_database': 'custom_ice_db'},
        ]))
        result = m.parse_iceberg_excel.function('s3a://b/f.xlsx', sample_iceberg_run_id, spark=mock_spark)
        assert result[0]['destination_iceberg_database'] == 'custom_ice_db'


class TestDiscoverHiveTables:

    def _location_router(self, tables):
        def sql_router(sql):
            df = MagicMock()
            if 'show databases' in sql.lower():
                df.count.return_value = 1
            elif 'show tables' in sql.lower():
                df.collect.return_value = [MagicMock(tableName=t) for t in tables]
            elif 'describe formatted' in sql.lower():
                loc = MagicMock()
                loc.col_name = 'Location'
                loc.data_type = 's3a://bucket/db/tbl'
                df.collect.return_value = [loc]
            return df
        return sql_router

    def test_discovers_all_tables_with_wildcard(self, mock_spark, sample_iceberg_db_config):
        mock_spark.sql.side_effect = self._location_router(['transactions', 'orders'])
        result = m.discover_hive_tables.function.__wrapped__(
            db_config=sample_iceberg_db_config, spark=mock_spark,
        )
        assert len(result['discovered_tables']) == 2

    def test_discovers_filtered_tables_by_pattern(self, mock_spark, sample_iceberg_db_config):
        import fnmatch
        import re
        sample_iceberg_db_config['table_pattern'] = 'trans*'

        def sql_router(sql):
            df = MagicMock()
            if 'show databases' in sql.lower():
                df.count.return_value = 1
            elif 'show tables' in sql.lower():
                all_t = ['transactions', 'orders', 'trans_history']
                like = re.search(r"like '([^']+)'", sql.lower())
                matched = [t for t in all_t if fnmatch.fnmatch(t, like.group(1).replace('%', '*'))] if like else all_t
                df.collect.return_value = [MagicMock(tableName=n) for n in matched]
            elif 'describe formatted' in sql.lower():
                loc = MagicMock()
                loc.col_name = 'Location'
                loc.data_type = 's3a://bucket/db/tbl'
                df.collect.return_value = [loc]
            return df

        mock_spark.sql.side_effect = sql_router
        result = m.discover_hive_tables.function.__wrapped__(
            db_config=sample_iceberg_db_config, spark=mock_spark,
        )
        names = [t['table'] for t in result['discovered_tables']]
        assert 'transactions' in names
        assert 'orders' not in names

    def test_missing_database_records_skip_entry_per_token(self, mock_spark, sample_iceberg_db_config):
        """SHOW DATABASES matches nothing -> one DATABASE_NOT_FOUND entry per requested token."""
        sample_iceberg_db_config['table_tokens'] = ['transactions', 'orders']

        def sql_router(sql):
            df = MagicMock()
            df.count.return_value = 0
            df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router
        result = m.discover_hive_tables.function.__wrapped__(
            db_config=sample_iceberg_db_config, spark=mock_spark,
        )
        entries = result['discovered_tables']
        assert [e['table'] for e in entries] == ['transactions', 'orders']
        assert {e['skip_code'] for e in entries} == {'DATABASE_NOT_FOUND'}
        assert {e['skip_status'] for e in entries} == {'SKIPPED'}
        # No DESCRIBE FORMATTED should be attempted against a database that does not exist.
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'describe formatted' not in all_sql

    def test_wildcard_matching_nothing_records_skip_entry(self, mock_spark, sample_iceberg_db_config):
        """Database exists but the pattern matches no table -> NO_TABLES_MATCHED_PATTERN."""
        sample_iceberg_db_config['table_tokens'] = ['nosuch*']

        def sql_router(sql):
            df = MagicMock()
            if 'show databases' in sql.lower():
                df.count.return_value = 1
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df

        mock_spark.sql.side_effect = sql_router
        result = m.discover_hive_tables.function.__wrapped__(
            db_config=sample_iceberg_db_config, spark=mock_spark,
        )
        entries = result['discovered_tables']
        assert len(entries) == 1
        assert entries[0]['table'] == 'nosuch*'
        assert entries[0]['skip_code'] == 'NO_TABLES_MATCHED_PATTERN'
        assert entries[0]['skip_status'] == 'SKIPPED'

    def test_wildcard_excludes_staging_tables(self, mock_spark):
        def router(sql):
            df = MagicMock()
            sl = sql.lower()
            if 'show databases' in sl:
                df.count.return_value = 1
            elif 'show tables' in sl:
                df.collect.return_value = [
                    MagicMock(tableName='logs'),
                    MagicMock(tableName='logs__ice_staging'),
                    MagicMock(tableName='logs_backup_'),
                ]
                df.count.return_value = 3
            elif 'describe formatted' in sl:
                df.collect.return_value = [
                    MagicMock(col_name='Location', data_type='s3a://bucket/logs'),
                ]
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df
        mock_spark.sql.side_effect = router
        result = m.discover_hive_tables.function.__wrapped__(
            db_config={'source_database': 'db', 'table_tokens': ['*'],
                       'inplace_migration': True, 'destination_iceberg_database': 'db',
                       'run_id': 'r1'},
            spark=mock_spark,
        )
        names = [t['table'] for t in result['discovered_tables']]
        assert 'logs' in names
        assert 'logs__ice_staging' not in names
        assert 'logs_backup_' not in names

    def test_literal_token_with_surviving_backup_reports_incomplete_swap(self, mock_spark):
        def router(sql):
            df = MagicMock()
            sl = sql.lower()
            if 'show databases' in sl:
                df.count.return_value = 1
            elif 'show tables' in sl:
                name = sql.split("LIKE '")[1].rstrip("'").strip()
                df.count.return_value = (
                    1 if name in ('logs_backup_', 'logs__ice_staging') else 0)
                df.collect.return_value = []
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df
        mock_spark.sql.side_effect = router
        result = m.discover_hive_tables.function.__wrapped__(
            db_config={'source_database': 'db', 'table_tokens': ['logs'],
                       'inplace_migration': True, 'destination_iceberg_database': 'db',
                       'run_id': 'r1'},
            spark=mock_spark,
        )
        entry = result['discovered_tables'][0]
        assert entry['skip_code'] == 'INPLACE_CTAS_SWAP_INCOMPLETE'
        assert entry['skip_status'] == 'FAILED'
        assert 'RENAME TO db.logs' in entry['skip_message']

    def _missing_with_backup_router(self, backup_serde):
        """'logs' absent, 'logs_backup_' present with the given Serde Library, no staging."""
        def router(sql):
            df = MagicMock()
            sl = sql.lower()
            if 'show databases' in sl:
                df.count.return_value = 1
            elif 'show tables' in sl:
                name = sql.split("LIKE '")[1].rstrip("'").strip()
                df.count.return_value = 1 if name == 'logs_backup_' else 0
                df.collect.return_value = []
            elif 'describe formatted' in sl:
                df.collect.return_value = [
                    MagicMock(col_name='# Detailed Table Information', data_type=''),
                    MagicMock(col_name='Serde Library', data_type=backup_serde),
                ]
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df
        return router

    def _discover_logs(self, mock_spark):
        return m.discover_hive_tables.function.__wrapped__(
            db_config={'source_database': 'db', 'table_tokens': ['logs'],
                       'inplace_migration': True, 'destination_iceberg_database': 'db',
                       'run_id': 'r1'},
            spark=mock_spark,
        )['discovered_tables'][0]

    def test_parquet_backup_without_staging_is_table_not_found_with_a_hint(self, mock_spark):
        """system.migrate parks the original under '<tbl>_backup_' too when
        iceberg_drop_backup is false. Failing the run on that is a wrong diagnosis, and the
        rollback SQL it prints would restore a metadata-only backup sharing data files."""
        mock_spark.sql.side_effect = self._missing_with_backup_router(
            'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe')
        entry = self._discover_logs(mock_spark)
        assert entry['skip_code'] == 'TABLE_NOT_FOUND'
        assert entry['skip_status'] == 'SKIPPED'
        assert 'db.logs_backup_' in entry['skip_message']

    def test_text_backup_without_staging_still_reports_an_incomplete_swap(self, mock_spark):
        """The staging copy may already have been cleared by a later attempt. A TEXT backup
        still identifies the in-place text path — system.migrate never parks a text table —
        so the run must fail rather than report the missing table as a benign skip."""
        mock_spark.sql.side_effect = self._missing_with_backup_router(
            'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe')
        entry = self._discover_logs(mock_spark)
        assert entry['skip_code'] == 'INPLACE_CTAS_SWAP_INCOMPLETE'
        assert entry['skip_status'] == 'FAILED'
        assert 'RENAME TO db.logs' in entry['skip_message']

    def test_literal_token_with_surviving_backup_in_snapshot_mode_is_table_not_found(self, mock_spark):
        """Same shape (token missing, its '_backup_' surviving) means nothing in snapshot mode —
        a genuinely missing table beside an unrelated 'foo_backup_' must not be misread as an
        interrupted in-place swap and permanently FAILED."""
        def router(sql):
            df = MagicMock()
            sl = sql.lower()
            if 'show databases' in sl:
                df.count.return_value = 1
            elif 'show tables' in sl:
                name = sql.split("LIKE '")[1].rstrip("'").strip()
                df.count.return_value = 1 if name == 'logs_backup_' else 0
                df.collect.return_value = []
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df
        mock_spark.sql.side_effect = router
        result = m.discover_hive_tables.function.__wrapped__(
            db_config={'source_database': 'db', 'table_tokens': ['logs'],
                       'inplace_migration': False, 'destination_iceberg_database': 'db_iceberg',
                       'run_id': 'r1'},
            spark=mock_spark,
        )
        entry = result['discovered_tables'][0]
        assert entry['skip_code'] == 'TABLE_NOT_FOUND'
        assert entry['skip_status'] == 'SKIPPED'

    def test_wildcard_orphan_backup_does_not_create_a_row(self, mock_spark):
        """A tenant table named foo_backup_ must not be reported as a broken swap."""
        def router(sql):
            df = MagicMock()
            sl = sql.lower()
            if 'show databases' in sl:
                df.count.return_value = 1
            elif 'show tables' in sl:
                df.collect.return_value = [MagicMock(tableName='foo_backup_')]
                df.count.return_value = 1
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df
        mock_spark.sql.side_effect = router
        result = m.discover_hive_tables.function.__wrapped__(
            db_config={'source_database': 'db', 'table_tokens': ['*'],
                       'inplace_migration': True, 'destination_iceberg_database': 'db',
                       'run_id': 'r1'},
            spark=mock_spark,
        )
        codes = [t.get('skip_code') for t in result['discovered_tables']]
        assert 'INPLACE_CTAS_SWAP_INCOMPLETE' not in codes
        assert codes == ['NO_TABLES_MATCHED_PATTERN']


class TestMigrateTablesToIceberg:

    def _default_router(self, count=1000):
        def router(sql):
            sl = sql.lower()
            df = MagicMock()
            if 'count(*)' in sl:
                row = MagicMock()
                row.__getitem__ = lambda self, k: count
                df.collect.return_value = [row]
            elif 'show partitions' in sl:
                df.collect.return_value = []
            elif 'describe formatted' in sl:
                loc = MagicMock()
                loc.col_name = 'Location'
                loc.data_type = 's3a://bucket/t'
                df.collect.return_value = [loc]
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df
        return router

    def test_snapshot_migration(self, mock_spark, sample_iceberg_discovery):
        mock_spark.sql.side_effect = self._default_router()
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=sample_iceberg_discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        assert result['results'][0]['status'] == 'COMPLETED'

    def test_inplace_migration(self, mock_spark, sample_iceberg_discovery):
        sample_iceberg_discovery['inplace_migration'] = True
        sample_iceberg_discovery['destination_iceberg_database'] = 'sales_data_s3'
        mock_spark.sql.side_effect = self._default_router(500)
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=sample_iceberg_discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        assert any(r['migration_type'] == 'INPLACE' for r in result['results'])

    def test_failure_raises(self, mock_spark, sample_iceberg_discovery):
        # source_format must be PARQUET so system.snapshot is called (UNKNOWN/TEXT triggers CTAS)
        for tbl in sample_iceberg_discovery['discovered_tables']:
            tbl['source_format'] = 'PARQUET'

        def router(sql):
            sl = sql.lower()
            df = MagicMock()
            if 'count(*)' in sl:
                row = MagicMock()
                row.__getitem__ = lambda self, k: 1000
                df.collect.return_value = [row]
            elif 'show partitions' in sl:
                df.collect.return_value = []
            elif 'system.snapshot' in sl or 'system.migrate' in sl:
                raise Exception("Snapshot procedure failed")
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        with pytest.raises(Exception, match="Iceberg migration failed"):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=sample_iceberg_discovery, dag_run_id='dag_test',
                spark=mock_spark, ti=MagicMock(),
            )

    def test_inplace_non_v1_source_skips(self, mock_spark, sample_iceberg_discovery):
        sample_iceberg_discovery['inplace_migration'] = True
        sample_iceberg_discovery['destination_iceberg_database'] = 'sales_data_s3'

        def router(sql):
            sl = sql.lower()
            df = MagicMock()
            if 'count(*)' in sl:
                row = MagicMock()
                row.__getitem__ = lambda self, k: 1000
                df.collect.return_value = [row]
            elif 'show partitions' in sl:
                df.collect.return_value = []
            elif 'system.migrate' in sl:
                raise Exception("Cannot use non-v1 table 'sales_data_s3.transactions' as a source")
            elif 'describe formatted' in sl:
                loc = MagicMock()
                loc.col_name = 'Location'
                loc.data_type = 's3a://bucket/t'
                df.collect.return_value = [loc]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=sample_iceberg_discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )

        assert result['_has_failures'] is False
        assert result['results'][0]['status'] == 'SKIPPED'
        assert result['results'][0]['migration_type'] == 'INPLACE'

    def test_permanent_error_lets_other_tables_finish_then_fails(self, mock_spark):
        from airflow.exceptions import AirflowFailException

        discovery = {
            'source_database': 'sales_data_s3',
            'destination_iceberg_database': 'sales_data_s3_iceberg',
            'inplace_migration': False,
            'run_id': 'iceberg_run_20250101_120000_abcd1234',
            'discovered_tables': [
                {'table': 'bad_tbl', 'location': 's3a://bucket/bad_tbl',
                'source_format': 'PARQUET', 'partition_columns': []},
                {'table': 'good_tbl', 'location': 's3a://bucket/good_tbl',
                'source_format': 'PARQUET', 'partition_columns': []},
            ],
        }

        def router(sql):
            sl = sql.lower()
            df = MagicMock()
            if 'count(*)' in sl:
                row = MagicMock()
                row.__getitem__ = lambda self, k: 1000
                df.collect.return_value = [row]
            elif 'show partitions' in sl:
                df.collect.return_value = []
            elif 'system.snapshot' in sl and 'bad_tbl' in sl:
                raise Exception("Corrupt Parquet file at footer")
            elif 'describe formatted' in sl:
                loc = MagicMock()
                loc.col_name = 'Location'
                loc.data_type = 's3a://bucket/t'
                df.collect.return_value = [loc]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router

        with pytest.raises(AirflowFailException):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=discovery, dag_run_id='dag_test',
                spark=mock_spark, ti=MagicMock(),
            )

        # both tables were attempted before the permanent failure was raised
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'bad_tbl' in all_sql and 'good_tbl' in all_sql

    # ------------------------------------------------------------------
    # Partition counting and AVRO CTAS tests (new behaviour)
    # ------------------------------------------------------------------

    def _make_partitioned_discovery(self, source_format='PARQUET'):
        return {
            'source_database': 'sales_data_s3',
            'destination_iceberg_database': 'sales_data_s3_iceberg',
            'inplace_migration': False,
            'run_id': 'iceberg_run_20250101_120000_abcd1234',
            'discovered_tables': [{
                'table': 'transactions',
                'location': 's3a://bucket/transactions',
                'source_format': source_format,
                'table_type': 'EXTERNAL',
                'partition_columns': ['dt'],
            }],
        }

    def _make_inplace_discovery(self, source_format='PARQUET'):
        d = self._make_partitioned_discovery(source_format)
        d['inplace_migration'] = True
        d['destination_iceberg_database'] = d['source_database']
        return d

    class _FakeRow(dict):
        """dict that returns None for unknown keys instead of raising KeyError,
        so tests with multi-column partition_columns don't need every key set."""
        def __missing__(self, key):
            return None

    def _partition_router(self, *, hive_rows=5, non_empty=1, registered=2, iceberg=1, iceberg_with_data=None):
        """SQL router that distinguishes SELECT DISTINCT, .partitions, row-count, SHOW PARTITIONS."""
        if iceberg_with_data is None:
            iceberg_with_data = iceberg
        def router(sql):
            sl = sql.lower()
            df = MagicMock()
            row = MagicMock()
            if 'select distinct' in sl:
                df.collect.return_value = [
                    self._FakeRow(dt=f'nonempty_{i}') for i in range(non_empty)
                ]
            elif '.partitions' in sl:
                val = iceberg_with_data if 'record_count' in sl else iceberg
                row.__getitem__ = lambda self, k, _v=val: _v
                df.collect.return_value = [row]
            elif 'count(*)' in sl:
                row.__getitem__ = lambda self, k: hive_rows
                df.collect.return_value = [row]
            elif 'show partitions' in sl:
                specs = [f'dt=nonempty_{i}' for i in range(non_empty)]
                specs += [f'dt=empty_{i}' for i in range(max(registered - non_empty, 0))]
                df.collect.return_value = [[s] for s in specs]
                df.count.return_value = registered
            elif 'describe formatted' in sl:
                loc = MagicMock()
                loc.col_name = 'Location'
                loc.data_type = 's3a://bucket/t'
                df.collect.return_value = [loc]
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df
        return router

    def test_empty_table_with_registered_empty_partitions_passes(self, mock_spark):
        """0 rows, 3 registered-but-empty partitions: raw .partitions=3 (old bug ->
        mismatch), data-bearing=0 -> partition_match must be True."""
        discovery = self._make_partitioned_discovery()
        mock_spark.sql.side_effect = self._partition_router(
            hive_rows=0, non_empty=0, registered=3, iceberg=3, iceberg_with_data=0,
        )
        r = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test', spark=mock_spark, ti=MagicMock(),
        )['results'][0]
        assert r['partition_match'] is True
        assert r['iceberg_partition_count'] == 0

    def test_partially_empty_partitions_match(self, mock_spark):
        """5 registered, 3 with data -> source 3 vs dest 3 -> match."""
        discovery = self._make_partitioned_discovery()
        mock_spark.sql.side_effect = self._partition_router(
            hive_rows=10, non_empty=3, registered=5, iceberg=5, iceberg_with_data=3,
        )
        r = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test', spark=mock_spark, ti=MagicMock(),
        )['results'][0]
        assert r['partition_match'] is True

    def test_drop_backup_defaults_to_false(self, mock_spark):
        discovery = self._make_inplace_discovery()
        base = m.get_config()          # capture BEFORE patching -> no recursion
        mock_spark.sql.side_effect = self._partition_router()
        with patch.object(m, 'get_config', return_value={**base, 'iceberg_drop_backup': False}):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=discovery, dag_run_id='dag_test', spark=mock_spark, ti=MagicMock(),
            )
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'system.migrate' in all_sql
        assert 'drop_backup' not in all_sql

    def test_drop_backup_true_passes_parameter(self, mock_spark):
        discovery = self._make_inplace_discovery()
        base = m.get_config()
        mock_spark.sql.side_effect = self._partition_router()
        with patch.object(m, 'get_config', return_value={**base, 'iceberg_drop_backup': True}):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=discovery, dag_run_id='dag_test', spark=mock_spark, ti=MagicMock(),
            )
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'drop_backup => true' in all_sql

    def test_discovery_skips_backup_tables(self, mock_spark):
        def show_tables(sql):
            df = MagicMock()
            if 'show databases' in sql.lower():
                df.count.return_value = 1
                return df
            df.collect.return_value = [
                MagicMock(tableName='sales'),
                MagicMock(tableName='sales_backup_'),
                MagicMock(tableName='orders__BACKUP__'),
            ]
            return df
        mock_spark.sql.side_effect = show_tables
        resolved = m.discover_hive_tables.function.__wrapped__(
            db_config={'source_database': 'db', 'table_pattern': '*'}, spark=mock_spark,
        )
        names = {t['table'] for t in resolved.get('discovered_tables', [])}
        assert 'sales' in names
        assert 'sales_backup_' not in names
        assert 'orders__BACKUP__' not in names

    def test_stale_backup_dropped_only_when_table_is_iceberg(self, mock_spark):
        # _is_iceberg_table False -> no DROP issued
        assert m._drop_stale_inplace_backup(mock_spark, 'db', 'tbl') is None

    def test_source_partition_count_uses_select_distinct(self, mock_spark):
        """SELECT DISTINCT is used to count non-empty source partitions."""
        discovery = self._make_partitioned_discovery()
        mock_spark.sql.side_effect = self._partition_router(non_empty=2, iceberg=2)
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'select distinct' in all_sql
        assert result['results'][0]['hive_partition_count'] == 2

    def test_all_empty_partitions_pass_validation(self, mock_spark):
        """Tables with all Hive partitions empty: non-empty=0, Iceberg=0 → partition_match=True."""
        discovery = self._make_partitioned_discovery()
        # 3 partitions registered but all empty; Iceberg also sees 0
        mock_spark.sql.side_effect = self._partition_router(
            hive_rows=0, non_empty=0, registered=3, iceberg=0,
        )
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        r = result['results'][0]
        assert r['hive_partition_count'] == 0
        assert r['iceberg_partition_count'] == 0
        assert r['partition_match'] is True

    def test_empty_partition_names_written_to_tracking_insert(self, mock_spark):
        """Empty partition names must be persisted into the tracking table's
        empty_partition_names column, not just logged."""
        discovery = self._make_partitioned_discovery()
        mock_spark.sql.side_effect = self._partition_router(
            hive_rows=10, non_empty=3, registered=5, iceberg=5, iceberg_with_data=3,
        )
        m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test', spark=mock_spark, ti=MagicMock(),
        )
        insert_calls = [
            str(c) for c in mock_spark.sql.call_args_list
            if 'insert into' in str(c).lower()
        ]
        assert any('dt=empty_0' in c and 'dt=empty_1' in c for c in insert_calls)

    def test_no_empty_partitions_writes_null_to_tracking_insert(self, mock_spark):
        """When nothing is empty, the column must be NULL, not an empty string."""
        discovery = self._make_partitioned_discovery()
        mock_spark.sql.side_effect = self._partition_router(
            hive_rows=10, non_empty=2, registered=2, iceberg=2,
        )
        m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test', spark=mock_spark, ti=MagicMock(),
        )
        insert_sqls = [
            c.args[0] for c in mock_spark.sql.call_args_list
            if c.args and 'insert into' in c.args[0].lower()
        ]
        assert any(sql.rstrip().endswith('NULL\n                )') for sql in insert_sqls)

    def test_total_registered_written_to_tracking_insert(self, mock_spark):
        """SHOW PARTITIONS count (total_registered=7) is stored in the tracking INSERT SQL."""
        discovery = self._make_partitioned_discovery()
        mock_spark.sql.side_effect = self._partition_router(non_empty=1, registered=7, iceberg=1)
        m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        insert_calls = [
            str(c) for c in mock_spark.sql.call_args_list
            if 'insert into' in str(c).lower()
        ]
        assert any('7' in c for c in insert_calls), "total_registered=7 must appear in INSERT SQL"

    def test_avro_empty_table_uses_ctas(self, mock_spark):
        """Empty AVRO table uses CTAS (not system.snapshot) to avoid metadata dir collision."""
        discovery = self._make_partitioned_discovery(source_format='AVRO')
        mock_spark.sql.side_effect = self._partition_router(hive_rows=0, non_empty=0, registered=0, iceberg=0)
        m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'system.snapshot' not in all_sql
        assert 'create table' in all_sql and 'as select' in all_sql

    def test_avro_nonempty_table_uses_system_snapshot(self, mock_spark):
        """Non-empty AVRO table uses system.snapshot (CTAS only applies to empty AVRO)."""
        discovery = self._make_partitioned_discovery(source_format='AVRO')
        mock_spark.sql.side_effect = self._partition_router(hive_rows=5, non_empty=1, registered=1, iceberg=1)
        m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'system.snapshot' in all_sql

    # ------------------------------------------------------------------
    # TEXT-format and unpartitioned partition-count guard (from PR)
    # ------------------------------------------------------------------

    def test_text_format_snapshot_uses_ctas(self, mock_spark):
        """TEXT-format (LazySimpleSerDe) snapshot must use CTAS — system.snapshot rejects it."""
        discovery = {
            'source_database': 'sales_data_s3',
            'destination_iceberg_database': 'sales_data_s3_iceberg',
            'inplace_migration': False,
            'run_id': 'iceberg_run_20250101_120000_abcd1234',
            'discovered_tables': [{'table': 'logs', 'location': 's3a://bucket/logs',
                                    'source_format': 'TEXT', 'partition_columns': []}],
        }
        mock_spark.sql.side_effect = self._partition_router(hive_rows=10)
        m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'system.snapshot' not in all_sql
        assert 'create table' in all_sql and 'as select' in all_sql

    def test_text_format_partitioned_snapshot_includes_partitioned_by(self, mock_spark):
        """TEXT-format partitioned CTAS must include PARTITIONED BY with the source partition columns."""
        discovery = {
            'source_database': 'sales_data_s3',
            'destination_iceberg_database': 'sales_data_s3_iceberg',
            'inplace_migration': False,
            'run_id': 'iceberg_run_20250101_120000_abcd1234',
            'discovered_tables': [{'table': 'logs', 'location': 's3a://bucket/logs',
                                    'source_format': 'TEXT', 'partition_columns': ['dt', 'region']}],
        }
        mock_spark.sql.side_effect = self._partition_router(hive_rows=10, non_empty=2, registered=2, iceberg=2)
        m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'partitioned by' in all_sql
        assert 'dt' in all_sql and 'region' in all_sql
        assert 'system.snapshot' not in all_sql

    def _text_inplace_discovery(self, *, table_type='EXTERNAL', partition_columns=None):
        return {
            'source_database': 'sales_data_s3',
            'destination_iceberg_database': 'sales_data_s3',
            'inplace_migration': True,
            'run_id': 'iceberg_run_20250101_120000_abcd1234',
            'discovered_tables': [{
                'table': 'logs',
                'location': 's3a://bucket/logs',
                'source_format': 'TEXT',
                'table_type': table_type,
                'partition_columns': partition_columns or [],
            }],
        }

    def _text_inplace_router(self, *, hive_rows=10, existing=('logs',), staging_rows=None):
        """Existence for the repair check, counts for the gate, columns for the schema gate."""
        staging_rows = hive_rows if staging_rows is None else staging_rows
        def router(sql):
            df = MagicMock()
            sl = sql.lower()
            if 'show tables' in sl:
                name = sql.split("LIKE '")[1].rstrip("'").strip()
                df.count.return_value = 1 if name in existing else 0
            elif 'select distinct' in sl:
                # Real values, not a count: the caller derives the non-empty partition
                # keys from these and diffs them against SHOW PARTITIONS.
                df.collect.return_value = [_keyed_row(v) for v in ('a', 'b')]
            elif '.partitions' in sl:
                row = MagicMock()
                row.__getitem__ = lambda self, k: 2
                df.collect.return_value = [row]
            elif 'count(*)' in sl:
                rows = staging_rows if '__ice_staging' in sl else hive_rows
                row = MagicMock()
                row.__getitem__ = lambda self, k, _v=rows: _v
                df.collect.return_value = [row]
            elif 'show partitions' in sl:
                df.count.return_value = 2
                df.collect.return_value = [_keyed_row(f"dt={v}") for v in ('a', 'b')]
            elif 'describe formatted' in sl:
                df.collect.return_value = [
                    MagicMock(col_name='Location', data_type='s3a://bucket/logs_iceberg')
                ]
            elif sl.strip().startswith('describe '):
                df.collect.return_value = [MagicMock(col_name='id', data_type='varchar(20)')]
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df
        return router

    def test_text_inplace_copies_and_swaps(self, mock_spark):
        mock_spark.sql.side_effect = self._text_inplace_router()
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=self._text_inplace_discovery(), dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        row = result['results'][0]
        assert row['status'] == 'COMPLETED'
        assert row['migration_type'] == 'INPLACE_CTAS'
        assert row['destination_table'] == 'sales_data_s3.logs'
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'system.migrate' not in issued
        assert 'RENAME TO sales_data_s3.logs_backup_' in issued
        assert 'INPLACE_CTAS' in issued          # reached the tracking insert

    def test_text_inplace_disabled_by_flag_is_skipped(self, mock_spark):
        base = m.get_config()
        mock_spark.sql.side_effect = self._text_inplace_router()
        with patch.object(m, 'get_config',
                          return_value={**base, 'iceberg_inplace_text_ctas': False}):
            result = m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=self._text_inplace_discovery(), dag_run_id='dag_test',
                spark=mock_spark, ti=MagicMock(),
            )
        row = result['results'][0]
        assert row['status'] == 'SKIPPED'
        assert row['reason_code'] == 'TEXT_FORMAT_INPLACE_UNSUPPORTED'
        assert row['migration_type'] == 'INPLACE'
        assert result['_has_failures'] is False
        assert 'CREATE TABLE' not in ' '.join(str(c) for c in mock_spark.sql.call_args_list)

    def test_managed_text_table_is_skipped(self, mock_spark):
        mock_spark.sql.side_effect = self._text_inplace_router()
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=self._text_inplace_discovery(table_type='MANAGED'),
            dag_run_id='dag_test', spark=mock_spark, ti=MagicMock(),
        )
        row = result['results'][0]
        assert row['status'] == 'SKIPPED'
        assert row['reason_code'] == 'MANAGED_TEXT_INPLACE_UNSUPPORTED'
        assert 'CREATE TABLE' not in ' '.join(str(c) for c in mock_spark.sql.call_args_list)

    def test_missing_table_type_is_skipped_as_managed(self, mock_spark):
        mock_spark.sql.side_effect = self._text_inplace_router()
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=self._text_inplace_discovery(table_type=None),
            dag_run_id='dag_test', spark=mock_spark, ti=MagicMock(),
        )
        assert result['results'][0]['reason_code'] == 'MANAGED_TEXT_INPLACE_UNSUPPORTED'

    def test_backup_conflict_is_skipped_without_touching_anything(self, mock_spark):
        mock_spark.sql.side_effect = self._text_inplace_router(existing=('logs', 'logs_backup_'))
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=self._text_inplace_discovery(), dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        row = result['results'][0]
        assert row['status'] == 'SKIPPED'
        assert row['reason_code'] == 'INPLACE_CTAS_BACKUP_CONFLICT'
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'CREATE TABLE' not in issued
        assert 'RENAME TO' not in issued

    def test_unrecoverable_state_is_failed(self, mock_spark):
        # A recorded FAILED table makes the task raise after pushing its result to
        # XCom, so read the result from the mocked ti rather than the return value.
        ti = MagicMock()
        mock_spark.sql.side_effect = self._text_inplace_router(existing=())
        with pytest.raises(Exception, match='Iceberg migration failed'):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=self._text_inplace_discovery(), dag_run_id='dag_test',
                spark=mock_spark, ti=ti,
            )
        row = ti.xcom_push.call_args.kwargs['value']['results'][0]
        assert row['status'] == 'FAILED'
        assert row['reason_code'] == 'INPLACE_CTAS_SWAP_INCOMPLETE'
        assert row['migration_type'] == 'INPLACE_CTAS'

    def test_blocked_rollback_is_failed_not_skipped(self, mock_spark):
        """Source gone and the backup unprovable means the table is missing from the
        metastore. Recording that as SKIPPED would let the run finish green."""
        ti = MagicMock()
        # The router reports every backup at s3a://bucket/logs_iceberg, which does not
        # match the discovery location — so the rollback cannot be confirmed safe.
        mock_spark.sql.side_effect = self._text_inplace_router(existing=('logs_backup_',))
        with pytest.raises(Exception, match='Iceberg migration failed'):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=self._text_inplace_discovery(), dag_run_id='dag_test',
                spark=mock_spark, ti=ti,
            )
        row = ti.xcom_push.call_args.kwargs['value']['results'][0]
        assert row['status'] == 'FAILED'
        assert row['reason_code'] == 'INPLACE_CTAS_SWAP_INCOMPLETE'
        assert row['migration_type'] == 'INPLACE_CTAS'
        assert 'RENAME TO' not in ' '.join(str(c) for c in mock_spark.sql.call_args_list)

    def test_retry_of_an_already_swapped_table_stays_completed(self, mock_spark):
        """A retry driven by another table's failure re-runs this one. Recording it as
        SKIPPED would overwrite its COMPLETED tracking row and drop it out of validation."""
        base = self._text_inplace_router()

        def router(sql):
            if 'describe formatted' in sql.lower():
                df = MagicMock()
                df.collect.return_value = [
                    MagicMock(col_name='Provider', data_type='iceberg'),
                    MagicMock(col_name='Location', data_type='s3a://bucket/logs_iceberg'),
                ]
                return df
            return base(sql)

        mock_spark.sql.side_effect = router
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=self._text_inplace_discovery(), dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        row = result['results'][0]
        assert row['status'] == 'COMPLETED'
        assert row['migration_type'] == 'INPLACE_CTAS'
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'AS SELECT' not in issued
        assert 'RENAME TO' not in issued

    def test_reverify_survives_show_partitions_failing_on_the_iceberg_table(self, mock_spark):
        """On the re-verify path the source name is already Iceberg, and SHOW PARTITIONS rejects
        V2 tables — it throws after the DISTINCT count succeeded. Keeping that DISTINCT value
        against a dest count of 0 would report a partition mismatch no retry can ever clear."""
        base = self._text_inplace_router()

        def router(sql):
            sl = sql.lower()
            if 'show partitions' in sl:
                raise Exception('SHOW PARTITIONS is not supported for v2 tables')
            if 'describe formatted' in sl:
                df = MagicMock()
                df.collect.return_value = [
                    MagicMock(col_name='Provider', data_type='iceberg'),
                    MagicMock(col_name='Location', data_type='s3a://bucket/logs_iceberg'),
                ]
                return df
            return base(sql)

        mock_spark.sql.side_effect = router
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=self._text_inplace_discovery(partition_columns=['dt']),
            dag_run_id='dag_test', spark=mock_spark, ti=MagicMock(),
        )
        row = result['results'][0]
        assert row['status'] == 'COMPLETED'
        assert row['partition_match'] is True
        assert row['hive_partition_count'] == 0

    def test_pending_rollback_outranks_another_tables_permanent_error(self, mock_spark):
        """permanent_fail suppresses every remaining retry, and the retry is what rolls a partial
        swap back — one table's corrupt file must not strand another table mid-swap."""
        ti = MagicMock()
        discovery = self._text_inplace_discovery()
        discovery['discovered_tables'] = [
            {**discovery['discovered_tables'][0], 'table': 'logs_a'},
            {**discovery['discovered_tables'][0], 'table': 'logs_b'},
        ]
        base = self._text_inplace_router(existing=('logs_a', 'logs_b'))

        def router(sql):
            sl = sql.lower()
            if 'logs_a' in sl and 'as select' in sl:
                raise Exception('Corrupt Parquet file at footer')
            if 'logs_b' in sl and 'rename to' in sl:
                raise Exception('metastore connection reset')
            return base(sql)

        mock_spark.sql.side_effect = router
        with (
            patch.object(m, 'permanent_fail',
                         side_effect=AssertionError('permanent_fail must not be called')),
            pytest.raises(Exception, match='Iceberg migration failed'),
        ):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=discovery, dag_run_id='dag_test', spark=mock_spark, ti=ti,
            )
        rows = {r['source_table']: r for r in ti.xcom_push.call_args.kwargs['value']['results']}
        assert rows['sales_data_s3.logs_b']['reason_code'] == 'INPLACE_CTAS_SWAP_INCOMPLETE'

    def _skip_entry_discovery(self, code, status='FAILED', table='t_gone'):
        d = self._text_inplace_discovery()
        d['discovered_tables'] = [{
            'table': table, 'location': 's3a://bucket/logs', 'source_format': 'TEXT',
            'table_type': 'EXTERNAL', 'partition_columns': [],
            'skip_code': code, 'skip_status': status,
            'skip_message': f'{code} detail from discovery',
        }]
        return d

    def test_discovery_swap_incomplete_skip_suppresses_retries(self, mock_spark):
        """The table has to be restored by hand and never reaches the repair path, so each
        retry only re-records the same row — 3x the task duration for no chance of success."""
        mock_spark.sql.side_effect = self._text_inplace_router(existing=())
        with (
            patch.object(m, 'permanent_fail', side_effect=Exception('permanent')) as pf,
            pytest.raises(Exception, match='permanent'),
        ):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=self._skip_entry_discovery('INPLACE_CTAS_SWAP_INCOMPLETE'),
                dag_run_id='dag_test', spark=mock_spark, ti=MagicMock(),
            )
        assert pf.called

    def test_other_failed_discovery_skips_keep_their_retries(self, mock_spark):
        """PERMISSION_DENIED and METADATA_READ_ERROR can be a transient metastore blip,
        where the retry is exactly what fixes them."""
        ti = MagicMock()
        mock_spark.sql.side_effect = self._text_inplace_router(existing=())
        with (
            patch.object(m, 'permanent_fail',
                         side_effect=AssertionError('permanent_fail must not be called')),
            pytest.raises(Exception, match='Iceberg migration failed'),
        ):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=self._skip_entry_discovery('PERMISSION_DENIED'),
                dag_run_id='dag_test', spark=mock_spark, ti=ti,
            )
        assert ti.xcom_push.call_args.kwargs['value']['results'][0]['status'] == 'FAILED'

    def test_failure_summary_names_the_failed_tables(self, mock_spark):
        """The summary reaches XCom, the HTML report and the notification email, so ending it
        at 'table(s): ' with no names strands whoever reads it."""
        ti = MagicMock()
        mock_spark.sql.side_effect = self._text_inplace_router(existing=())
        with pytest.raises(Exception, match='Iceberg migration failed'):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=self._skip_entry_discovery('PERMISSION_DENIED', table='t_gone'),
                dag_run_id='dag_test', spark=mock_spark, ti=ti,
            )
        summary = ti.xcom_push.call_args.kwargs['value']['_failure_summary']
        assert 'sales_data_s3.t_gone' in summary
        assert not summary.rstrip().endswith(':')

    def test_permanent_ctas_error_short_circuits_before_any_rename(self, mock_spark):
        """Nothing has been renamed yet, so retrying only re-copies the table. The
        rollback guarantee only needs to hold once a RENAME has been issued."""
        ti = MagicMock()
        base = self._text_inplace_router()

        def router(sql):
            if 'as select' in sql.lower():
                raise Exception(
                    "A column or function parameter with name `dt` cannot be found. "
                    "Verify the spelling and correctness of the column name."
                )
            return base(sql)

        mock_spark.sql.side_effect = router
        with (
            patch.object(m, 'permanent_fail', side_effect=Exception('permanent')) as pf,
            pytest.raises(Exception, match='permanent'),
        ):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=self._text_inplace_discovery(), dag_run_id='dag_test',
                spark=mock_spark, ti=ti,
            )
        assert pf.called

    def test_failed_staging_cleanup_does_not_mask_the_verification_error(self, mock_spark):
        """The DROP that clears the staging copy runs while a TextCtasError is in flight;
        letting it throw would replace the row-count detail with a generic classification."""
        ti = MagicMock()
        base = self._text_inplace_router(hive_rows=10, staging_rows=9)

        def router(sql):
            if sql.lower().strip().startswith('drop table'):
                raise Exception('metastore unavailable')
            return base(sql)

        mock_spark.sql.side_effect = router
        with pytest.raises(Exception, match='Iceberg migration failed'):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=self._text_inplace_discovery(), dag_run_id='dag_test',
                spark=mock_spark, ti=ti,
            )
        row = ti.xcom_push.call_args.kwargs['value']['results'][0]
        assert row['reason_code'] == 'INPLACE_CTAS_VERIFY_FAILED'
        assert '9 rows' in row['error']

    def test_repair_exception_for_one_table_does_not_block_the_next(self, mock_spark):
        """_repair_partial_text_swap is the only pre-check that issues SQL, so it is the only
        one that can raise. That must not escape the per-table loop and abandon the rest of
        the group — it must be recorded as FAILED and the loop must move on."""
        ti = MagicMock()
        discovery = self._text_inplace_discovery()
        discovery['discovered_tables'] = [
            {**discovery['discovered_tables'][0], 'table': 'logs_a'},
            {**discovery['discovered_tables'][0], 'table': 'logs_b'},
        ]
        mock_spark.sql.side_effect = self._text_inplace_router(existing=('logs_a', 'logs_b'))
        with (
            patch.object(m, '_repair_partial_text_swap', side_effect=[Exception('boom'), 'READY']),
            pytest.raises(Exception, match='Iceberg migration failed'),
        ):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=discovery, dag_run_id='dag_test', spark=mock_spark, ti=ti,
            )
        rows = {r['source_table']: r for r in ti.xcom_push.call_args.kwargs['value']['results']}
        failed = rows['sales_data_s3.logs_a']
        assert failed['status'] == 'FAILED'
        assert failed['reason_code'] == 'INPLACE_CTAS_SWAP_INCOMPLETE'
        assert failed['migration_type'] == 'INPLACE_CTAS'
        assert rows['sales_data_s3.logs_b']['status'] == 'COMPLETED'

    def test_verification_failure_is_failed_not_permanent(self, mock_spark):
        """TextCtasError must not suppress retries — the retry performs the rollback."""
        ti = MagicMock()
        mock_spark.sql.side_effect = self._text_inplace_router(hive_rows=10, staging_rows=9)
        with (
            patch.object(m, 'permanent_fail',
                         side_effect=AssertionError('permanent_fail must not be called')),
            pytest.raises(Exception, match='Iceberg migration failed'),
        ):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=self._text_inplace_discovery(), dag_run_id='dag_test',
                spark=mock_spark, ti=ti,
            )
        row = ti.xcom_push.call_args.kwargs['value']['results'][0]
        assert row['status'] == 'FAILED'
        assert row['reason_code'] == 'INPLACE_CTAS_VERIFY_FAILED'
        assert row['migration_type'] == 'INPLACE_CTAS'
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'DROP TABLE IF EXISTS sales_data_s3.logs__ice_staging PURGE' in issued
        assert 'RENAME TO' not in issued

    def test_swap_window_failure_with_permanent_marker_still_keeps_retries(self, mock_spark):
        """A RENAME failure inside the swap window can raise a raw AnalysisException whose
        message hits a permanent-error marker (e.g. TABLE_OR_VIEW_NOT_FOUND). Permanently
        failing here would abandon the source parked as {tbl}_backup_ with no {tbl} in the
        metastore and no further retry to roll it back — so this must stay retriable."""
        ti = MagicMock()
        base_router = self._text_inplace_router()

        def router(sql):
            if 'rename to' in sql.lower():
                raise Exception("TABLE_OR_VIEW_NOT_FOUND: Table or view not found: sales_data_s3.logs")
            return base_router(sql)

        mock_spark.sql.side_effect = router
        with (
            patch.object(m, 'permanent_fail',
                         side_effect=AssertionError('permanent_fail must not be called')),
            pytest.raises(Exception, match='Iceberg migration failed'),
        ):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=self._text_inplace_discovery(), dag_run_id='dag_test',
                spark=mock_spark, ti=ti,
            )
        row = ti.xcom_push.call_args.kwargs['value']['results'][0]
        assert row['status'] == 'FAILED'

    def test_empty_text_table_completes(self, mock_spark):
        """0 rows on both sides passes the gate; partition validation is trivially satisfied."""
        mock_spark.sql.side_effect = self._text_inplace_router(hive_rows=0)
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=self._text_inplace_discovery(partition_columns=['dt']),
            dag_run_id='dag_test', spark=mock_spark, ti=MagicMock(),
        )
        row = result['results'][0]
        assert row['status'] == 'COMPLETED'
        assert row['migration_type'] == 'INPLACE_CTAS'
        assert row['partition_match'] is True

    def test_parquet_inplace_still_uses_system_migrate(self, mock_spark):
        """Regression guard: the zero-copy path must be untouched."""
        mock_spark.sql.side_effect = self._partition_router()
        m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=self._make_inplace_discovery(source_format='PARQUET'),
            dag_run_id='dag_test', spark=mock_spark, ti=MagicMock(),
        )
        assert 'system.migrate' in ' '.join(
            str(c) for c in mock_spark.sql.call_args_list).lower()

    def test_unpartitioned_table_skips_iceberg_partitions_query(self, mock_spark):
        """Unpartitioned tables must NOT query .partitions — Iceberg returns 1 spurious row there."""
        discovery = {
            'source_database': 'sales_data_s3',
            'destination_iceberg_database': 'sales_data_s3_iceberg',
            'inplace_migration': False,
            'run_id': 'iceberg_run_20250101_120000_abcd1234',
            'discovered_tables': [{'table': 'events', 'location': 's3a://bucket/events',
                                    'source_format': 'PARQUET', 'partition_columns': []}],
        }
        mock_spark.sql.side_effect = self._partition_router(hive_rows=500)
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert '.partitions' not in all_sql
        assert result['results'][0]['partition_match'] is True


class TestUpdateMigrationDurations:

    def test_updates_duration_in_tracking(self, mock_spark, sample_iceberg_migration_result, mock_iceberg_retry):
        m.update_migration_durations.function(
            migration_result=sample_iceberg_migration_result, spark=mock_spark,
        )
        assert mock_iceberg_retry.called


class TestValidateIcebergTables:

    def test_matching_schema(self, mock_spark, sample_iceberg_migration_result):
        def router(sql):
            df = MagicMock()
            if 'describe' in sql.lower():
                df.collect.return_value = [MagicMock(col_name='id', data_type='bigint')]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.validate_iceberg_tables.function.__wrapped__(
            migration_result=sample_iceberg_migration_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['schema_match'] is True

    def test_schema_mismatch(self, mock_spark, sample_iceberg_migration_result):
        call_tracker = [0]

        def router(sql):
            df = MagicMock()
            if 'describe' in sql.lower():
                call_tracker[0] += 1
                dtype = 'bigint' if call_tracker[0] % 2 == 1 else 'int'
                df.collect.return_value = [MagicMock(col_name='id', data_type=dtype)]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.validate_iceberg_tables.function.__wrapped__(
            migration_result=sample_iceberg_migration_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['schema_match'] is False

    def test_smallint_normalized_to_int_schema_match(self, mock_spark, sample_iceberg_migration_result):
        """SMALLINT in Hive source must match INT in Iceberg dest (Iceberg promotes it)."""
        call_n = [0]

        def router(sql):
            df = MagicMock()
            if 'describe' in sql.lower():
                call_n[0] += 1
                dtype = 'smallint' if call_n[0] % 2 == 1 else 'int'
                df.collect.return_value = [MagicMock(col_name='region_code', data_type=dtype)]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.validate_iceberg_tables.function.__wrapped__(
            migration_result=sample_iceberg_migration_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['schema_match'] is True

    def test_tinyint_normalized_to_int_schema_match(self, mock_spark, sample_iceberg_migration_result):
        """TINYINT in Hive source must match INT in Iceberg dest (Iceberg promotes it)."""
        call_n = [0]

        def router(sql):
            df = MagicMock()
            if 'describe' in sql.lower():
                call_n[0] += 1
                dtype = 'tinyint' if call_n[0] % 2 == 1 else 'int'
                df.collect.return_value = [MagicMock(col_name='flag', data_type=dtype)]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.validate_iceberg_tables.function.__wrapped__(
            migration_result=sample_iceberg_migration_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['schema_match'] is True


class TestUpdateIcebergValidationStatus:

    def _make_validation_result(self, sample, row_count_match=True):
        return {
            **sample,
            'validation_results': [{
                'source_table': 'transactions',
                'destination_table': 'sales_data_s3_iceberg.transactions',
                'status': 'COMPLETED', 'source_hive_row_count': 1000,
                'dest_iceberg_row_count': 1000, 'row_count_match': row_count_match,
                'source_hive_partition_count': 2, 'dest_iceberg_partition_count': 2,
                'partition_count_match': True, 'schema_match': True,
                'schema_differences': '', 'per_table_validation_duration': 4.5,
                'error': None,
            }],
            '_task_duration': 4.5,
        }

    def test_sets_validated_on_match(self, mock_spark, sample_iceberg_migration_result, mock_iceberg_retry):
        vr = self._make_validation_result(sample_iceberg_migration_result)
        m.update_iceberg_validation_status.function(validation_result=vr, spark=mock_spark)
        assert any('VALIDATED' in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_sets_validation_failed_on_mismatch(self, mock_spark, sample_iceberg_migration_result, mock_iceberg_retry):
        vr = self._make_validation_result(sample_iceberg_migration_result, row_count_match=False)
        m.update_iceberg_validation_status.function(validation_result=vr, spark=mock_spark)
        assert any('VALIDATION_FAILED' in str(c) for c in mock_iceberg_retry.call_args_list)


class TestGenerateIcebergHtmlReport:

    def test_generates_report_and_writes_to_s3(self, mock_spark, sample_iceberg_run_id):
        tbl_row = SimpleNamespace(
            source_database='sales_s3', source_table='transactions',
            migration_type='SNAPSHOT', destination_table='sales_s3_iceberg.transactions',
            status='VALIDATED', migration_duration_seconds=45.0,
            validation_duration_seconds=4.5, validation_status='COMPLETED',
            row_count_match=True, partition_count_match=True, schema_match=True,
            source_hive_row_count=1000, destination_iceberg_row_count=1000,
            source_hive_partition_count=2, dest_iceberg_partition_count=2,
        )
        ivs_row = MagicMock()
        ivs_row.__getitem__ = lambda self, k: 1 if k == 'total_tables_validated' else 0
        ivs_row.total_tables_validated = 1
        ivs_row.tables_passed_validation = 1
        ivs_row.tables_failed_validation = 0
        ivs_row.total_row_count_mismatches = 0
        ivs_row.total_partition_count_mismatches = 0
        ivs_row.total_schema_mismatches = 0

        def router(sql):
            df = MagicMock()
            if 'order by' in sql.lower():
                df.collect.return_value = [tbl_row]
            elif 'sum(case when' in sql.lower():
                df.collect.return_value = [ivs_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.generate_iceberg_html_report.function(run_id=sample_iceberg_run_id, spark=mock_spark)
        assert result['report_path'].endswith('.html')
        assert sample_iceberg_run_id in result['report_path']

    def test_generates_report_when_row_has_no_error_field(self, mock_spark, sample_iceberg_run_id):
        tbl_row = SimpleNamespace(
            source_database='sales_s3', source_table='transactions',
            migration_type='INPLACE', destination_table='sales_s3.transactions',
            status='SKIPPED', migration_duration_seconds=12.0,
            validation_duration_seconds=None, validation_status=None,
            row_count_match=None, partition_count_match=None, schema_match=None,
            source_hive_row_count=None, destination_iceberg_row_count=None,
            source_hive_partition_count=None, dest_iceberg_partition_count=None,
        )
        ivs_row = MagicMock()
        ivs_row.__getitem__ = lambda self, k: 0
        ivs_row.total_tables_validated = 0
        ivs_row.tables_passed_validation = 0
        ivs_row.tables_failed_validation = 0
        ivs_row.total_row_count_mismatches = 0
        ivs_row.total_partition_count_mismatches = 0
        ivs_row.total_schema_mismatches = 0

        def router(sql):
            df = MagicMock()
            if 'order by' in sql.lower():
                df.collect.return_value = [tbl_row]
            elif 'sum(case when' in sql.lower():
                df.collect.return_value = [ivs_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.generate_iceberg_html_report.function(run_id=sample_iceberg_run_id, spark=mock_spark)
        assert result['report_path'].endswith('.html')

    def test_html_report_includes_hive_total_partitions_column(self, mock_spark, sample_iceberg_run_id):
        """HTML report must contain 'Source Partitions (non-empty)' and 'Hive Total Partitions' headers."""
        tbl_row = SimpleNamespace(
            source_database='sales_s3', source_table='transactions',
            migration_type='SNAPSHOT', destination_table='sales_s3_iceberg.transactions',
            status='VALIDATED', migration_duration_seconds=10.0,
            validation_duration_seconds=1.0, validation_status='COMPLETED',
            row_count_match=True, partition_count_match=True, schema_match=True,
            source_hive_row_count=5, destination_iceberg_row_count=5,
            source_hive_partition_count=1, dest_iceberg_partition_count=1,
        )
        ivs_row = MagicMock()
        ivs_row.__getitem__ = lambda self, k: 1 if k == 'total_tables_validated' else 0
        ivs_row.total_tables_validated = 1
        ivs_row.tables_passed_validation = 1
        ivs_row.tables_failed_validation = 0
        ivs_row.total_row_count_mismatches = 0
        ivs_row.total_partition_count_mismatches = 0
        ivs_row.total_schema_mismatches = 0

        def router(sql):
            df = MagicMock()
            if 'order by' in sql.lower():
                df.collect.return_value = [tbl_row]
            elif 'sum(case when' in sql.lower():
                df.collect.return_value = [ivs_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        m.generate_iceberg_html_report.function(run_id=sample_iceberg_run_id, spark=mock_spark)

        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        written_bytes = fs_mock.create.return_value.write.call_args[0][0]
        html = written_bytes.decode('utf-8')
        assert 'Hive Total Partitions' in html
        assert 'Source Partitions (non-empty)' in html

    class _Row(SimpleNamespace):
        """Row stub that also answers asDict(), which _row_value() needs to see error_message."""

        def asDict(self, recursive=False):
            return dict(self.__dict__)

    def test_html_report_shows_empty_partition_names(self, mock_spark, sample_iceberg_run_id):
        """Empty partition names persisted in the tracking table must render in the
        HTML report's 'Empty Partitions (dropped)' column."""
        tbl_row = self._Row(
            source_database='sales_s3', source_table='transactions',
            migration_type='SNAPSHOT', destination_table='sales_s3_iceberg.transactions',
            status='VALIDATED', migration_duration_seconds=10.0,
            validation_duration_seconds=1.0, validation_status='COMPLETED',
            row_count_match=True, partition_count_match=False, schema_match=True,
            source_hive_row_count=5, destination_iceberg_row_count=5,
            source_hive_partition_count=3, source_hive_total_partition_count=5,
            dest_iceberg_partition_count=3,
            empty_partition_names='dt=2024-01-01, dt=2024-01-02',
        )
        ivs_row = MagicMock()
        ivs_row.__getitem__ = lambda self, k: 1 if k == 'total_tables_validated' else 0
        ivs_row.total_tables_validated = 1
        ivs_row.tables_passed_validation = 0
        ivs_row.tables_failed_validation = 1
        ivs_row.total_row_count_mismatches = 0
        ivs_row.total_partition_count_mismatches = 1
        ivs_row.total_schema_mismatches = 0

        def router(sql):
            df = MagicMock()
            if 'order by' in sql.lower():
                df.collect.return_value = [tbl_row]
            elif 'sum(case when' in sql.lower():
                df.collect.return_value = [ivs_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        m.generate_iceberg_html_report.function(run_id=sample_iceberg_run_id, spark=mock_spark)

        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        written_bytes = fs_mock.create.return_value.write.call_args[0][0]
        html = written_bytes.decode('utf-8')
        assert 'Empty Partitions (dropped)' in html
        assert 'dt=2024-01-01' in html
        assert 'dt=2024-01-02' in html

    def test_html_report_empty_partitions_cell_shows_zero_when_none(self, mock_spark, sample_iceberg_run_id):
        """Tables with no empty partitions (or rows written before this column existed)
        must render '0', not raise, and not show stray partition text."""
        tbl_row = SimpleNamespace(
            source_database='sales_s3', source_table='transactions',
            migration_type='SNAPSHOT', destination_table='sales_s3_iceberg.transactions',
            status='VALIDATED', migration_duration_seconds=10.0,
            validation_duration_seconds=1.0, validation_status='COMPLETED',
            row_count_match=True, partition_count_match=True, schema_match=True,
            source_hive_row_count=5, destination_iceberg_row_count=5,
            source_hive_partition_count=1, dest_iceberg_partition_count=1,
        )
        ivs_row = MagicMock()
        ivs_row.__getitem__ = lambda self, k: 1 if k == 'total_tables_validated' else 0
        ivs_row.total_tables_validated = 1
        ivs_row.tables_passed_validation = 1
        ivs_row.tables_failed_validation = 0
        ivs_row.total_row_count_mismatches = 0
        ivs_row.total_partition_count_mismatches = 0
        ivs_row.total_schema_mismatches = 0

        def router(sql):
            df = MagicMock()
            if 'order by' in sql.lower():
                df.collect.return_value = [tbl_row]
            elif 'sum(case when' in sql.lower():
                df.collect.return_value = [ivs_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        m.generate_iceberg_html_report.function(run_id=sample_iceberg_run_id, spark=mock_spark)

        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        written_bytes = fs_mock.create.return_value.write.call_args[0][0]
        html = written_bytes.decode('utf-8')
        assert '<td class="metric">0</td>' in html

    def test_status_column_carries_reason_and_no_per_table_detail_section(
        self, mock_spark, sample_iceberg_run_id,
    ):
        """Skip reason renders under the status badge; the per-table detail table is gone."""
        tbl_row = self._Row(
            source_database='sales_s3', source_table='transactions',
            migration_type='SNAPSHOT', destination_table='sales_s3_iceberg.transactions',
            status='SKIPPED', migration_duration_seconds=None,
            validation_duration_seconds=None, validation_status=None,
            row_count_match=None, partition_count_match=None, schema_match=None,
            source_hive_row_count=None, destination_iceberg_row_count=None,
            source_hive_partition_count=None, dest_iceberg_partition_count=None,
            error_message="[TABLE_NOT_FOUND] Table 'sales_s3.transactions' does not exist",
        )
        ivs_row = MagicMock()
        ivs_row.__getitem__ = lambda self, k: 0
        ivs_row.total_tables_validated = 0

        def router(sql):
            df = MagicMock()
            if 'order by' in sql.lower():
                df.collect.return_value = [tbl_row]
            elif 'sum(case when' in sql.lower():
                df.collect.return_value = [ivs_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        m.generate_iceberg_html_report.function(run_id=sample_iceberg_run_id, spark=mock_spark)

        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        html = fs_mock.create.return_value.write.call_args[0][0].decode('utf-8')

        # Status cell: badge, then the human label, then the raw code.
        status_cell = html.split('<td class="status-cell">')[1].split('</td>')[0]
        assert 'SKIPPED' in status_cell
        assert 'Table not found in metastore' in status_cell
        assert 'TABLE_NOT_FOUND' in status_cell

        # The reason breakdown stays; the per-table detail table is removed.
        assert 'Reason breakdown' in html
        assert 'Per-table detail' not in html

    def test_status_column_has_no_reason_markup_for_successful_table(
        self, mock_spark, sample_iceberg_run_id,
    ):
        """A VALIDATED table has no error_message, so nothing is added under the badge."""
        tbl_row = self._Row(
            source_database='sales_s3', source_table='transactions',
            migration_type='SNAPSHOT', destination_table='sales_s3_iceberg.transactions',
            status='VALIDATED', migration_duration_seconds=10.0,
            validation_duration_seconds=1.0, validation_status='COMPLETED',
            row_count_match=True, partition_count_match=True, schema_match=True,
            source_hive_row_count=5, destination_iceberg_row_count=5,
            source_hive_partition_count=1, dest_iceberg_partition_count=1,
            error_message=None,
        )
        ivs_row = MagicMock()
        ivs_row.__getitem__ = lambda self, k: 0
        ivs_row.total_tables_validated = 0

        def router(sql):
            df = MagicMock()
            if 'order by' in sql.lower():
                df.collect.return_value = [tbl_row]
            elif 'sum(case when' in sql.lower():
                df.collect.return_value = [ivs_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        m.generate_iceberg_html_report.function(run_id=sample_iceberg_run_id, spark=mock_spark)

        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        html = fs_mock.create.return_value.write.call_args[0][0].decode('utf-8')

        status_cell = html.split('<td class="status-cell">')[1].split('</td>')[0]
        assert 'VALIDATED' in status_cell
        assert 'status-reason' not in status_cell
        assert 'reason-key' not in status_cell


class TestSendIcebergReportEmail:

    def test_skips_when_no_recipients(self, mock_spark, sample_iceberg_run_id):
        with patch('airflow.models.Variable.get', return_value=''):
            result = m.send_iceberg_report_email.function(
                report_result={'report_path': 's3a://bucket/report.html'},
                run_id=sample_iceberg_run_id, spark=mock_spark,
            )
        assert result['sent'] is False

    def test_sends_when_recipients_configured(self, mock_spark, sample_iceberg_run_id):
        reader_mock = MagicMock()
        reader_mock.readLine.side_effect = ['<html>iceberg report</html>', None]
        mock_spark._jvm.java.io.BufferedReader.return_value = reader_mock

        with patch('airflow.utils.email.send_email'), \
             patch('tempfile.NamedTemporaryFile') as mock_tmp, \
             patch('os.unlink'):
            tmp_inst = MagicMock()
            tmp_inst.name = '/tmp/ice_report.html'
            mock_tmp.return_value = tmp_inst
            result = m.send_iceberg_report_email.function(
                report_result={'report_path': 's3a://bucket/report.html'},
                run_id=sample_iceberg_run_id, spark=mock_spark,
            )
        assert result['sent'] is True


class TestFinalizeIcebergRun:

    def test_returns_completed(self, mock_spark, sample_iceberg_run_id):
        stats = MagicMock()
        stats.__getitem__ = lambda self, k: {
            'total': 2, 'successful': 2, 'failed': 0, 'skipped': 0, 'count_mismatches': 0,
        }[k]
        type_row = MagicMock()
        type_row.__getitem__ = lambda self, k: 'SNAPSHOT'

        def router(sql):
            df = MagicMock()
            if 'count_mismatches' in sql.lower():
                df.collect.return_value = [stats]
            elif 'migration_type' in sql.lower():
                df.collect.return_value = [type_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.finalize_iceberg_run.function(run_id=sample_iceberg_run_id, spark=mock_spark)
        assert result['status'] == 'COMPLETED'


class TestInplaceTextCtasFlag:

    def test_defaults_to_true(self):
        assert m.get_config()['iceberg_inplace_text_ctas'] is True

    def test_env_var_disables_it(self, monkeypatch):
        monkeypatch.setenv('MIGRATION_ICEBERG_INPLACE_TEXT_CTAS', 'false')
        assert m.get_config()['iceberg_inplace_text_ctas'] is False

    def test_other_falsey_spellings_disable_it(self, monkeypatch):
        monkeypatch.setenv('MIGRATION_ICEBERG_INPLACE_TEXT_CTAS', 'no')
        assert m.get_config()['iceberg_inplace_text_ctas'] is False


class TestStagingHelpers:

    def test_staging_name(self):
        assert m._ice_staging_name('logs') == 'logs__ice_staging'

    def test_recognises_staging_table(self):
        assert m._is_ice_staging_table('logs__ice_staging') is True
        assert m._is_ice_staging_table('LOGS__ICE_STAGING') is True
        assert m._is_ice_staging_table('logs') is False
        assert m._is_ice_staging_table('') is False

    def test_backup_and_staging_are_distinct(self):
        assert m._is_iceberg_backup_table('logs__ice_staging') is False
        assert m._is_ice_staging_table('logs_backup_') is False


class TestCtasTargetLocation:

    def test_s3a_gets_iceberg_sibling(self):
        assert m._ctas_target_location('s3a://bucket/db/logs') == 's3a://bucket/db/logs_iceberg'

    def test_trailing_slash_stripped(self):
        assert m._ctas_target_location('s3a://bucket/db/logs/') == 's3a://bucket/db/logs_iceberg'

    def test_s3_scheme_normalized_to_s3a(self):
        assert m._ctas_target_location('s3://bucket/db/logs') == 's3a://bucket/db/logs_iceberg'

    def test_non_s3_scheme_returns_none(self):
        # normalize_s3 would turn this into 's3a://maprfs:///data/logs'
        assert m._ctas_target_location('maprfs:///data/logs') is None

    def test_empty_returns_none(self):
        assert m._ctas_target_location('') is None
        assert m._ctas_target_location(None) is None

    def test_uppercase_scheme_does_not_double_prefix(self):
        # normalize_s3 matches schemes case-sensitively, so passing the raw value
        # through would yield 's3a://S3://bucket/db/logs'.
        assert m._ctas_target_location('S3://bucket/db/logs') == 's3a://bucket/db/logs_iceberg'
        assert m._ctas_target_location('S3A://bucket/db/logs') == 's3a://bucket/db/logs_iceberg'


class TestSchemaNormalization:

    def test_narrow_ints_promote_to_int(self):
        assert m._normalize_type_for_iceberg('tinyint') == 'int'
        assert m._normalize_type_for_iceberg('SMALLINT') == 'int'

    def test_char_and_varchar_become_string(self):
        assert m._normalize_type_for_iceberg('varchar(20)') == 'string'
        assert m._normalize_type_for_iceberg('CHAR(3)') == 'string'
        assert m._normalize_type_for_iceberg('varchar') == 'string'

    def test_nested_types_are_normalized(self):
        assert m._normalize_type_for_iceberg('array<tinyint>') == 'array<int>'
        assert m._normalize_type_for_iceberg('map<string,varchar(5)>') == 'map<string,string>'
        assert m._normalize_type_for_iceberg(
            'struct<a:varchar(10),b:tinyint>') == 'struct<a:string,b:int>'
        assert m._normalize_type_for_iceberg(
            'array<struct<a:char(2),b:smallint>>') == 'array<struct<a:string,b:int>>'

    def test_field_names_matching_type_keywords_are_left_alone(self):
        assert m._normalize_type_for_iceberg('struct<varchar:int>') == 'struct<varchar:int>'
        assert m._normalize_type_for_iceberg('struct<tinyint:tinyint>') == 'struct<tinyint:int>'
        assert m._normalize_type_for_iceberg('decimal(10,2)') == 'decimal(10,2)'

    def test_other_types_pass_through_lowercased(self):
        assert m._normalize_type_for_iceberg('  BIGINT ') == 'bigint'
        assert m._normalize_type_for_iceberg('decimal(10,2)') == 'decimal(10,2)'


class TestCompareTableSchemas:

    def _schema_router(self, src_cols, dest_cols):
        def router(sql):
            df = MagicMock()
            cols = dest_cols if '__ice_staging' in sql else src_cols
            df.collect.return_value = [
                MagicMock(col_name=n, data_type=t) for n, t in cols
            ]
            return df
        return router

    def test_varchar_source_matches_string_copy(self, mock_spark):
        """The whole point: Iceberg has no varchar, so this must not be a mismatch."""
        mock_spark.sql.side_effect = self._schema_router(
            [('id', 'bigint'), ('name', 'varchar(20)')],
            [('id', 'bigint'), ('name', 'string')],
        )
        match, diffs = m._compare_table_schemas(mock_spark, 'db.logs', 'db.logs__ice_staging')
        assert match is True
        assert diffs == []

    def test_missing_column_is_a_diff(self, mock_spark):
        mock_spark.sql.side_effect = self._schema_router(
            [('id', 'bigint'), ('name', 'string')],
            [('id', 'bigint')],
        )
        match, diffs = m._compare_table_schemas(mock_spark, 'db.logs', 'db.logs__ice_staging')
        assert match is False
        assert 'Missing column in Iceberg: name' in diffs

    def test_extra_column_is_a_diff(self, mock_spark):
        mock_spark.sql.side_effect = self._schema_router(
            [('id', 'bigint')],
            [('id', 'bigint'), ('surprise', 'string')],
        )
        match, diffs = m._compare_table_schemas(mock_spark, 'db.logs', 'db.logs__ice_staging')
        assert match is False
        assert 'Extra column in Iceberg: surprise' in diffs

    def test_real_type_mismatch_is_a_diff(self, mock_spark):
        mock_spark.sql.side_effect = self._schema_router(
            [('id', 'bigint')],
            [('id', 'string')],
        )
        match, diffs = m._compare_table_schemas(mock_spark, 'db.logs', 'db.logs__ice_staging')
        assert match is False
        assert any('Type mismatch for id' in d for d in diffs)

    def test_section_headers_are_ignored(self, mock_spark):
        mock_spark.sql.side_effect = self._schema_router(
            [('id', 'bigint'), ('# Partition Information', ''), ('dt', 'string')],
            [('id', 'bigint'), ('dt', 'string')],
        )
        match, diffs = m._compare_table_schemas(mock_spark, 'db.logs', 'db.logs__ice_staging')
        assert match is True


class TestRepairPartialTextSwap:

    def _router(self, existing, backup_location='s3a://bucket/db/logs'):
        """SHOW TABLES answers existence; DESCRIBE FORMATTED answers Location."""
        def router(sql):
            df = MagicMock()
            sl = sql.lower()
            if 'show tables' in sl:
                name = sql.split("LIKE '")[1].rstrip("'").strip()
                df.count.return_value = 1 if name in existing else 0
            elif 'describe formatted' in sl:
                df.collect.return_value = [
                    MagicMock(col_name='Location', data_type=backup_location)
                ]
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df
        return router

    def test_clean_state_is_ready(self, mock_spark):
        mock_spark.sql.side_effect = self._router({'logs'})
        assert m._repair_partial_text_swap(
            mock_spark, 'db', 'logs', 's3a://bucket/db/logs') == 'READY'

    def test_leftover_staging_is_purged(self, mock_spark):
        mock_spark.sql.side_effect = self._router({'logs', 'logs__ice_staging'})
        state = m._repair_partial_text_swap(mock_spark, 'db', 'logs', 's3a://bucket/db/logs')
        assert state == 'STAGING_CLEARED'
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'drop table if exists db.logs__ice_staging purge' in issued

    def test_live_table_with_backup_is_a_conflict(self, mock_spark):
        """A backup beside a live TEXT table is not provably ours — never drop it."""
        mock_spark.sql.side_effect = self._router({'logs', 'logs_backup_'})
        state = m._repair_partial_text_swap(mock_spark, 'db', 'logs', 's3a://bucket/db/logs')
        assert state == 'BACKUP_CONFLICT'
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'drop table' not in issued
        assert 'rename to' not in issued

    def test_staging_is_cleared_even_when_a_backup_blocks_the_swap(self, mock_spark):
        """The staging copy is ours and holds a full copy of the data. Discovery ignores
        '__ice_staging' names, so leaving it behind means it sits on S3 unnoticed."""
        mock_spark.sql.side_effect = self._router({'logs', 'logs_backup_', 'logs__ice_staging'})
        state = m._repair_partial_text_swap(mock_spark, 'db', 'logs', 's3a://bucket/db/logs')
        assert state == 'BACKUP_CONFLICT'
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'drop table if exists db.logs__ice_staging purge' in issued
        assert 'drop table if exists db.logs_backup_' not in issued
        assert 'rename to' not in issued

    def test_missing_table_with_matching_backup_rolls_back(self, mock_spark):
        mock_spark.sql.side_effect = self._router({'logs_backup_'})
        state = m._repair_partial_text_swap(mock_spark, 'db', 'logs', 's3a://bucket/db/logs')
        assert state == 'ROLLED_BACK'
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'ALTER TABLE db.logs_backup_ RENAME TO db.logs' in issued

    def test_rollback_also_purges_staging(self, mock_spark):
        mock_spark.sql.side_effect = self._router({'logs_backup_', 'logs__ice_staging'})
        state = m._repair_partial_text_swap(mock_spark, 'db', 'logs', 's3a://bucket/db/logs')
        assert state == 'ROLLED_BACK'
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'drop table if exists db.logs__ice_staging purge' in issued

    def test_backup_at_a_different_location_blocks_the_rollback(self, mock_spark):
        """Source gone and the backup unprovable is a different state from a stray backup
        beside a live table — it must not share BACKUP_CONFLICT, which is recorded SKIPPED."""
        mock_spark.sql.side_effect = self._router(
            {'logs_backup_'}, backup_location='s3a://bucket/other/thing')
        state = m._repair_partial_text_swap(mock_spark, 'db', 'logs', 's3a://bucket/db/logs')
        assert state == 'ROLLBACK_BLOCKED'
        assert 'RENAME TO' not in ' '.join(str(c) for c in mock_spark.sql.call_args_list)

    def test_unknown_expected_location_blocks_the_rollback(self, mock_spark):
        """Without a location from discovery the guard cannot be evaluated."""
        mock_spark.sql.side_effect = self._router({'logs_backup_'})
        assert m._repair_partial_text_swap(mock_spark, 'db', 'logs', None) == 'ROLLBACK_BLOCKED'

    def test_missing_table_and_no_backup_is_unrecoverable(self, mock_spark):
        mock_spark.sql.side_effect = self._router(set())
        assert m._repair_partial_text_swap(
            mock_spark, 'db', 'logs', 's3a://bucket/db/logs') == 'UNRECOVERABLE'

    def _iceberg_live_router(self, existing):
        """SHOW TABLES answers existence; DESCRIBE FORMATTED reports the live table as Iceberg —
        simulates a retry with the same discovery XCom hitting a table this run already swapped."""
        def router(sql):
            df = MagicMock()
            sl = sql.lower()
            if 'show tables' in sl:
                name = sql.split("LIKE '")[1].rstrip("'").strip()
                df.count.return_value = 1 if name in existing else 0
            elif 'describe formatted' in sl:
                df.collect.return_value = [MagicMock(col_name='Provider', data_type='iceberg')]
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df
        return router

    def test_retry_after_success_with_backup_present_is_already_migrated(self, mock_spark):
        """Attempt 2 re-processing a table attempt 1 already swapped must not downgrade it."""
        mock_spark.sql.side_effect = self._iceberg_live_router({'logs', 'logs_backup_'})
        state = m._repair_partial_text_swap(mock_spark, 'db', 'logs', 's3a://bucket/db/logs')
        assert state == 'ALREADY_MIGRATED'
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'drop table' not in issued
        assert 'rename to' not in issued

    def test_retry_after_success_with_backup_dropped_is_already_migrated(self, mock_spark):
        """Same shape under iceberg_drop_backup=true, where the backup no longer exists — the
        CTAS must not re-run over the copy that already holds the migrated data."""
        mock_spark.sql.side_effect = self._iceberg_live_router({'logs'})
        state = m._repair_partial_text_swap(mock_spark, 'db', 'logs', 's3a://bucket/db/logs')
        assert state == 'ALREADY_MIGRATED'
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'drop table' not in issued
        assert 'create table' not in issued
        assert 'rename to' not in issued


class TestMigrateTextTableInplace:

    def _router(self, *, staging_rows=10, src_cols=(('id', 'varchar(20)'),), dest_cols=None,
                fail_on=None):
        """COUNT(*) answers the row gate; DESCRIBE answers the schema gate."""
        dest_cols = (('id', 'string'),) if dest_cols is None else dest_cols
        def router(sql):
            sl = sql.lower()
            if fail_on and fail_on in sl:
                raise RuntimeError('boom')
            df = MagicMock()
            if 'count(*)' in sl:
                row = MagicMock()
                row.__getitem__ = lambda self, k: staging_rows
                df.collect.return_value = [row]
            elif sl.strip().startswith('describe '):
                cols = dest_cols if '__ice_staging' in sl else src_cols
                df.collect.return_value = [
                    MagicMock(col_name=n, data_type=t) for n, t in cols
                ]
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df
        return router

    def _call(self, spark, **kw):
        params = dict(location='s3a://bucket/db/logs', partition_columns=[],
                      hive_count=10, drop_backup=False)
        params.update(kw)
        return m._migrate_text_table_inplace(spark, 'db', 'logs', **params)

    def test_sql_order_is_copy_then_two_renames(self, mock_spark):
        mock_spark.sql.side_effect = self._router()
        self._call(mock_spark)
        issued = [str(c.args[0]) for c in mock_spark.sql.call_args_list]
        ctas = next(i for i, s in enumerate(issued) if 'CREATE TABLE' in s)
        to_backup = next(i for i, s in enumerate(issued)
                         if 'RENAME TO db.logs_backup_' in s)
        to_final = next(i for i, s in enumerate(issued)
                        if 'db.logs__ice_staging RENAME TO db.logs' in s)
        assert ctas < to_backup < to_final

    def test_ctas_carries_location_and_fanout(self, mock_spark):
        mock_spark.sql.side_effect = self._router()
        self._call(mock_spark)
        create = next(str(c.args[0]) for c in mock_spark.sql.call_args_list
                      if 'CREATE TABLE' in str(c.args[0]))
        assert "LOCATION 's3a://bucket/db/logs_iceberg'" in create
        assert "'write.spark.fanout.enabled'='true'" in create
        assert 'USING iceberg' in create
        assert 'PARTITIONED BY' not in create

    def test_partitioned_ctas_includes_partition_columns(self, mock_spark):
        mock_spark.sql.side_effect = self._router()
        self._call(mock_spark, partition_columns=['dt', 'region'])
        create = next(str(c.args[0]) for c in mock_spark.sql.call_args_list
                      if 'CREATE TABLE' in str(c.args[0]))
        assert 'PARTITIONED BY (dt, region)' in create

    def test_non_s3_location_omits_the_clause(self, mock_spark):
        mock_spark.sql.side_effect = self._router()
        self._call(mock_spark, location='maprfs:///data/logs')
        create = next(str(c.args[0]) for c in mock_spark.sql.call_args_list
                      if 'CREATE TABLE' in str(c.args[0]))
        assert 'LOCATION' not in create

    def test_row_mismatch_purges_staging_and_does_not_rename(self, mock_spark):
        mock_spark.sql.side_effect = self._router(staging_rows=9)
        with pytest.raises(m.TextCtasError, match='9 rows'):
            self._call(mock_spark, hive_count=10)
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'DROP TABLE IF EXISTS db.logs__ice_staging PURGE' in issued
        assert 'RENAME TO' not in issued

    def test_schema_mismatch_purges_staging_and_does_not_rename(self, mock_spark):
        mock_spark.sql.side_effect = self._router(dest_cols=(('id', 'bigint'),))
        with pytest.raises(m.TextCtasError, match='does not match the source schema'):
            self._call(mock_spark)
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'DROP TABLE IF EXISTS db.logs__ice_staging PURGE' in issued
        assert 'RENAME TO' not in issued

    def test_drop_backup_drops_without_purge(self, mock_spark):
        mock_spark.sql.side_effect = self._router()
        self._call(mock_spark, drop_backup=True)
        drop = next(str(c.args[0]) for c in mock_spark.sql.call_args_list
                    if 'DROP TABLE IF EXISTS db.logs_backup_' in str(c.args[0]))
        assert 'PURGE' not in drop

    def test_backup_kept_by_default(self, mock_spark):
        mock_spark.sql.side_effect = self._router()
        self._call(mock_spark)
        issued = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'DROP TABLE IF EXISTS db.logs_backup_' not in issued

    def test_failed_backup_drop_does_not_raise(self, mock_spark):
        """Step 4 is cleanup — the swap already committed."""
        mock_spark.sql.side_effect = self._router(fail_on='drop table if exists db.logs_backup_')
        result = self._call(mock_spark, drop_backup=True)
        assert result['backup_table'] == 'db.logs_backup_'
