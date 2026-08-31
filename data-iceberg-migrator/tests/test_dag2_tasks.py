"""DAG 2 Task Tests: iceberg_migration pipeline."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import migration_dag_iceberg as m
import pytest

from .helpers import make_excel_bytes, setup_spark_excel


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
                'partition_columns': ['dt'],
            }],
        }

    def _make_inplace_discovery(self, source_format='PARQUET'):
        d = self._make_partitioned_discovery(source_format)
        d['inplace_migration'] = True
        d['destination_iceberg_database'] = d['source_database']
        return d

    def _partition_router(self, *, hive_rows=5, non_empty=1, registered=2, iceberg=1, iceberg_with_data=None):
        """SQL router that distinguishes SELECT DISTINCT, .partitions, row-count, SHOW PARTITIONS."""
        if iceberg_with_data is None:
            iceberg_with_data = iceberg
        def router(sql):
            sl = sql.lower()
            df = MagicMock()
            row = MagicMock()
            if 'select distinct' in sl:
                row.__getitem__ = lambda self, k: non_empty
                df.collect.return_value = [row]
            elif '.partitions' in sl:
                val = iceberg_with_data if 'record_count' in sl else iceberg
                row.__getitem__ = lambda self, k, _v=val: _v
                df.collect.return_value = [row]
            elif 'count(*)' in sl:
                row.__getitem__ = lambda self, k: hive_rows
                df.collect.return_value = [row]
            elif 'show partitions' in sl:
                df.count.return_value = registered
                df.collect.return_value = [MagicMock() for _ in range(registered)]
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

    def test_text_format_inplace_is_skipped_not_failed(self, mock_spark):
        """TEXT+inplace is unsupported: must record SKIPPED (not FAILED) and leave _has_failures=False."""
        discovery = {
            'source_database': 'sales_data_s3',
            'destination_iceberg_database': 'sales_data_s3',
            'inplace_migration': True,
            'run_id': 'iceberg_run_20250101_120000_abcd1234',
            'discovered_tables': [{'table': 'logs', 'location': 's3a://bucket/logs',
                                    'source_format': 'TEXT', 'partition_columns': []}],
        }
        mock_spark.sql.side_effect = self._partition_router()
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        assert result['_has_failures'] is False
        assert result['results'][0]['status'] == 'SKIPPED'
        assert result['results'][0]['migration_type'] == 'INPLACE'

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


class TestSchemaNormalization:

    def test_narrow_ints_promote_to_int(self):
        assert m._normalize_type_for_iceberg('tinyint') == 'int'
        assert m._normalize_type_for_iceberg('SMALLINT') == 'int'

    def test_char_and_varchar_become_string(self):
        assert m._normalize_type_for_iceberg('varchar(20)') == 'string'
        assert m._normalize_type_for_iceberg('CHAR(3)') == 'string'
        assert m._normalize_type_for_iceberg('varchar') == 'string'

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
