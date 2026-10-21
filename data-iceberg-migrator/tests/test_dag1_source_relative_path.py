"""DAG 1: destination derived from the source path (use_source_relative_path)."""

from unittest.mock import MagicMock, patch

import migration_dag_mapr_to_s3 as m
import migrator_utils.migrations.shared as shared
import pytest

from . import test_dag1_tasks as dag1_tests


def _table(db, name, source_location, s3_location=None, **extra):
    return {
        'source_database': db,
        'source_table': name,
        'source_location': source_location,
        's3_location': s3_location or f's3a://b/{db}/{name}',
        **extra,
    }


class TestApplySourceRelativePaths:

    def test_rewrites_s3_location_and_strips_prefix(self):
        tables = [_table('d', 't', 'maprfs:///mapr/prod/data/t')]
        m._apply_source_relative_paths(tables, 's3a://b', ['/mapr/prod'])
        assert tables[0]['s3_location'] == 's3a://b/data/t'
        assert 'error' not in tables[0]

    def test_tables_with_errors_are_left_alone(self):
        tables = [_table('d', 't', '', error='gone', error_type='TABLE_NOT_FOUND')]
        m._apply_source_relative_paths(tables, 's3a://b', [])
        assert tables[0]['s3_location'] == 's3a://b/d/t'
        assert tables[0]['error_type'] == 'TABLE_NOT_FOUND'

    def test_unusable_source_path_is_skipped_not_raised(self):
        tables = [_table('d', 't', ''), _table('d', 'ok', '/data/ok')]
        m._apply_source_relative_paths(tables, 's3a://b', [])
        assert tables[0]['error_type'] == 'DESTINATION_PATH_INVALID'
        assert 'Cannot derive destination' in tables[0]['error']
        assert tables[1]['s3_location'] == 's3a://b/data/ok'


class TestFindDestinationConflicts:

    def test_distinct_destinations_do_not_conflict(self):
        tables = [_table('d', 'a', '/x/a', 's3a://b/x/a'),
                  _table('d', 'b', '/x/b', 's3a://b/x/b')]
        assert shared.find_destination_conflicts(tables) == {}

    def test_exact_clash_names_both_tables_and_source_paths(self):
        tables = [_table('d1', 'a', 'hdfs://n/x/t', 's3a://b/x/t'),
                  _table('d2', 'b', 'hdfs://n/y/t', 's3a://b/x/t')]
        result = shared.find_destination_conflicts(tables)
        assert set(result) == {('d1', 'a'), ('d2', 'b')}
        assert 'd2.b (hdfs://n/y/t)' in result[('d1', 'a')]
        assert 'd1.a (hdfs://n/x/t)' in result[('d2', 'b')]

    def test_nested_destination_conflicts_even_with_sibling_between(self):
        tables = [_table('d', 'p', '/x/p', 's3a://b/x/p'),
                  _table('d', 'sib', '/x/p-sib', 's3a://b/x/p-sib'),
                  _table('d', 'c', '/x/p/c', 's3a://b/x/p/c')]
        assert set(shared.find_destination_conflicts(tables)) == {('d', 'p'), ('d', 'c')}

    def test_prefix_sibling_is_not_nested(self):
        tables = [_table('d', 'a', '/x/a', 's3a://b/x/a'),
                  _table('d', 'ab', '/x/ab', 's3a://b/x/ab')]
        assert shared.find_destination_conflicts(tables) == {}

    def test_trailing_slash_is_ignored(self):
        tables = [_table('d', 'a', '/x', 's3a://b/x/'), _table('d', 'b', '/y', 's3a://b/x')]
        assert len(shared.find_destination_conflicts(tables)) == 2

    def test_same_table_repeated_for_partition_filters_is_not_a_conflict(self):
        tables = [_table('d', 'a', '/x/a', 's3a://b/x/a', partition_filter='dt=1'),
                  _table('d', 'a', '/x/a', 's3a://b/x/a', partition_filter='dt=2')]
        assert shared.find_destination_conflicts(tables) == {}

    def test_comparison_is_case_sensitive(self):
        tables = [_table('d', 'a', '/x/A', 's3a://b/x/A'),
                  _table('d', 'b', '/x/a', 's3a://b/x/a')]
        assert shared.find_destination_conflicts(tables) == {}

    def test_errored_tables_are_ignored(self):
        tables = [_table('d', 'a', '/x', 's3a://b/x', error='boom'),
                  _table('d', 'b', '/x', 's3a://b/x')]
        assert shared.find_destination_conflicts(tables) == {}


class TestResolveDestinationConflicts:

    def _run(self, enabled, discoveries):
        cfg = {'use_source_relative_path': enabled}
        with patch.object(m, 'get_config', return_value=cfg):
            return m.resolve_destination_conflicts.function(discoveries=discoveries)

    def test_disabled_returns_nothing(self):
        d = {'tables': [_table('d', 'a', '/x', 's3a://b/x'),
                        _table('e', 'b', '/y', 's3a://b/x')]}
        assert self._run(False, [d]) == {}

    def test_finds_conflicts_across_databases(self):
        d1 = {'tables': [_table('d', 'a', '/x', 's3a://b/x')]}
        d2 = {'tables': [_table('e', 'b', '/y', 's3a://b/x')]}
        result = self._run(True, [d1, None, d2])
        assert set(result) == {'d\ta', 'e\tb'}


class TestRecordAppliesConflicts:

    def _record(self, discovery, conflicts, mock_spark):
        df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: 0
        df.collect.return_value = [row]
        mock_spark.sql.return_value = df
        return m.record_discovered_tables.function(
            discovery=discovery, spark=mock_spark, conflicts=conflicts,
            ti=MagicMock(map_index=0),
        )

    def test_conflicting_table_is_marked_and_tracked(
        self, mock_spark, sample_discovery, mock_iceberg_retry
    ):
        tbl = sample_discovery['tables'][0]
        key = f"{tbl['source_database']}\t{tbl['source_table']}"
        result = self._record(sample_discovery, {key: 'clashes with e.b'}, mock_spark)
        marked = result['tables'][0]
        assert marked['error_type'] == 'DESTINATION_PATH_CONFLICT'
        assert marked['error'] == 'clashes with e.b'
        sql = ' '.join(c.args[1] for c in mock_iceberg_retry.call_args_list)
        assert "'DESTINATION_PATH_CONFLICT'" in sql
        assert 'clashes with e.b' in sql

    def test_no_conflicts_leaves_tables_untouched(
        self, mock_spark, sample_discovery, mock_iceberg_retry
    ):
        result = self._record(sample_discovery, {}, mock_spark)
        assert all(t.get('error_type') != 'DESTINATION_PATH_CONFLICT' for t in result['tables'])


@pytest.mark.parametrize('status', ['DESTINATION_PATH_CONFLICT', 'DESTINATION_PATH_INVALID'])
def test_destination_statuses_are_skippable(status):
    assert status in m.SKIPPABLE_DISCOVERY_ERRORS



class TestDiscoveryUsesSourcePath:

    def _discover(self, mock_ssh_hook, sample_run_id, config):
        _, client, _, _ = mock_ssh_hook
        harness = dag1_tests.TestDiscoverTablesViaSshSpark()
        metadata = [
            harness._make_metadata_record('orders'),
            harness._make_metadata_record(
                'events', source_location='hdfs://nn:8020/mapr/prod/data/events'
            ),
        ]
        with patch.object(m, 'get_config', return_value={**m.get_config(), **config}):
            return harness._run_discovery(
                client, metadata, sample_run_id, ['orders', 'events']
            )

    def test_default_mode_keeps_database_and_table_layout(self, mock_ssh_hook, sample_run_id):
        result = self._discover(mock_ssh_hook, sample_run_id, {})
        assert [t['s3_location'] for t in result['tables']] == [
            's3a://bucket/sales_s3/orders', 's3a://bucket/sales_s3/events',
        ]

    def test_enabled_mode_uses_stripped_source_path(self, mock_ssh_hook, sample_run_id):
        result = self._discover(mock_ssh_hook, sample_run_id, {
            'use_source_relative_path': True,
            'source_path_strip_prefixes': ['/mapr/prod'],
        })
        assert [t['s3_location'] for t in result['tables']] == [
            's3a://bucket/datalake/sales/orders', 's3a://bucket/data/events',
        ]
