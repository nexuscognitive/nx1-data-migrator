"""DAG 1: destination derived from the source path (use_source_relative_path)."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import migration_dag_mapr_to_s3 as m
import migrator_utils.migrations.shared as shared

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
        result = shared.find_destination_conflicts(tables)
        assert set(result) == {('d', 'p'), ('d', 'c')}
        # Each row's message reports its own destination and names the other side.
        assert 's3a://b/x/p overlaps' in result[('d', 'p')]
        assert 'd.c (/x/p/c)' in result[('d', 'p')]
        assert 's3a://b/x/p/c overlaps' in result[('d', 'c')]
        assert 'd.p (/x/p)' in result[('d', 'c')]

    def test_three_level_nesting_lists_all_peers(self):
        tables = [_table('d', 'a', '/x', 's3a://b/x'),
                  _table('d', 'b', '/x/y', 's3a://b/x/y'),
                  _table('d', 'c', '/x/y/z', 's3a://b/x/y/z')]
        result = shared.find_destination_conflicts(tables)
        assert set(result) == {('d', 'a'), ('d', 'b'), ('d', 'c')}
        # Intermediate node must see both the parent and the deeper descendant.
        assert 'd.a (/x)' in result[('d', 'b')]
        assert 'd.c (/x/y/z)' in result[('d', 'b')]

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


def test_invalid_destination_status_is_skippable():
    assert 'DESTINATION_PATH_INVALID' in m.SKIPPABLE_DISCOVERY_ERRORS


def test_destination_overlap_is_not_blocking():
    # Conflicts are surfaced as advisories in the report, never as skippable errors.
    assert 'DESTINATION_PATH_CONFLICT' not in m.SKIPPABLE_DISCOVERY_ERRORS



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


def _report_row(db, tbl, dest, src='', status='VALIDATED'):
    """Minimal migration_table_status row for generate_html_report."""
    return SimpleNamespace(
        source_database=db, source_table=tbl,
        overall_status=status, source_location=src, dest_location=dest,
        discovery_duration_seconds=0.0, distcp_duration_seconds=0.0,
        distcp_bytes_copied=0, distcp_files_copied=0,
        distcp_is_incremental=False, table_create_duration_seconds=0.0,
        validation_duration_seconds=0.0, validation_status='COMPLETED',
        row_count_match=True, partition_count_match=True, schema_match=True,
        source_row_count=0, dest_hive_row_count=0,
        source_partition_count=0, dest_partition_count=0,
        source_total_size_bytes=0, s3_total_size_bytes_before=0,
        s3_total_size_bytes_after=0, s3_bytes_transferred=0,
        file_size_match=True, source_file_count=0,
        s3_file_count_before=0, s3_file_count_after=0,
        s3_files_transferred=0, file_count_match=True,
        distcp_status='COMPLETED', file_format='PARQUET',
        partition_filter=None, filtered_partition_count=None,
        error_message=None,
    )


def _run_report(mock_spark, sample_run_id, table_rows):
    run_row = SimpleNamespace(dag_run_id='dag_run_test')

    def sql_router(sql):
        df = MagicMock()
        sl = sql.lower()
        if 'migration_runs' in sl and 'where' in sl:
            df.collect.return_value = [run_row]
        elif 'order by' in sl:
            df.collect.return_value = table_rows
        else:
            df.collect.return_value = []
        return df

    mock_spark.sql.side_effect = sql_router
    result = m.generate_html_report.function(run_id=sample_run_id, spark=mock_spark)
    stream = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value.create.return_value
    html = stream.write.call_args[0][0].decode('utf-8')
    return result, html


class TestAdvisoriesSection:

    def test_overlap_renders_advisories_section_and_counts_in_result(
        self, mock_spark, sample_run_id
    ):
        rows = [
            _report_row('d', 'parent', 's3a://b/x', src='/x'),
            _report_row('d', 'child', 's3a://b/x/leaf', src='/x/leaf'),
        ]
        result, html = _run_report(mock_spark, sample_run_id, rows)
        assert result['destination_overlap_count'] == 2
        assert '<h2>Advisories</h2>' in html
        assert 'advisory-box' in html
        # Each row names its own destination and the peer.
        assert 'd.child (/x/leaf)' in html
        assert 'd.parent (/x)' in html

    def test_no_overlap_omits_advisories_section(self, mock_spark, sample_run_id):
        rows = [
            _report_row('d', 'a', 's3a://b/x/a', src='/x/a'),
            _report_row('d', 'b', 's3a://b/x/b', src='/x/b'),
        ]
        result, html = _run_report(mock_spark, sample_run_id, rows)
        assert result['destination_overlap_count'] == 0
        assert '<h2>Advisories</h2>' not in html

    def test_skippable_rows_do_not_participate_in_overlap_check(
        self, mock_spark, sample_run_id
    ):
        # A table that never made it past discovery (dest unused) shouldn't be
        # counted as the owner of a destination.
        rows = [
            _report_row('d', 'ghost', 's3a://b/x', src='', status='SOURCE_PATH_NOT_FOUND'),
            _report_row('d', 'real', 's3a://b/x', src='/x'),
        ]
        result, html = _run_report(mock_spark, sample_run_id, rows)
        assert result['destination_overlap_count'] == 0
        assert '<h2>Advisories</h2>' not in html
