"""Tests for DistCp batch cost estimation and packing in shared.py."""

import random

from migrator_utils.migrations.shared import (
    distcp_batching_mode,
    effective_source_metrics,
    estimate_distcp_cost,
    pack_tables_into_batches,
    resolve_batch_cap,
)

GB = 1024 ** 3
MB = 1024 ** 2


def _config(**overrides):
    base = {
        'distcp_preserve_delete': True,
        'distcp_cost_mbps': 150,
        'distcp_cost_seconds_per_job': 45,
        'distcp_cost_seconds_per_path_scan': 12,
        'distcp_batch_target_cost_seconds': 1800,
        'distcp_max_batches': 60,
        'distcp_max_tables_per_batch': 25,
    }
    base.update(overrides)
    return base


def _table(name='t', **overrides):
    base = {
        'source_database': 'db',
        'source_table': name,
        'dest_database': 'db_s3',
        'source_total_size_bytes': 10 * GB,
        'source_file_count': 100,
        'partition_filter_active': False,
    }
    base.update(overrides)
    return base


class TestEffectiveSourceMetrics:

    def test_unfiltered_uses_source_totals(self):
        assert effective_source_metrics(_table()) == (10 * GB, 100)

    def test_filtered_uses_filtered_totals(self):
        t = _table(
            partition_filter_active=True,
            filtered_source_size_bytes=2 * GB,
            filtered_file_count=20,
        )
        assert effective_source_metrics(t) == (2 * GB, 20)

    def test_filtered_falls_back_to_source_totals_when_absent(self):
        """An older discovery payload has no filtered_* keys."""
        t = _table(partition_filter_active=True)
        assert effective_source_metrics(t) == (10 * GB, 100)

    def test_none_values_coerce_to_zero(self):
        t = _table(source_total_size_bytes=None, source_file_count=None)
        assert effective_source_metrics(t) == (0, 0)


class TestEstimateDistcpCost:

    def test_plain_table_charges_one_job_and_one_scan_pair(self):
        cfg = _config()
        # 30 base + 10GiB/150MB/s + 1*45 + 1*12*2 + 100*0.002
        expected = 30 + (10 * GB) / (150 * MB) + 45 + 24 + 0.2
        assert estimate_distcp_cost(_table(), cfg) == expected

    def test_partition_filter_with_preserve_delete_charges_per_partition(self):
        cfg = _config()
        t = _table(
            partition_filter_active=True,
            filtered_source_size_bytes=1 * GB,
            filtered_file_count=30,
            filtered_partitions=['d=1', 'd=2', 'd=3'],
            partition_file_counts={'d=1': 10, 'd=2': 10, 'd=3': 10},
        )
        expected = 30 + (1 * GB) / (150 * MB) + 3 * 45 + 3 * 12 * 2 + 30 * 0.002
        assert estimate_distcp_cost(t, cfg) == expected

    def test_empty_partitions_are_not_charged_a_job(self):
        """Empty partitions take the mkdir path, not a DistCp job (:1834-1845)."""
        cfg = _config()
        t = _table(
            partition_filter_active=True,
            filtered_source_size_bytes=1 * GB,
            filtered_file_count=20,
            filtered_partitions=['d=1', 'd=2'],
            partition_file_counts={'d=1': 20, 'd=2': 0},
        )
        expected = 30 + (1 * GB) / (150 * MB) + 1 * 45 + 2 * 12 * 2 + 20 * 0.002
        assert estimate_distcp_cost(t, cfg) == expected

    def test_empty_partition_file_counts_treats_all_as_non_empty(self):
        """Parity with the shell's .get(p, 1) default at :1834."""
        cfg = _config()
        t = _table(
            partition_filter_active=True,
            filtered_source_size_bytes=1 * GB,
            filtered_file_count=20,
            filtered_partitions=['d=1', 'd=2'],
            partition_file_counts={},
        )
        expected = 30 + (1 * GB) / (150 * MB) + 2 * 45 + 2 * 12 * 2 + 20 * 0.002
        assert estimate_distcp_cost(t, cfg) == expected

    def test_path_list_mode_charges_one_job_but_all_scans(self):
        cfg = _config(distcp_preserve_delete=False)
        t = _table(
            partition_filter_active=True,
            filtered_source_size_bytes=1 * GB,
            filtered_file_count=20,
            filtered_partitions=['d=1', 'd=2', 'd=3'],
            partition_file_counts={'d=1': 5, 'd=2': 5, 'd=3': 10},
        )
        expected = 30 + (1 * GB) / (150 * MB) + 45 + 3 * 12 * 2 + 20 * 0.002
        assert estimate_distcp_cost(t, cfg) == expected

    def test_empty_source_is_mkdir_only(self):
        """:1686-1750 takes an mkdir branch — no job, no metrics scans."""
        cfg = _config()
        t = _table(
            source_file_count=0,
            source_total_size_bytes=0,
            partition_filter_active=True,
            filtered_file_count=0,
            filtered_source_size_bytes=0,
            filtered_partitions=['d=1', 'd=2'],
        )
        assert estimate_distcp_cost(t, cfg) == 30 + 2 * 2

    def test_filter_matching_no_partitions_is_base_only(self):
        """The loop SKIPs these before any copy work (:1646-1663)."""
        cfg = _config()
        t = _table(
            partition_filter_active=True,
            filtered_partitions=[],
            filtered_file_count=5,
            filtered_source_size_bytes=1 * GB,
        )
        assert estimate_distcp_cost(t, cfg) == 30

    def test_skippable_table_costs_nothing(self):
        cfg = _config()
        assert estimate_distcp_cost(_table(error_type='TABLE_NOT_FOUND'), cfg) == 0.0

    def test_errored_table_costs_nothing(self):
        cfg = _config()
        assert estimate_distcp_cost(_table(error='boom'), cfg) == 0.0


class TestResolveBatchCap:

    def test_target_wins_when_total_is_small(self):
        assert resolve_batch_cap(9000.0, _config()) == 1800.0

    def test_max_batches_raises_the_cap_when_it_binds(self):
        # 300000 / 60 = 5000 > 1800
        assert resolve_batch_cap(300000.0, _config()) == 5000.0

    def test_zero_cost_run_still_returns_the_target(self):
        assert resolve_batch_cap(0.0, _config()) == 1800.0


class TestPackTablesIntoBatches:

    def test_small_tables_share_one_bin(self):
        cfg = _config()
        tables = [_table(f't{i}', source_total_size_bytes=MB, source_file_count=1)
                  for i in range(5)]
        bins = pack_tables_into_batches(tables, 1800.0, cfg)
        assert len(bins) == 1
        assert len(bins[0][1]) == 5

    def test_table_over_cap_gets_its_own_bin(self):
        cfg = _config()
        monster = _table('big', source_total_size_bytes=2000 * GB, source_file_count=50000)
        small = _table('small', source_total_size_bytes=MB, source_file_count=1)
        bins = pack_tables_into_batches([small, monster], 1800.0, cfg)
        assert len(bins) == 2
        by_name = {b[1][0]['source_table']: b for b in bins if len(b[1]) == 1}
        assert by_name['big'][0] > 1800.0
        assert len(by_name['big'][1]) == 1

    def test_max_tables_per_batch_is_a_hard_cap(self):
        cfg = _config(distcp_max_tables_per_batch=3)
        tables = [_table(f't{i}', source_total_size_bytes=MB, source_file_count=1)
                  for i in range(7)]
        bins = pack_tables_into_batches(tables, 1800.0, cfg)
        assert all(len(b[1]) <= 3 for b in bins)
        assert sum(len(b[1]) for b in bins) == 7

    def test_bins_are_sorted_by_cost_descending(self):
        cfg = _config(distcp_max_tables_per_batch=1)
        tables = [
            _table('small', source_total_size_bytes=MB, source_file_count=1),
            _table('large', source_total_size_bytes=500 * GB, source_file_count=1000),
            _table('medium', source_total_size_bytes=50 * GB, source_file_count=100),
        ]
        bins = pack_tables_into_batches(tables, 1800.0, cfg)
        costs = [b[0] for b in bins]
        assert costs == sorted(costs, reverse=True)

    def test_zero_cost_tables_are_still_packed(self):
        """Their SKIPPED status is written from the element they belong to."""
        cfg = _config()
        tables = [_table('gone', error_type='TABLE_NOT_FOUND'), _table('real')]
        bins = pack_tables_into_batches(tables, 1800.0, cfg)
        packed = {t['source_table'] for _, ts in bins for t in ts}
        assert packed == {'gone', 'real'}

    def test_no_table_is_ever_dropped_or_duplicated(self):
        rng = random.Random(1234)
        cfg = _config(distcp_max_tables_per_batch=4)
        tables = [
            _table(
                f't{i}',
                source_total_size_bytes=rng.choice([0, MB, GB, 100 * GB, 3000 * GB]),
                source_file_count=rng.choice([0, 1, 100, 100000]),
            )
            for i in range(60)
        ]
        bins = pack_tables_into_batches(tables, 1800.0, cfg)
        packed = [t['source_table'] for _, ts in bins for t in ts]
        assert sorted(packed) == sorted(t['source_table'] for t in tables)
        assert len(packed) == len(set(packed))


class TestDistcpBatchingMode:

    def test_banner_names_every_weight_and_knob(self):
        banner = distcp_batching_mode(_config())
        for fragment in ('1800', '60', '25', '150', '45', '12'):
            assert fragment in banner
