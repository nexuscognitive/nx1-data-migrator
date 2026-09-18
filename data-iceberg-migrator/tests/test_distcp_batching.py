"""Tests for DistCp batch cost estimation and packing in shared.py."""

import random

import pytest
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

    def test_retired_weight_keys_in_config_are_ignored(self):
        """The three weights are constants now. A client who still has them set
        must get the same estimate as one who does not."""
        table = _table(source_total_size_bytes=10 * GB, source_file_count=100)
        baseline = estimate_distcp_cost(table, {'distcp_preserve_delete': True})
        with_retired = estimate_distcp_cost(table, _config(
            distcp_cost_mbps=1,
            distcp_cost_seconds_per_job=9999,
            distcp_cost_seconds_per_path_scan=9999,
        ))
        assert with_retired == baseline

    def test_estimate_needs_only_preserve_delete_from_config(self):
        """Guards against a weight sneaking back into config: a config holding
        nothing but the one flag the function legitimately reads must work."""
        table = _table(source_total_size_bytes=10 * GB, source_file_count=100)
        # 30 base + 10240 MiB / 150 + 45 job + 24 scan + 100 * 0.002 = 167.47s
        assert estimate_distcp_cost(
            table, {'distcp_preserve_delete': True}
        ) == pytest.approx(167.47, abs=0.01)


class TestResolveBatchCap:

    BACKSTOP = 8 * 3600.0
    # 0.9 * 28800 / 1.25
    CEILING = 20736.0

    def test_backstop_ceiling_binds_for_a_large_run(self):
        """1000h of work must not produce one 1000h batch: the cap is the
        largest batch whose budget can still fire before the SIGKILL."""
        assert resolve_batch_cap(1000 * 3600.0, 3, self.BACKSTOP) == self.CEILING

    def test_lane_floor_binds_for_a_small_run(self):
        """A 600s run over 3 lanes must not collapse into one batch."""
        assert resolve_batch_cap(600.0, 3, self.BACKSTOP) == 100.0

    def test_cap_is_never_above_the_ceiling(self):
        for total in (0.0, 1.0, 1e3, 1e6, 1e9):
            assert resolve_batch_cap(total, 3, self.BACKSTOP) <= self.CEILING

    def test_a_budget_built_on_the_cap_always_fires_before_the_backstop(self):
        """The property the ceiling exists for."""
        cap = resolve_batch_cap(1e9, 3, self.BACKSTOP)
        assert cap * 1.25 <= 0.9 * self.BACKSTOP

    def test_more_lanes_lower_the_cap(self):
        small = 600.0
        assert (resolve_batch_cap(small, 6, self.BACKSTOP)
                < resolve_batch_cap(small, 3, self.BACKSTOP))

    def test_zero_cost_run_gives_a_zero_cap(self):
        """Every table errored. Documented and desired: the packer then fills
        bins to max_tables_per_batch, because 0 + 0 <= 0."""
        assert resolve_batch_cap(0.0, 3, self.BACKSTOP) == 0.0

    def test_nonpositive_lanes_fall_back_to_the_ceiling(self):
        assert resolve_batch_cap(1e9, 0, self.BACKSTOP) == self.CEILING


class TestPackTablesIntoBatches:

    def test_a_zero_cap_fills_bins_to_the_table_limit(self):
        """Every table errored, so total cost is 0 and the cap is 0. A
        zero-cost table satisfies bin_cost + 0 <= 0, so bins fill to
        max_tables_per_batch rather than one table per bin."""
        cfg = _config()
        tables = [_table(f't{i}', error='boom') for i in range(60)]
        bins = pack_tables_into_batches(tables, 0.0, cfg)
        assert [len(ts) for _, ts in bins] == [25, 25, 10]
        packed = [t['source_table'] for _, ts in bins for t in ts]
        assert len(packed) == len(set(packed)) == 60

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

    def test_two_tables_that_together_exceed_the_cap_are_split(self):
        """The cap's actual job: each fits alone, the pair does not."""
        cfg = _config()
        tables = [_table(f't{i}', source_total_size_bytes=100 * GB,
                         source_file_count=100) for i in range(2)]
        # Derived from the estimate so the test does not re-implement the model.
        each = estimate_distcp_cost(tables[0], cfg)
        cap = each * 1.5
        assert each <= cap < each * 2
        bins = pack_tables_into_batches(tables, cap, cfg)
        assert len(bins) == 2
        assert all(len(b[1]) == 1 for b in bins)

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

    BACKSTOP = 8 * 3600.0
    CEILING = 20736.0

    def test_banner_names_the_cap_and_the_tables_cap(self):
        banner = distcp_batching_mode(
            self.CEILING, 3, self.BACKSTOP, _config())
        assert '20736' in banner
        assert '25' in banner

    def test_banner_says_when_the_backstop_bound_the_cap(self):
        banner = distcp_batching_mode(
            self.CEILING, 3, self.BACKSTOP, _config())
        assert 'backstop' in banner

    def test_banner_says_when_the_lane_floor_bound_the_cap(self):
        """Without this a plan of one giant batch reads identically whether the
        backstop bound or the lane floor did."""
        banner = distcp_batching_mode(100.0, 3, self.BACKSTOP, _config())
        assert 'lane' in banner
        assert 'backstop' not in banner

    def test_banner_reports_the_fixed_weights_as_fixed(self):
        banner = distcp_batching_mode(
            self.CEILING, 3, self.BACKSTOP, _config())
        for fragment in ('150', '45', '12'):
            assert fragment in banner
        assert 'not configurable' in banner


class TestRetiredKeys:

    RETIRED = (
        'migration_distcp_cost_mbps',
        'migration_distcp_cost_seconds_per_job',
        'migration_distcp_cost_seconds_per_path_scan',
        'migration_distcp_batch_target_cost_seconds',
        'migration_distcp_max_batches',
    )

    def test_get_config_no_longer_emits_them(self):
        from migrator_utils.migrations.shared import get_config
        cfg = get_config()
        for base in self.RETIRED:
            assert base.removeprefix('migration_') not in cfg

    def test_the_two_survivors_are_still_emitted(self):
        from migrator_utils.migrations.shared import get_config
        cfg = get_config()
        assert cfg['distcp_max_tables_per_batch'] == 25
        assert cfg['distcp_call_timeout_max_seconds'] == 21600

    def test_a_set_retired_env_var_warns_by_name(self, monkeypatch, caplog):
        """A client who deliberately set one must be told it is ignored, not
        silently overridden."""
        import logging

        from migrator_utils.migrations.shared import get_config
        monkeypatch.setenv('MIGRATION_DISTCP_COST_MBPS', '600')
        with caplog.at_level(logging.WARNING):
            get_config()
        assert 'migration_distcp_cost_mbps' in caplog.text
        assert 'no longer used' in caplog.text

    def test_an_unset_retired_key_is_silent(self, monkeypatch, caplog):
        import logging

        from migrator_utils.migrations.shared import get_config
        for base in self.RETIRED:
            monkeypatch.delenv(base.upper(), raising=False)
        with caplog.at_level(logging.WARNING):
            get_config()
        assert 'no longer used' not in caplog.text
