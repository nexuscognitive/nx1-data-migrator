"""DistCp job sizing (the queue budget and the legacy fixed values) and JVM options."""

import pytest
from migrator_utils.migrations.shared import (
    distcp_budget,
    distcp_job_size,
    distcp_jvm_opts,
    distcp_sizing_mode,
)


def _config(**overrides):
    base = {
        'distcp_total_mappers': None,
        'distcp_total_bandwidth_mbps': None,
        'distcp_concurrent_jobs': None,
        'distcp_mappers': '',
        'distcp_bandwidth': '',
        'distcp_default_mappers': 50,
        'distcp_default_bandwidth': 100,
    }
    base.update(overrides)
    return base


def _budget(total_mappers, total_bandwidth_mbps, concurrent_jobs, **overrides):
    return _config(
        distcp_total_mappers=total_mappers,
        distcp_total_bandwidth_mbps=total_bandwidth_mbps,
        distcp_concurrent_jobs=concurrent_jobs,
        **overrides,
    )


class TestDistcpBudget:

    def test_unset_budget_is_none(self):
        assert distcp_budget(_config()) is None

    def test_full_budget_is_returned(self):
        assert distcp_budget(_budget(8, 80, 2)) == (8, 80, 2)


class TestDistcpJobSize:

    def test_budget_divides_totals_by_concurrency(self):
        assert distcp_job_size(_budget(8, 80, 2)) == (4, 10)

    def test_budget_floors_both_divisions(self):
        assert distcp_job_size(_budget(10, 100, 3)) == (3, 11)

    def test_budget_wins_over_forced_pair_and_defaults(self):
        cfg = _budget(8, 80, 2, distcp_mappers='7', distcp_bandwidth='30')
        assert distcp_job_size(cfg) == (4, 10)

    def test_no_budget_uses_forced_pair_when_both_set(self):
        assert distcp_job_size(_config(distcp_mappers='7', distcp_bandwidth='30')) == (7, 30)

    def test_no_budget_half_forced_pair_raises(self):
        with pytest.raises(ValueError, match='migration_distcp_bandwidth'):
            distcp_job_size(_config(distcp_mappers='7'))

    def test_no_budget_no_pair_uses_defaults(self):
        assert distcp_job_size(_config()) == (50, 100)

    def test_no_budget_honours_configured_defaults(self):
        assert distcp_job_size(_config(distcp_default_mappers=8, distcp_default_bandwidth=250)) == (8, 250)


class TestDistcpSizingMode:

    def test_sizing_mode_reports_budget(self):
        mode = distcp_sizing_mode(_budget(8, 80, 2))
        assert mode.startswith('BUDGET —')
        assert '-m 4' in mode and '-bandwidth 10' in mode

    def test_sizing_mode_reports_forced_pair(self):
        mode = distcp_sizing_mode(_config(distcp_mappers='7', distcp_bandwidth='30'))
        assert mode.startswith('FIXED — forced')
        assert '-m 7 -bandwidth 30' in mode

    def test_sizing_mode_reports_defaults(self):
        mode = distcp_sizing_mode(_config())
        assert mode.startswith('FIXED — defaults')
        assert '-m 50 -bandwidth 100' in mode


class TestDistcpJvmOpts:

    def test_empty_config_emits_nothing(self):
        assert distcp_jvm_opts(_config()) == ''

    def test_missing_keys_emit_nothing(self):
        assert distcp_jvm_opts({}) == ''

    def test_whitespace_only_values_are_skipped(self):
        cfg = _config(
            distcp_map_memory_mb='  ',
            distcp_map_java_opts='\t',
            distcp_extra_hadoop_opts=' ',
        )
        assert distcp_jvm_opts(cfg) == ''

    def test_all_knobs_populated(self):
        cfg = _config(
            distcp_map_memory_mb='4096',
            distcp_map_java_opts='-Xmx3276m -XX:+UseG1GC',
            distcp_extra_hadoop_opts='-Dmapreduce.task.timeout=1200000',
        )
        assert distcp_jvm_opts(cfg) == (
            " -Dmapreduce.map.memory.mb=4096"
            " -Dmapreduce.map.java.opts='-Xmx3276m -XX:+UseG1GC'"
            " -Dmapreduce.task.timeout=1200000"
        )

    def test_map_memory_only(self):
        cfg = _config(distcp_map_memory_mb='2048')
        assert distcp_jvm_opts(cfg) == " -Dmapreduce.map.memory.mb=2048"

    def test_client_java_opts_not_emitted_as_minus_d(self):
        cfg = _config(distcp_client_java_opts='-Xmx2g')
        assert distcp_jvm_opts(cfg) == ''


class TestDistcpQueueOpts:
    def test_queue_emits_queuename_flag(self):
        cfg = _config(distcp_queue_name='etl_low')
        assert distcp_jvm_opts(cfg) == " -Dmapreduce.job.queuename=etl_low"

    @pytest.mark.parametrize('blank', ['', '   ', None])
    def test_blank_queue_emits_nothing(self, blank):
        cfg = _config(distcp_queue_name=blank)
        assert 'queuename' not in distcp_jvm_opts(cfg)
        assert distcp_jvm_opts(cfg) == ''

    def test_queue_precedes_extra_opts(self):
        cfg = _config(distcp_queue_name='q1', distcp_extra_hadoop_opts='-Dfoo=bar')
        assert distcp_jvm_opts(cfg) == " -Dmapreduce.job.queuename=q1 -Dfoo=bar"
