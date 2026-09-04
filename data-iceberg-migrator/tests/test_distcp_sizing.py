"""Tests for DistCp auto-sizing and JVM option assembly in shared.py."""

import pytest
from migrator_utils.migrations.shared import distcp_jvm_opts, size_distcp_job

GB = 1024 ** 3
TB = 1024 ** 4
MB = 1024 ** 2


def _config(**overrides):
    base = {
        'distcp_mappers': '',
        'distcp_bandwidth': '',
        'distcp_target_bytes_per_mapper': 2 * GB,
        'distcp_min_mappers': 1,
        'distcp_max_mappers': 100,
        'distcp_target_aggregate_mbps': 2000,
        'distcp_default_mappers': 50,
        'distcp_default_bandwidth': 100,
    }
    base.update(overrides)
    return base


class TestSizeDistcpJob:

    def test_both_overrides_returned_verbatim(self):
        cfg = _config(distcp_mappers='10', distcp_bandwidth='50')
        assert size_distcp_job(40 * GB, 800, cfg) == (10, 50)

    def test_override_ignores_size_entirely(self):
        cfg = _config(distcp_mappers='7', distcp_bandwidth='3')
        assert size_distcp_job(0, 0, cfg) == (7, 3)

    def test_only_mappers_set_raises(self):
        cfg = _config(distcp_mappers='10')
        with pytest.raises(ValueError, match='distcp_bandwidth'):
            size_distcp_job(40 * GB, 800, cfg)

    def test_only_bandwidth_set_raises(self):
        cfg = _config(distcp_bandwidth='50')
        with pytest.raises(ValueError, match='distcp_mappers'):
            size_distcp_job(40 * GB, 800, cfg)

    @pytest.mark.parametrize("size_bytes,file_count,expected", [
        (500 * MB, 6, (1, 2000)),
        (40 * GB, 800, (20, 100)),
        (2 * TB, 30000, (100, 20)),
        (900 * GB, 3, (3, 666)),
        (8 * GB, 200000, (4, 500)),
    ])
    def test_auto_sizing(self, size_bytes, file_count, expected):
        assert size_distcp_job(size_bytes, file_count, _config()) == expected

    def test_zero_size_with_files_falls_back_to_defaults(self):
        cfg = _config(distcp_default_mappers=50, distcp_default_bandwidth=100)
        assert size_distcp_job(0, 12, cfg) == (50, 100)

    def test_zero_size_fallback_honours_configured_defaults(self):
        cfg = _config(distcp_default_mappers=8, distcp_default_bandwidth=250)
        assert size_distcp_job(0, 12, cfg) == (8, 250)

    def test_zero_size_and_zero_files_uses_min_mappers(self):
        assert size_distcp_job(0, 0, _config()) == (1, 2000)

    def test_ceiling_is_exact_above_float_precision(self):
        # 2**53 + 1 is the first size where float division rounds down.
        cfg = _config(distcp_max_mappers=10 ** 9, distcp_target_aggregate_mbps=10 ** 9)
        mappers, _ = size_distcp_job(2 ** 53 + 1, 10 ** 9, cfg)
        assert mappers == 4194305

    def test_exact_multiple_does_not_round_up(self):
        cfg = _config(distcp_max_mappers=10 ** 9, distcp_target_aggregate_mbps=10 ** 9)
        mappers, _ = size_distcp_job(4194304 * 2 * GB, 10 ** 9, cfg)
        assert mappers == 4194304

    def test_bandwidth_never_drops_below_one(self):
        cfg = _config(distcp_target_aggregate_mbps=10, distcp_max_mappers=100)
        assert size_distcp_job(2 * TB, 30000, cfg) == (100, 1)


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
