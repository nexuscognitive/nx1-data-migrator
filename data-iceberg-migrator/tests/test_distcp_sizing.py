"""Tests for DistCp auto-sizing and JVM option assembly in shared.py."""

import pytest
from migrator_utils.migrations.shared import (
    distcp_jvm_opts,
    distcp_sizing_mode,
    size_distcp_job,
)

GB = 1024 ** 3
TB = 1024 ** 4
MB = 1024 ** 2


def _config(**overrides):
    base = {
        # True here so the existing tests below exercise the forced/auto
        # branches they're named for. The master-switch-off behavior has its
        # own test class using the real default (False).
        'distcp_enable_auto_sizing': True,
        'distcp_mappers': '',
        'distcp_bandwidth': '',
        'distcp_target_bytes_per_mapper': 2 * GB,
        'distcp_min_mappers': 1,
        'distcp_max_mappers': 100,
        'distcp_target_aggregate_mbps': 2000,
        'distcp_max_mapper_bandwidth': 500,
        'distcp_default_mappers': 50,
        'distcp_default_bandwidth': 100,
    }
    base.update(overrides)
    return base


class TestAutoSizingMasterSwitch:
    """distcp_enable_auto_sizing defaults to False, so an upgrade with no new
    Variable set reproduces the pre-auto-sizing DAG exactly."""

    def test_disabled_ignores_size_and_uses_fixed_defaults(self):
        cfg = _config(distcp_enable_auto_sizing=False)
        assert size_distcp_job(900 * GB, 3, cfg) == (50, 100)
        assert size_distcp_job(500 * MB, 6, cfg) == (50, 100)
        assert size_distcp_job(0, 0, cfg) == (50, 100)

    def test_disabled_honours_configured_fixed_defaults(self):
        cfg = _config(
            distcp_enable_auto_sizing=False,
            distcp_default_mappers=8, distcp_default_bandwidth=250,
        )
        assert size_distcp_job(2 * TB, 30000, cfg) == (8, 250)

    def test_disabled_ignores_forced_override_pair_too(self):
        # The master switch gates the whole feature, including the
        # forced-pair sub-mode — disabled means only the fixed defaults apply.
        cfg = _config(
            distcp_enable_auto_sizing=False,
            distcp_mappers='10', distcp_bandwidth='50',
        )
        assert size_distcp_job(40 * GB, 800, cfg) == (50, 100)

    def test_missing_key_defaults_to_disabled(self):
        # get_config() always supplies this key, but size_distcp_job should
        # fail safe (old behavior) rather than KeyError if it's ever absent.
        cfg = _config()
        del cfg['distcp_enable_auto_sizing']
        assert size_distcp_job(900 * GB, 3, cfg) == (50, 100)

    def test_sizing_mode_reports_disabled(self):
        mode = distcp_sizing_mode(_config(distcp_enable_auto_sizing=False))
        assert mode.startswith("DISABLED")
        assert "-m 50 -bandwidth 100" in mode

    def test_sizing_mode_disabled_reflects_configured_defaults(self):
        cfg = _config(
            distcp_enable_auto_sizing=False,
            distcp_default_mappers=8, distcp_default_bandwidth=250,
        )
        assert "-m 8 -bandwidth 250" in distcp_sizing_mode(cfg)


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
        # 500 MB/6 files and 900 GB/3 files would be (1, 2000) and (3, 666)
        # without the default 500 MB/s per-mapper cap — both land on 1-3
        # mappers, so the cap is what keeps either from being told to sustain
        # more than 500 MB/s on a single stream.
        (500 * MB, 6, (1, 500)),
        (40 * GB, 800, (20, 100)),
        (2 * TB, 30000, (100, 20)),
        (900 * GB, 3, (3, 500)),
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
        # 1 mapper would get the full 2000 MB/s aggregate; the default 500
        # MB/s per-mapper cap brings it down.
        assert size_distcp_job(0, 0, _config()) == (1, 500)

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

    def test_max_mapper_bandwidth_caps_low_file_count_table(self):
        # 900 GB / 3 files would otherwise be (3, 2166) at a 6500 aggregate —
        # the default cap of 500 already covers this; raise it to isolate the
        # clamp at a value the default wouldn't also trigger.
        cfg = _config(distcp_target_aggregate_mbps=6500, distcp_max_mapper_bandwidth=1000)
        assert size_distcp_job(900 * GB, 3, cfg) == (3, 1000)

    def test_max_mapper_bandwidth_noop_when_already_below_cap(self):
        cfg = _config(distcp_target_aggregate_mbps=2000, distcp_max_mapper_bandwidth=1000)
        assert size_distcp_job(40 * GB, 800, cfg) == (20, 100)

    def test_max_mapper_bandwidth_does_not_apply_to_forced_values(self):
        # Forced mode is explicit user intent; the cap is an auto-sizing guard.
        cfg = _config(
            distcp_mappers='1', distcp_bandwidth='9000',
            distcp_max_mapper_bandwidth=500,
        )
        assert size_distcp_job(900 * GB, 3, cfg) == (1, 9000)


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


class TestDistcpSizingMode:
    """The forced branch of size_distcp_job returns silently, so a pinned -m 1
    is indistinguishable in the log from auto-sizing that chose 1. This is what
    makes the two tellable apart."""

    def test_auto_when_neither_half_is_set(self):
        mode = distcp_sizing_mode(_config())
        assert mode.startswith("AUTO")
        # The knobs in play belong in the line, so the numbers below it make sense.
        assert str(2 * GB) in mode and "2000 MB/s aggregate" in mode
        assert "500 MB/s per-mapper cap" in mode

    def test_forced_when_both_halves_are_set(self):
        mode = distcp_sizing_mode(_config(distcp_mappers='1', distcp_bandwidth='100'))
        assert mode.startswith("FORCED")
        assert "-m 1 -bandwidth 100" in mode

    def test_forced_names_the_env_file_trap(self):
        # The deployed env.shared counts as "set", which is how this feature
        # ships inert on an upgrade.
        mode = distcp_sizing_mode(_config(distcp_mappers='50', distcp_bandwidth='100'))
        assert "env.shared" in mode

    @pytest.mark.parametrize('half', [
        {'distcp_mappers': '1'},
        {'distcp_bandwidth': '100'},
    ])
    def test_half_set_still_reports_auto(self, half):
        # size_distcp_job raises on a half-set pair; the mode line must not claim
        # the values are pinned before that happens.
        assert distcp_sizing_mode(_config(**half)).startswith("AUTO")

    @pytest.mark.parametrize('blank', ['', '   '])
    def test_whitespace_only_is_not_forced(self, blank):
        mode = distcp_sizing_mode(_config(distcp_mappers=blank, distcp_bandwidth=blank))
        assert mode.startswith("AUTO")

    def test_auto_mode_notes_configured_cap(self):
        mode = distcp_sizing_mode(_config(distcp_max_mapper_bandwidth=750))
        assert "750 MB/s per-mapper cap" in mode
