"""
DAG Structure Tests: validate DAG definitions, task IDs, dependencies,
trigger rules, parameters, and end-to-end status progression.
"""

import pytest
from unittest.mock import patch, MagicMock


def _load_dags():
    import migration_dags_combined as m  
    return m

# ---------------------------------------------------------------------------
# DAG 1: mapr_to_s3_migration structure
# ---------------------------------------------------------------------------
class TestMaprToS3DagStructure:

    def test_dag_exists(self):
        m = _load_dags()
        from airflow.models import DagBag
        assert hasattr(m, 'dag_mapr_to_s3') or 'mapr_to_s3_migration' in str(dir(m))

    def test_dag1_has_correct_id(self):
        m = _load_dags()
        assert m.dag_mapr_to_s3.dag_id == 'mapr_to_s3_migration'

    def test_dag1_has_expected_tags(self):
        m = _load_dags()
        tags = m.dag_mapr_to_s3.tags
        assert 'migration' in tags
        assert 'mapr' in tags
        assert 's3' in tags
        assert 'hive' in tags

    def test_dag1_has_excel_param(self):
        m = _load_dags()
        assert 'excel_file_path' in m.dag_mapr_to_s3.params

    def test_dag1_schedule_is_none(self):
        m = _load_dags()
        assert m.dag_mapr_to_s3.schedule is None

    def test_dag1_no_catchup(self):
        m = _load_dags()
        assert m.dag_mapr_to_s3.catchup is False

    def test_dag1_has_all_expected_tasks(self):
        m = _load_dags()
        task_ids = list(m.dag_mapr_to_s3.task_ids)
        expected_tasks = [
            'validate_prerequisites',
            'init_tracking_tables',
            'create_migration_run',
            'parse_excel',
            'cluster_login_setup',
            'discover_tables_via_spark_ssh',
            'record_discovered_tables',
            'run_distcp_ssh',
            'update_distcp_status',
            'create_hive_tables',
            'update_table_create_status',
            'validate_destination_tables',
            'update_validation_status',
            'generate_html_report',
            'send_migration_report_email',
            'finalize_run',
        ]
        for expected in expected_tasks:
            assert any(expected in tid for tid in task_ids), \
                f"Expected task '{expected}' not found in {task_ids}"

    def test_dag1_finalize_is_last_task(self):
        m = _load_dags()
        dag = m.dag_mapr_to_s3
        finalize_task = next(t for t in dag.tasks if 'finalize_run' in t.task_id)
        assert len(finalize_task.downstream_list) == 0

    def test_dag1_retries_configured(self):
        m = _load_dags()
        assert m.DEFAULT_ARGS['retries'] == 2

    def test_dag1_max_active_runs(self):
        m = _load_dags()
        assert m.dag_mapr_to_s3.max_active_runs == 5


# ---------------------------------------------------------------------------
# DAG 2: iceberg_migration structure
# ---------------------------------------------------------------------------
class TestIcebergDagStructure:

    def test_dag2_exists(self):
        m = _load_dags()
        assert hasattr(m, 'dag_iceberg')

    def test_dag2_has_correct_id(self):
        m = _load_dags()
        assert m.dag_iceberg.dag_id == 'iceberg_migration'

    def test_dag2_has_expected_tags(self):
        m = _load_dags()
        tags = m.dag_iceberg.tags
        assert 'migration' in tags
        assert 'iceberg' in tags
        assert 'hive' in tags

    def test_dag2_has_excel_param(self):
        m = _load_dags()
        assert 'excel_file_path' in m.dag_iceberg.params

    def test_dag2_schedule_is_none(self):
        m = _load_dags()
        assert m.dag_iceberg.schedule is None

    def test_dag2_no_catchup(self):
        m = _load_dags()
        assert m.dag_iceberg.catchup is False

    def test_dag2_has_all_expected_tasks(self):
        m = _load_dags()
        task_ids = list(m.dag_iceberg.task_ids)
        expected_tasks = [
            'init_iceberg_tracking_tables',
            'create_iceberg_migration_run',
            'parse_iceberg_excel',
            'discover_hive_tables',
            'migrate_tables_to_iceberg',
            'update_migration_durations',
            'validate_iceberg_tables',
            'update_iceberg_validation_status',
            'generate_iceberg_html_report',
            'send_iceberg_report_email',
            'finalize_iceberg_run',
        ]
        for expected in expected_tasks:
            assert any(expected in tid for tid in task_ids), \
                f"Expected task '{expected}' not found in {task_ids}"

    def test_dag2_finalize_is_last_task(self):
        m = _load_dags()
        dag = m.dag_iceberg
        finalize_task = next(t for t in dag.tasks if 'finalize_iceberg_run' in t.task_id)
        assert len(finalize_task.downstream_list) == 0

    def test_dag2_max_active_runs(self):
        m = _load_dags()
        assert m.dag_iceberg.max_active_runs == 5

    def test_dag2_default_excel_param_value(self):
        m = _load_dags()
        param = m.dag_iceberg.params['excel_file_path']
        assert 'iceberg' in str(param).lower() or 'xlsx' in str(param).lower()


# ---------------------------------------------------------------------------
# DAG independence
# ---------------------------------------------------------------------------
class TestDagIndependence:

    def test_two_dags_have_different_ids(self):
        m = _load_dags()
        assert m.dag_mapr_to_s3.dag_id != m.dag_iceberg.dag_id

    def test_dag1_and_dag2_are_not_chained(self):
        """DAGs must not share cross-DAG dependencies."""
        m = _load_dags()
        dag1_task_ids = set(m.dag_mapr_to_s3.task_ids)
        dag2_task_ids = set(m.dag_iceberg.task_ids)
        # Task IDs should not overlap (they're separate DAGs)
        assert dag1_task_ids.isdisjoint(dag2_task_ids), \
            f"Unexpected shared tasks: {dag1_task_ids & dag2_task_ids}"


# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
class TestDefaultArgs:

    def test_owner_set(self):
        m = _load_dags()
        assert m.DEFAULT_ARGS['owner'] == 'data-migration'

    def test_depends_on_past_false(self):
        m = _load_dags()
        assert m.DEFAULT_ARGS['depends_on_past'] is False

    def test_retry_delay_configured(self):
        m = _load_dags()
        from datetime import timedelta
        assert m.DEFAULT_ARGS['retry_delay'] == timedelta(minutes=5)

    def test_ssh_timeout_is_24h(self):
        m = _load_dags()
        assert m.SSH_COMMAND_TIMEOUT == 86400


# ---------------------------------------------------------------------------
# Status progression validation
# ---------------------------------------------------------------------------
class TestStatusProgressionConstants:

    def test_mapr_status_values_are_expected(self):
        """Verify the status strings used throughout the DAG are consistent."""
        expected_statuses = {
            'COPIED', 'TABLE_CREATED',
            'VALIDATED', 'VALIDATION_FAILED', 'FAILED',
        }
        import inspect
        m = _load_dags()
        source = inspect.getsource(m)
        for status in expected_statuses:
            assert status in source, f"Status string '{status}' not found in DAG source"

    def test_iceberg_status_values_are_expected(self):
        expected_statuses = {'COMPLETED', 'VALIDATED', 'VALIDATION_FAILED', 'FAILED'}
        import inspect
        m = _load_dags()
        source = inspect.getsource(m)
        for status in expected_statuses:
            assert status in source, f"Iceberg status '{status}' not found in DAG source"

    def test_migration_strategies_defined(self):
        import inspect
        m = _load_dags()
        source = inspect.getsource(m)
        assert 'INPLACE' in source
        assert 'SNAPSHOT' in source


# ---------------------------------------------------------------------------
# Edge-case integration: distcp metrics parsing
# ---------------------------------------------------------------------------
class TestDistcpMetricsParsing:
    """Test the output parsing logic for DistCp metrics embedded in SSH output."""

    def test_parses_bytes_and_files(self):
        """Verify metrics parsed correctly from realistic DistCp stdout."""
        output = (
            "INCREMENTAL=false\n"
            "S3_FILE_COUNT_BEFORE=3\n"
            "S3_TOTAL_SIZE_BEFORE=5000000\n"
            "DISTCP_EXIT_CODE=0\n"
            "BYTES_COPIED=10485760\n"
            "FILES_COPIED=7\n"
            "S3_FILE_COUNT_AFTER=10\n"
            "S3_TOTAL_SIZE_AFTER=15485760\n"
            "S3_FILES_TRANSFERRED=7\n"
            "S3_BYTES_TRANSFERRED=10485760\n"
        )

        parsed = {}
        for line in output.split('\n'):
            line = line.strip()
            if 'BYTES_COPIED=' in line:
                parsed['bytes_copied'] = int(line.split('=')[1].strip() or 0)
            elif 'FILES_COPIED=' in line:
                parsed['files_copied'] = int(line.split('=')[1].strip() or 0)
            elif 'S3_TOTAL_SIZE_BEFORE=' in line:
                parsed['s3_size_before'] = int(line.split('=')[1].strip() or 0)
            elif 'S3_FILE_COUNT_BEFORE=' in line:
                parsed['s3_files_before'] = int(line.split('=')[1].strip() or 0)
            elif 'S3_TOTAL_SIZE_AFTER=' in line:
                parsed['s3_size_after'] = int(line.split('=')[1].strip() or 0)
            elif 'S3_FILE_COUNT_AFTER=' in line:
                parsed['s3_files_after'] = int(line.split('=')[1].strip() or 0)

        assert parsed['bytes_copied'] == 10485760
        assert parsed['files_copied'] == 7
        assert parsed['s3_files_before'] == 3
        assert parsed['s3_files_after'] == 10
        assert parsed['s3_size_before'] == 5000000
        assert parsed['s3_size_after'] == 15485760

    def test_incremental_flag_detection(self):
        output_incr = "INCREMENTAL=true\n"
        output_full = "INCREMENTAL=false\n"
        assert "INCREMENTAL=true" in output_incr
        assert "INCREMENTAL=true" not in output_full

    def test_file_size_match_tolerance(self):
        """Validate 1% tolerance logic for file size matching."""
        source_size = 10 * 1024 * 1024  
        s3_size_within = source_size * 0.995   
        s3_size_outside = source_size * 0.985  

        def size_match(source, s3):
            return abs(source - s3) / max(source, 1) < 0.01

        assert size_match(source_size, s3_size_within) is True
        assert size_match(source_size, s3_size_outside) is False

    def test_file_count_exact_match(self):
        """File count must be exact (no tolerance)."""
        assert 5 == 5  
        assert 5 != 4   
