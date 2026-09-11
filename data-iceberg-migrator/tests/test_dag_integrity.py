"""DAG integrity tests: verify DAG IDs and that all DAGs loaded without import errors."""

from unittest.mock import patch

import migration_dag_folder_copy as m3
import migration_dag_iceberg as m2
import migration_dag_mapr_to_s3 as m1
import migration_dag_parquet_hms as m4


class TestMaprToS3DagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert m1.dag_mapr_to_s3.dag_id == 'source_to_s3_migration'

    def test_excel_param_defined(self):
        assert 'excel_file_path' in m1.dag_mapr_to_s3.params


class TestIcebergDagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert m2.dag_iceberg.dag_id == 'iceberg_migration'

    def test_excel_param_defined(self):
        assert 'excel_file_path' in m2.dag_iceberg.params

    def test_inplace_text_ctas_param_defined(self):
        assert 'iceberg_inplace_text_ctas' in m2.dag_iceberg.params


class TestFolderCopyDagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert m3.dag_folder_copy.dag_id == 'folder_only_data_copy'

    def test_excel_param_defined(self):
        assert 'excel_file_path' in m3.dag_folder_copy.params


class TestParquetHmsDagIntegrity:

    def test_dag_loads_with_correct_id(self):
        assert m4.dag_parquet_hms.dag_id == 'parquet_hms_registration'

    def test_excel_param_defined(self):
        assert 'excel_file_path' in m4.dag_parquet_hms.params

class TestDagOwnerResolution:
    """The owner literal is the task pod's HADOOP_USER_NAME, not just a UI label.

    A pod_mutation_hook in nx1-full-package reads the dag.owners column and
    injects it into every task pod as HADOOP_USER_NAME/SPARK_USER, so this is
    the principal Ranger authorizes for direct S3/HDFS access.
    """

    _MODULES = (
        'migration_dag_mapr_to_s3',
        'migration_dag_iceberg',
        'migration_dag_folder_copy',
        'migration_dag_parquet_hms',
        'migration_dag_iceberg_catalog',
    )

    def _resolve_all(self, **patch_kwargs):
        import importlib
        with patch('airflow.models.Variable.get', **patch_kwargs) as mock_get:
            resolved = {
                name: importlib.import_module(name)._resolve_dag_owner()
                for name in self._MODULES
            }
        return resolved, mock_get

    def test_reads_the_plain_migration_dag_owner_variable(self):
        resolved, _ = self._resolve_all(return_value='alice')
        assert resolved == dict.fromkeys(self._MODULES, 'alice')

    def test_never_reads_a_portal_namespaced_key(self):
        """Parse time has no run, so it must stay out of the nx1_ namespace."""
        _, mock_get = self._resolve_all(return_value='alice')
        read_keys = {call.args[0] for call in mock_get.call_args_list}
        assert read_keys == {'migration_dag_owner'}

    def test_strips_surrounding_whitespace(self):
        """A stray space would make a different Hadoop user."""
        resolved, _ = self._resolve_all(return_value='  alice  ')
        assert resolved == dict.fromkeys(self._MODULES, 'alice')

    def test_falls_back_to_literal_when_variable_is_blank(self):
        """Airflow reports a Variable cleared in the UI as present but empty."""
        resolved, _ = self._resolve_all(return_value='   ')
        assert resolved == dict.fromkeys(self._MODULES, 'data-migration')

    def test_falls_back_to_literal_when_owner_contains_a_comma(self):
        """The pod hook splits dag.owners on ',', so a comma silently truncates."""
        resolved, _ = self._resolve_all(return_value='alice,bob')
        assert resolved == dict.fromkeys(self._MODULES, 'data-migration')

    def test_falls_back_to_literal_when_the_lookup_fails(self):
        """Raising at parse time would drop the DAG from Airflow entirely."""
        resolved, _ = self._resolve_all(side_effect=RuntimeError('metadata db down'))
        assert resolved == dict.fromkeys(self._MODULES, 'data-migration')

    def test_keeps_the_literal_deploy_py_rewrites(self):
        """deploy.py swaps `return 'data-migration'` for the --owner value."""
        from pathlib import Path
        root = Path(__file__).resolve().parent.parent
        for name in self._MODULES:
            source = (root / f'{name}.py').read_text()
            assert "return 'data-migration'" in source, name
