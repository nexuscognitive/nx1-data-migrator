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


class TestDagOwnerIsNotReadFromVariables:
    """DAG parse time has no run, so it cannot tell one origin from the other.

    Reading the portal's migration_dag_owner here pinned every hand-launched run
    to the last portal user and overrode deploy.py --owner.
    """

    _MODULES = (
        'migration_dag_mapr_to_s3',
        'migration_dag_iceberg',
        'migration_dag_folder_copy',
        'migration_dag_parquet_hms',
    )

    def test_resolve_dag_owner_ignores_the_variable(self):
        import importlib
        with patch('airflow.models.Variable.get', return_value='portal_user'):
            for name in self._MODULES:
                mod = importlib.import_module(name)
                assert mod._resolve_dag_owner() == 'data-migration', name

    def test_resolve_dag_owner_keeps_the_literal_deploy_py_rewrites(self):
        """deploy.py swaps `return 'data-migration'` for the --owner value."""
        from pathlib import Path
        root = Path(__file__).resolve().parent.parent
        for name in self._MODULES:
            source = (root / f'{name}.py').read_text()
            assert "return 'data-migration'" in source, name
