"""Tests for the standalone iceberg_cleanup remediation script."""

from unittest.mock import MagicMock

import iceberg_cleanup as ic
import pytest


def _row(col_name, data_type):
    r = MagicMock()
    r.col_name = col_name
    r.data_type = data_type
    return r


def _make_router(sql_calls, provider="iceberg", location="s3a://bucket/db/tbl",
                 describe_ok=True, drop_raises=False):
    def router(sql):
        sql_calls.append(sql)
        s = sql.strip().upper()
        df = MagicMock()
        if s.startswith("DESCRIBE FORMATTED"):
            rows = []
            if provider:
                rows.append(_row("Provider", provider))
            if location:
                rows.append(_row("Location", location))
            df.collect.return_value = rows
        elif s.startswith("DESCRIBE "):
            if not describe_ok:
                raise Exception("Table or view not found")
            df.collect.return_value = []
        elif s.startswith("DROP TABLE"):
            if drop_raises:
                raise Exception("drop failed")
            df.collect.return_value = []
        else:
            df.collect.return_value = []
        return df
    return router


class TestIsIcebergTable:

    def test_true_for_iceberg_provider(self, mock_spark):
        mock_spark.sql.side_effect = _make_router([], provider="iceberg")
        assert ic.is_iceberg_table(mock_spark, "db.tbl") is True

    def test_false_for_hive_table(self, mock_spark):
        mock_spark.sql.side_effect = _make_router([], provider="hive")
        assert ic.is_iceberg_table(mock_spark, "db.tbl") is False

    def test_false_when_describe_fails(self, mock_spark):
        mock_spark.sql.side_effect = Exception("boom")
        assert ic.is_iceberg_table(mock_spark, "db.tbl") is False


class TestResolveTables:

    def test_qualified_names_pass_through(self):
        assert ic.resolve_tables(None, "a.b, c.d") == ["a.b", "c.d"]

    def test_bare_names_qualified_with_database(self):
        assert ic.resolve_tables("db", "t1, t2") == ["db.t1", "db.t2"]

    def test_bare_name_without_database_raises(self):
        with pytest.raises(ValueError):
            ic.resolve_tables(None, "t1")


class TestCleanupTable:

    def test_skips_not_found(self, mock_spark):
        sql_calls = []
        mock_spark.sql.side_effect = _make_router(sql_calls, describe_ok=False)
        result = ic.cleanup_table(mock_spark, "db.tbl", dry_run=False)
        assert result["status"] == "SKIPPED_NOT_FOUND"
        assert "DROP TABLE" not in " ".join(sql_calls).upper()

    def test_skips_non_iceberg(self, mock_spark):
        sql_calls = []
        mock_spark.sql.side_effect = _make_router(sql_calls, provider="hive")
        result = ic.cleanup_table(mock_spark, "db.tbl", dry_run=False)
        assert result["status"] == "SKIPPED_NOT_ICEBERG"
        assert "DROP TABLE" not in " ".join(sql_calls).upper()

    def test_dry_run_makes_no_changes(self, mock_spark):
        sql_calls = []
        mock_spark.sql.side_effect = _make_router(sql_calls, provider="iceberg")
        result = ic.cleanup_table(mock_spark, "db.tbl", dry_run=True)
        assert result["status"] == "DRY_RUN"
        assert result["location"] == "s3a://bucket/db/tbl"
        assert "DROP TABLE" not in " ".join(sql_calls).upper()
        # metadata folder is not touched in dry run
        assert not mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value.delete.called

    def test_apply_drops_no_purge_and_deletes_metadata(self, mock_spark):
        sql_calls = []
        mock_spark.sql.side_effect = _make_router(sql_calls, provider="iceberg")
        result = ic.cleanup_table(mock_spark, "db.tbl", dry_run=False)
        assert result["status"] == "CLEANED"
        joined = " ".join(sql_calls).upper()
        assert "DROP TABLE IF EXISTS DB.TBL" in joined
        assert "PURGE" not in joined  # metadata-only drop; data preserved
        # metadata/ folder deleted recursively
        fs = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        assert fs.delete.called

    def test_apply_reports_failure(self, mock_spark):
        sql_calls = []
        mock_spark.sql.side_effect = _make_router(sql_calls, provider="iceberg", drop_raises=True)
        result = ic.cleanup_table(mock_spark, "db.tbl", dry_run=False)
        assert result["status"] == "FAILED"
        assert result["error"]


class TestDeleteMetadataFolder:

    def test_deletes_metadata_path(self, mock_spark):
        fs = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        fs.exists.return_value = True
        assert ic.delete_metadata_folder(mock_spark, "s3a://bucket/db/tbl") is True
        assert fs.delete.called

    def test_noop_when_no_metadata_folder(self, mock_spark):
        fs = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        fs.exists.return_value = False
        assert ic.delete_metadata_folder(mock_spark, "s3a://bucket/db/tbl") is False
        assert not fs.delete.called


class TestCleanupTables:

    def test_processes_all_tables(self, mock_spark):
        mock_spark.sql.side_effect = _make_router([], provider="iceberg")
        results = ic.cleanup_tables(mock_spark, ["db.a", "db.b"], dry_run=True)
        assert [r["table"] for r in results] == ["db.a", "db.b"]
        assert all(r["status"] == "DRY_RUN" for r in results)
