import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"))

import migration_metrics_collector as m  # noqa: E402


def _rec(table, **kw):
    rec = {
        "pair": "a -> b", "database": "db", "table": table, "status": "OK", "error": None, "notes": [],
        "row_count": 10, "schema": [{"name": "id", "type": "int"}], "partition_schema": [],
        "registered_partitions": None, "partition_rows": None,
    }
    rec.update(kw)
    return rec


def _snap(side, tables, pairs=None):
    return {"side": side, "database_pairs": pairs or [{"source_db": "a", "dest_db": "b"}], "tables": tables}


def test_pair_id_and_table_ident():
    assert m.pair_id({"source_db": "s", "dest_db": "d"}) == "s -> d"
    assert m.pair_id({"source_db": "s", "dest_db": "d", "partition_filter": "dt>'1'"}) == "s -> d [dt>'1']"
    assert m.table_ident("", "db", "t`x") == "`db`.`t``x`"
    assert m.table_ident("cat", "db") == "`cat`.`db`"


def test_match_tables():
    names = ["orders", "Order_items", "tmp_a", "t1"]
    assert m.match_tables(names, ["*"], ["tmp_*"]) == ["orders", "Order_items", "t1"]
    assert m.match_tables(names, ["ORDER*"], None) == ["orders", "Order_items"]
    assert m.match_tables(names, None, None) == names


def test_partition_keys_keep_hive_escaping_and_unescape_for_filter():
    assert m.normalize_partition_spec("DT=2024-01-01/Region=a%2Fb/") == "dt=2024-01-01/region=a%2Fb"
    assert m.partition_values("dt=x/r=a%2Fb/n=__HIVE_DEFAULT_PARTITION__", ["dt", "r", "n", "zz"]) == [
        "x", "a/b", None, None,
    ]


def test_filter_partition_keys_without_predicate_needs_no_spark():
    assert m.filter_partition_keys(None, ["dt=1", "dt=2"], ["dt"], "") == {"dt=1", "dt=2"}
    assert m.filter_partition_keys(None, [], ["dt"], "dt > 1") == set()


def test_classify_data_file():
    assert m.classify_data_file("/dt=1/r=x/part-0.parquet", 2, "hive") == "dt=1/r=x"
    assert m.classify_data_file("/DT=1/r=x/sub/part-0.parquet", 2, "hive") == "dt=1/r=x"
    assert m.classify_data_file("/part-0.parquet", 0, "hive") == ""
    assert m.classify_data_file("/stray/part-0.parquet", 1, "hive") == ""
    assert m.classify_data_file("/dt=1/_SUCCESS", 1, "hive") is None
    assert m.classify_data_file("/dt=1/.part-0.crc", 1, "hive") is None
    assert m.classify_data_file("/_temporary/0/dt=1/f", 1, "hive") is None
    assert m.classify_data_file("/metadata/v1.metadata.json", 1, "iceberg") is None
    assert m.classify_data_file("/data/dt=1/f.parquet", 1, "iceberg") == "dt=1"


def test_parse_describe_hive_table():
    rows = [
        ("id", "int"), ("dt", "string"),
        ("# Partition Information", ""), ("# col_name", "data_type"), ("DT", "string"),
        ("", ""),
        ("# Detailed Table Information", ""), ("Type", "EXTERNAL"), ("Provider", "hive"),
        ("Location", "s3a://b/t"),
    ]
    info = m.parse_describe(rows)
    assert info["partition_columns"] == ["dt"] and info["partition_transforms"] == []
    assert (info["type"], info["provider"], info["location"]) == ("EXTERNAL", "hive", "s3a://b/t")


def test_parse_describe_iceberg_identity_and_transform():
    rows = [
        ("id", "bigint"), ("# Partitioning", ""), ("Part 0", "region"), ("Part 1", "days(ts)"),
        ("# Metadata Columns", ""), ("_spec_id", "int"),
        ("# Detailed Table Information", ""), ("Provider", "iceberg"),
    ]
    info = m.parse_describe(rows)
    assert info["partition_columns"] == ["region"] and info["partition_transforms"] == ["days(ts)"]
    assert info["provider"] == "iceberg"


def test_compare_columns_wording():
    src = [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]
    dest = [{"name": "a", "type": "BIGINT"}, {"name": "c", "type": "string"}]
    assert m.compare_columns(src, dest, "column") == [
        "Type mismatch for column a: int (source) vs bigint (dest)",
        "Missing column on dest: b",
        "Extra column on dest: c",
    ]
    assert m.compare_columns(None, None, "column") == []


def test_compare_table_pass():
    s = _rec("t", registered_partitions=["dt=1"], data_file_count=2, data_bytes=10, unregistered_partitions=[])
    result, diffs = m.compare_table(s, dict(s))
    assert result["status"] == "PASS" and result["issues"] == "" and diffs == []


def test_compare_table_row_and_schema_mismatch_fail():
    s = _rec("t")
    d = _rec("t", row_count=7, schema=[{"name": "id", "type": "int"}, {"name": "x", "type": "int"}])
    result, _ = m.compare_table(s, d)
    assert result["status"] == "FAIL"
    assert "Row count mismatch: source=10, dest=7" in result["issues"]
    assert result["schema_differences"] == "Extra column on dest: x"


def test_compare_table_partition_schema_mismatch_fail():
    s = _rec("t", partition_schema=[{"name": "dt", "type": "date"}])
    d = _rec("t", partition_schema=[{"name": "dt", "type": "string"}])
    result, _ = m.compare_table(s, d)
    assert result["status"] == "FAIL" and "partition column dt: date (source) vs string" in result["issues"]


def test_compare_table_storage_and_registration_only_warn():
    s = _rec("t", registered_partitions=["dt=1", "dt=2"], data_file_count=4, data_bytes=40,
             storage_partitions={"dt=1": [2, 20], "dt=2": [2, 20]}, unregistered_partitions=[])
    d = _rec("t", registered_partitions=["dt=1"], data_file_count=5, data_bytes=41,
             storage_partitions={"dt=1": [3, 21], "dt=2": [2, 20]}, unregistered_partitions=["dt=2"])
    result, diffs = m.compare_table(s, d)
    assert result["status"] == "WARN"
    for text in ("Partition count mismatch: source=2, dest=1", "MSCK REPAIR TABLE",
                 "Data file count mismatch", "Data size mismatch"):
        assert text in result["issues"]
    assert [(x["partition"], x["issue"]) for x in diffs] == [
        ("dt=1", "FILE_STATS_MISMATCH"), ("dt=2", "NOT_REGISTERED_ON_DEST"),
    ]
    assert diffs[0]["source_files"] == 2 and diffs[0]["dest_bytes"] == 21


def test_compare_table_terminal_statuses():
    ok = _rec("t")
    assert m.compare_table(_rec("t", status="VIEW"), None)[0]["status"] == "SKIPPED_VIEW"
    assert m.compare_table(None, ok)[0]["status"] == "EXTRA_ON_DEST"
    assert m.compare_table(ok, None)[0]["status"] == "MISSING_ON_DEST"
    err = m.compare_table(_rec("t", status="ERROR", error="boom"), ok)[0]
    assert err == {"status": "ERROR", "issues": "source: boom"}
    assert m.compare_table(ok, _rec("t", status="ERROR", error="x"))[0]["issues"] == "dest: x"
    assert m.compare_table(ok, _rec("t", status="VIEW"))[0]["status"] == "FAIL"


def test_partition_row_diffs():
    s = _rec("t", partition_rows={"dt=1": 5, "dt=2": 3})
    d = _rec("t", partition_rows={"dt=1": 4, "dt=3": 1})
    diffs = m.partition_diffs(s, d)
    assert [(x["partition"], x["issue"], x["source_rows"], x["dest_rows"]) for x in diffs] == [
        ("dt=1", "ROW_COUNT_MISMATCH", 5, 4),
        ("dt=2", "ROWS_MISSING_ON_DEST", 3, None),
        ("dt=3", "ROWS_EXTRA_ON_DEST", None, 1),
    ]


def test_compare_snapshots_sorts_worst_first_and_reports_db_errors_once():
    src = _snap("source", [_rec("ok"), _rec("bad"), _rec("gone")])
    dest = _snap("dest", [_rec("ok"), _rec("BAD", row_count=1), _rec("new")])
    rows, diffs, warnings = m.compare_snapshots(src, dest)
    assert [(r["table"], r["status"]) for r in rows] == [
        ("bad", "FAIL"), ("gone", "MISSING_ON_DEST"), ("new", "EXTRA_ON_DEST"), ("ok", "PASS"),
    ]
    assert warnings == [] and diffs == []

    dest_db_down = _snap("dest", [{"pair": "a -> b", "database": "b", "table": None, "status": "ERROR",
                                   "error": "cannot list tables in b: nope"}])
    rows, _, _ = m.compare_snapshots(src, dest_db_down)
    assert [(r["table"], r["status"]) for r in rows] == [("*", "ERROR")]
    assert rows[0]["issues"] == "dest: cannot list tables in b: nope"


def test_compare_snapshots_warns_when_pairs_differ():
    src = _snap("source", [], pairs=[{"source_db": "a", "dest_db": "b"}])
    dest = _snap("dest", [], pairs=[{"source_db": "a", "dest_db": "c"}])
    assert m.compare_snapshots(src, dest)[2]


def test_describe_record_and_fmt_bytes():
    rec = _rec("t", registered_partitions=["dt=1"], data_file_count=1200, data_bytes=3 * 1024 ** 3,
               collect_seconds=1.5)
    assert m.describe_record(rec) == "db.t: rows=10 partitions=1 files=1,200 (3.00 GB) [1.5s]"
    assert m.describe_record(_rec("t", status="ERROR", error="boom")) == "db.t: ERROR boom"
    assert m.fmt_bytes(512) == "512 B" and m.fmt_bytes(None) == ""


def test_parse_args():
    a = m.parse_args(["--config", "c.json", "--output", "o.json"])
    assert (a.config, a.output) == ("c.json", "o.json")
