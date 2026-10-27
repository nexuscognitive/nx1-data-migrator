import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"))

import out_of_dir_partitions_report as r  # noqa: E402


def _part(name, loc, files=2, size=10, rows=None, exists=True):
    return {"partition": name, "location": loc, "exists": exists, "file_count": files,
            "size_bytes": size, "row_count": rows}


def test_parse_args_defaults_and_options():
    a = r.parse_args(["--database", "db"])
    assert a.database == "db" and a.tables == [] and a.row_counts is False
    assert a.partition_filter is None and a.output is None
    a = r.parse_args(["--database", "db", "--tables", "a, b_*", "--partition-filter", "dt=1",
                      "--row-counts", "--output", "/tmp/x.csv"])
    assert a.tables == ["a", "b_*"] and a.partition_filter == "dt=1"
    assert a.row_counts is True and a.output == "/tmp/x.csv"


def test_match_tables():
    names = ["orders", "Order_items", "t1"]
    assert r.match_tables(names, []) == names
    assert r.match_tables(names, ["order*"]) == ["orders", "Order_items"]


def test_find_out_of_dir_flags_sibling_prefix_and_scheme():
    locs = {"dt=1": "maprfs://c/a/b/dt=1", "dt=2": "maprfs://c/a/bc/dt=2",
            "dt=3": "hdfs://c/a/b/dt=3", "dt=4": "maprfs://c/z/dt=4"}
    found = r.find_out_of_dir(locs, "maprfs://c/a/b/", lambda x: x)
    assert [n for n, _ in found] == ["dt=2", "dt=3", "dt=4"]


def test_find_out_of_dir_applies_filter_and_qualify():
    locs = {"dt=1": "/x/dt=1", "dt=2": "/x/dt=2"}
    found = r.find_out_of_dir(locs, "/a", lambda x: "maprfs:" + x, "dt=2")
    assert found == [("dt=2", "/x/dt=2")]


def test_summarize_with_and_without_row_counts():
    res = {"table": "t", "total_partitions": 5,
           "partitions": [_part("dt=1", "/x/1", 2, 10, 7), _part("dt=2", "/x/2", 3, 20, 3)]}
    s = r.summarize(res)
    assert s == {"total_partitions": 5, "out_of_dir_count": 2, "total_files": 5,
                 "total_bytes": 30, "total_rows": 10}
    res["partitions"] = [_part("dt=1", "/x/1")]
    assert r.summarize(res)["total_rows"] is None
    assert r.summarize({"table": "t"})["out_of_dir_count"] == 0


def test_csv_rows_and_write_csv(tmp_path):
    res = [{"table": "t", "status": "ok", "total_partitions": 1,
            "partitions": [_part("dt=1", "/x/1", 2, 10, 7), _part("dt=2", "/x/2", 0, 0, None, False)]},
           {"table": "u", "status": "skipped", "reason": "not partitioned", "partitions": []}]
    assert r.csv_rows("db", res[0])[0] == ["db", "t", "dt=1", "/x/1", True, 2, 10, 7]
    assert r.csv_rows("db", res[0])[1][-1] == ""
    out = str(tmp_path / "r.csv")
    r.write_csv(out, "db", res)
    with open(out) as f:
        rows = list(csv.reader(f))
    assert rows[0] == r.CSV_COLUMNS and len(rows) == 3


def test_format_report_lists_skipped_error_and_partitions():
    res = [{"table": "t", "status": "ok", "total_partitions": 4,
            "partitions": [_part("dt=1", "/x/1", 2, 10, 7)]},
           {"table": "u", "status": "skipped", "reason": "not partitioned", "partitions": []},
           {"table": "v", "status": "error", "reason": "boom", "partitions": []}]
    text = r.format_report("db", res, True)
    assert "db.t: partitions=4, out_of_dir=1, files=2, bytes=10, rows=7" in text
    assert "dt=1 -> /x/1 | exists=True files=2 bytes=10 rows=7" in text
    assert "db.u: SKIPPED (not partitioned)" in text
    assert "db.v: ERROR (boom)" in text
