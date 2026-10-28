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


def test_no_tables_message():
    assert r.no_tables_message("db", ["a"], ["x*"], ["a"]) is None
    assert r.no_tables_message("db", ["a", "b"], ["x*", "y"], []) == "No tables in db matched: x*,y"
    assert r.no_tables_message("db", [], [], []) == "No tables found in db"
    assert r.no_tables_message("db", [], ["x"], []) == "No tables found in db"


def test_format_report_shows_zero_rows_without_out_of_dir_partitions():
    res = [{"table": "t", "status": "ok", "total_partitions": 3, "partitions": []}]
    text = r.format_report("db", res, True)
    assert "db.t: partitions=3, out_of_dir=0, files=0, bytes=0, rows=0" in text
    assert "None" not in text
    assert "rows=" not in r.format_report("db", res, False)


class TestIsPathInsideRoot:

    ROOT = 'maprfs://cluster/a/b'

    def test_same_path(self):
        assert r._is_path_inside_root('maprfs://cluster/a/b', self.ROOT) is True

    def test_child(self):
        assert r._is_path_inside_root('maprfs://cluster/a/b/dt=1', self.ROOT) is True

    def test_sibling_with_shared_prefix(self):
        assert r._is_path_inside_root('maprfs://cluster/a/bc', self.ROOT) is False
        assert r._is_path_inside_root('maprfs://cluster/a/bc/dt=1', self.ROOT) is False

    def test_trailing_slash_on_either_side(self):
        assert r._is_path_inside_root('maprfs://cluster/a/b/', self.ROOT) is True
        assert r._is_path_inside_root('maprfs://cluster/a/b/dt=1/', self.ROOT + '/') is True

    def test_different_scheme(self):
        assert r._is_path_inside_root('hdfs://cluster/a/b/dt=1', self.ROOT) is False

    def test_different_authority(self):
        assert r._is_path_inside_root('maprfs://other/a/b/dt=1', self.ROOT) is False

    def test_empty_values_are_not_inside(self):
        assert r._is_path_inside_root('', self.ROOT) is False
        assert r._is_path_inside_root(self.ROOT, '') is False

    def test_normalize_path_keeps_bare_slash(self):
        assert r._normalize_path('/') == '/'
        assert r._normalize_path(' /a/b// ') == '/a/b'

    def test_missing_scheme_or_authority_compares_paths_only(self):
        assert r._is_path_inside_root('/a/b/dt=1', self.ROOT) is True
        assert r._is_path_inside_root('maprfs:///a/b/dt=1', self.ROOT) is True
        assert r._is_path_inside_root('maprfs:/a/b/dt=1', self.ROOT) is True
        assert r._is_path_inside_root('/a/bc/dt=1', self.ROOT) is False

    def test_scheme_is_compared_case_insensitively(self):
        assert r._is_path_inside_root('MAPRFS://cluster/a/b/dt=1', self.ROOT) is True
        assert r._is_path_inside_root('HDFS://cluster/a/b/dt=1', self.ROOT) is False
