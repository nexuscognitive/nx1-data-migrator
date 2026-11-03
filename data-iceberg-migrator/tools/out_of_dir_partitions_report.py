import argparse
import csv
import fnmatch
import re
import sys

try:
    from partition_utils import apply_partition_filter, partitions_to_where_clause
except ImportError:
    from migrator_utils.migrations.partition_utils import (
        apply_partition_filter,
        partitions_to_where_clause,
    )

CSV_COLUMNS = ["database", "table", "partition", "location", "exists", "file_count", "size_bytes", "row_count"]


_LOCATION_RE = re.compile(r"^(?:([A-Za-z][A-Za-z0-9+.-]*):)?(?://([^/]*))?(.*)$")


def _normalize_path(path):
    path = (path or "").strip()
    while len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    return path


def _split_location(path):
    m = _LOCATION_RE.match(_normalize_path(path))
    scheme, authority, rest = m.group(1), m.group(2), m.group(3)
    return (scheme or "").lower(), authority or "", re.sub(r"/{2,}", "/", rest)


def _is_path_inside_root(path, root):
    p_scheme, p_auth, p_path = _split_location(path)
    r_scheme, r_auth, r_path = _split_location(root)
    if not p_path or not r_path:
        return False
    if p_scheme and r_scheme and p_scheme != r_scheme:
        return False
    if p_auth and r_auth and p_auth != r_auth:
        return False
    p_path = _normalize_path(p_path)
    r_path = _normalize_path(r_path)
    return p_path == r_path or p_path.startswith(r_path.rstrip("/") + "/")


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="List partitions located outside their table directory")
    ap.add_argument("--database", required=True)
    ap.add_argument("--tables", default="")
    ap.add_argument("--partition-filter", dest="partition_filter", default=None)
    ap.add_argument("--row-counts", dest="row_counts", action="store_true")
    ap.add_argument("--output", default=None)
    args = ap.parse_args(argv)
    args.tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    return args


def match_tables(all_tables, tokens):
    if not tokens:
        return list(all_tables)
    return [t for t in all_tables if any(fnmatch.fnmatch(t.lower(), k.lower()) for k in tokens)]


def no_tables_message(database, all_tables, tokens, matched):
    if matched:
        return None
    if not all_tables:
        return "No tables found in %s" % database
    return "No tables in %s matched: %s" % (database, ",".join(tokens))


def find_out_of_dir(part_locations, root, qualify, partition_filter=None):
    names = sorted(part_locations)
    if partition_filter:
        names = apply_partition_filter(names, partition_filter)
    root_q = qualify(root)
    return [(n, part_locations[n]) for n in names if not _is_path_inside_root(qualify(part_locations[n]), root_q)]


def summarize(result):
    parts = result.get("partitions", [])
    counts = [p["row_count"] for p in parts if p.get("row_count") is not None]
    return {
        "total_partitions": result.get("total_partitions", 0),
        "out_of_dir_count": len(parts),
        "total_files": sum(p.get("file_count") or 0 for p in parts),
        "total_bytes": sum(p.get("size_bytes") or 0 for p in parts),
        "total_rows": sum(counts) if counts else None,
    }


def csv_rows(database, result):
    return [
        [
            database,
            result["table"],
            p["partition"],
            p["location"],
            p["exists"],
            p["file_count"],
            p["size_bytes"],
            "" if p.get("row_count") is None else p["row_count"],
        ]
        for p in result.get("partitions", [])
    ]


def format_report(database, results, row_counts=False):
    lines = []
    for r in results:
        head = "%s.%s" % (database, r["table"])
        if r["status"] == "skipped":
            lines.append("%s: SKIPPED (%s)" % (head, r["reason"]))
            continue
        if r["status"] == "error":
            lines.append("%s: ERROR (%s)" % (head, r["reason"]))
            continue
        s = summarize(r)
        rows = "" if not row_counts else ", rows=%s" % (s["total_rows"] or 0)
        lines.append(
            "%s: partitions=%d, out_of_dir=%d, files=%d, bytes=%d%s"
            % (head, s["total_partitions"], s["out_of_dir_count"], s["total_files"], s["total_bytes"], rows)
        )
        for p in r["partitions"]:
            extra = "" if p.get("row_count") is None else " rows=%s" % p["row_count"]
            lines.append(
                "    %s -> %s | exists=%s files=%s bytes=%s%s"
                % (p["partition"], p["location"], p["exists"], p["file_count"], p["size_bytes"], extra)
            )
    return "\n".join(lines)


def write_csv(path, database, results):
    if sys.version_info[0] >= 3:
        f = open(path, "w", newline="")
    else:
        f = open(path, "wb")
    with f:
        w = csv.writer(f)
        w.writerow(CSV_COLUMNS)
        for r in results:
            for row in csv_rows(database, r):
                w.writerow(row)


def _location_row(spark, sql):
    for row in spark.sql(sql).collect():
        if (row.col_name or "").strip().rstrip(":").lower() == "location":
            return (row.data_type or "").strip()
    return None


def _partition_columns(spark, qualified):
    cols, in_section = [], False
    for row in spark.sql("DESCRIBE FORMATTED %s" % qualified).collect():
        name = (row.col_name or "").strip()
        if name == "# Partition Information":
            in_section = True
        elif in_section and name == "# col_name":
            continue
        elif in_section and name.startswith("#"):
            break
        elif in_section and name:
            cols.append(name)
    return cols


def _bulk_partition_locations(spark, db, tbl, cols):
    catalog = spark._jsparkSession.sessionState().catalog().externalCatalog()
    seq = catalog.listPartitions(db, tbl, spark._jvm.scala.Option.empty())
    conv = spark._jvm.scala.collection.JavaConverters
    locations = {}
    for cp in conv.seqAsJavaListConverter(seq).asJava():
        spec = conv.mapAsJavaMapConverter(cp.spec()).asJava()
        uri = cp.storage().locationUri()
        if uri.isDefined():
            locations["/".join("%s=%s" % (c, spec.get(c)) for c in cols)] = uri.get().toString()
    return locations, int(seq.size())


def _make_qualify(spark):
    jpath = spark._jvm.org.apache.hadoop.fs.Path
    conf = spark._jsc.hadoopConfiguration()

    def qualify(x):
        try:
            p = jpath(x)
            return p.getFileSystem(conf).makeQualified(p).toString()
        except Exception:
            return x.strip()

    return qualify


def _probe(spark, location):
    path = spark._jvm.org.apache.hadoop.fs.Path(location)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return False, 0, 0
    cs = fs.getContentSummary(path)
    return True, int(cs.getFileCount()), int(cs.getLength())


def inspect_table(spark, db, tbl, qualify, partition_filter=None, row_counts=False):
    qualified = "%s.%s" % (db, tbl)
    result = {"table": tbl, "status": "ok", "reason": None, "total_partitions": 0, "partitions": []}
    try:
        cols = _partition_columns(spark, qualified)
        if not cols:
            result.update(status="skipped", reason="not partitioned")
            return result
        root = _location_row(spark, "DESCRIBE FORMATTED %s" % qualified)
        if not root:
            result.update(status="skipped", reason="no table location")
            return result
        locations, total = _bulk_partition_locations(spark, db, tbl, cols)
        result["total_partitions"] = total
        for name, loc in find_out_of_dir(locations, root, qualify, partition_filter):
            exists, files, size = _probe(spark, loc)
            item = {
                "partition": name,
                "location": qualify(loc),
                "exists": exists,
                "file_count": files,
                "size_bytes": size,
                "row_count": None,
            }
            if row_counts:
                where = partitions_to_where_clause([name])
                item["row_count"] = int(
                    spark.sql("SELECT COUNT(*) AS c FROM %s WHERE %s" % (qualified, where)).collect()[0].c
                )
            result["partitions"].append(item)
    except Exception as e:
        result.update(status="error", reason=str(e)[:300])
    return result


def main(argv=None):
    args = parse_args(argv)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("out_of_dir_partitions_report").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    all_tables = [r.tableName for r in spark.sql("SHOW TABLES IN %s" % args.database).collect()]
    matched = match_tables(all_tables, args.tables)
    message = no_tables_message(args.database, all_tables, args.tables, matched)
    if message:
        print(message)
        spark.stop()
        return
    qualify = _make_qualify(spark)
    results = []
    for tbl in matched:
        results.append(inspect_table(spark, args.database, tbl, qualify, args.partition_filter, args.row_counts))
    print(format_report(args.database, results, args.row_counts))
    if args.output:
        write_csv(args.output, args.database, results)
    spark.stop()


if __name__ == "__main__":
    main()
