# -*- coding: utf-8 -*-
"""Collect per-table migration validation metrics into a JSON snapshot, and compare two snapshots.

The source side runs on an edge node via spark-submit, so this file stays Python 2.7 and
Spark 2.x compatible (no f-strings), like DAG1's discovery script. The validation notebook
imports it for the destination side and for the comparison, so both sides are measured by
the same code.

    spark-submit --master 'local[*]' migration_metrics_collector.py --config cfg.json --output out.json
"""
from __future__ import print_function

import argparse
import contextlib
import fnmatch
import json
import re
import sys
import time
from multiprocessing.pool import ThreadPool

try:
    from urllib.parse import unquote
except ImportError:
    from urllib import unquote

HIVE_NULL = "__HIVE_DEFAULT_PARTITION__"
SNAPSHOT_MARKER = "SNAPSHOT_WRITTEN"


def log(msg):
    print(msg)
    sys.stdout.flush()


def quote_ident(name):
    return "`" + name.replace("`", "``") + "`"


def table_ident(catalog, db, table=None):
    return ".".join(quote_ident(p) for p in (catalog, db, table) if p)


def pair_id(pair):
    pf = pair.get("partition_filter") or ""
    return "{0} -> {1}".format(pair["source_db"], pair["dest_db"]) + (" [{0}]".format(pf) if pf else "")


def short_error(exc, limit=500):
    msg = str(exc).strip()
    return (msg.splitlines()[0] if msg else type(exc).__name__)[:limit]


def fmt_bytes(n):
    if n is None:
        return ""
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return "{0:.0f} {1}".format(n, unit) if unit == "B" else "{0:.2f} {1}".format(n, unit)
        n /= 1024


def match_tables(names, includes, excludes):
    def matches(name, patterns):
        return any(fnmatch.fnmatch(name.lower(), p.lower()) for p in patterns)

    return [n for n in names if matches(n, includes or ["*"]) and not matches(n, excludes or [])]


# ── Partition keys ──────────────────────────────────────────────────────────────────
# SHOW PARTITIONS and partition directory names share Hive's escaping (e.g. "a%2Fb"), so keys
# from those two sources are compared escaped. Values are only unescaped for partition_filter.


def normalize_partition_spec(spec):
    segments = []
    for seg in spec.strip().strip("/").split("/"):
        key, sep, value = seg.partition("=")
        if sep:
            segments.append("{0}={1}".format(key.lower(), value))
    return "/".join(segments)


def partition_values(key, part_cols):
    values = {}
    for seg in key.split("/"):
        col, _, value = seg.partition("=")
        value = unquote(value)
        values[col] = None if value == HIVE_NULL else value
    return [values.get(c) for c in part_cols]


def filter_partition_keys(spark, keys, part_cols, predicate):
    """Keep the partition keys that satisfy a Spark SQL predicate on the partition columns."""
    keys = list(keys)
    if not predicate or not keys:
        return set(keys)
    from pyspark.sql.types import StringType, StructField, StructType

    rows = [tuple([k] + partition_values(k, part_cols)) for k in keys]
    schema = StructType([StructField("__key", StringType())] + [StructField(c, StringType()) for c in part_cols])
    return set(r["__key"] for r in spark.createDataFrame(rows, schema).where(predicate).select("__key").collect())


def classify_data_file(rel_path, depth, provider):
    """Partition key a data file belongs to, "" if it is outside partition dirs, None if not data."""
    segs = rel_path.strip("/").split("/")
    # Same rule Hive and Spark use when reading: anything under a _ or . name is not data.
    if any(s.startswith(("_", ".")) for s in segs):
        return None
    if provider == "iceberg":
        if segs[0] == "metadata":
            return None
        if segs[0] == "data":
            segs = segs[1:]
    dirs = segs[:-1]
    if depth and len(dirs) >= depth and all("=" in d for d in dirs[:depth]):
        return normalize_partition_spec("/".join(dirs[:depth]))
    return ""


# ── Metadata ────────────────────────────────────────────────────────────────────────


def parse_describe(rows):
    """Parse DESCRIBE TABLE EXTENDED rows given as (col_name, data_type) pairs."""
    info = {"partition_columns": [], "partition_transforms": []}
    section = "columns"
    for col_name, data_type in rows:
        key = (col_name or "").strip()
        value = (data_type or "").strip()
        if key.startswith("#"):
            if key == "# Partition Information":
                section = "hive_partitions"
            elif key == "# Partitioning":
                section = "v2_partitions"
            elif key != "# col_name":
                section = "details"
            continue
        if not key:
            continue
        if section == "hive_partitions":
            info["partition_columns"].append(key.lower())
        elif section == "v2_partitions":
            # Iceberg lists identity partitions as a bare column, others as e.g. "days(ts)".
            if re.match(r"^`?[\w.]+`?$", value):
                info["partition_columns"].append(value.strip("`").lower())
            else:
                info["partition_transforms"].append(value)
        elif section == "details":
            info.setdefault(key.lower(), value)
    return info


def describe_table(spark, name):
    rows = spark.sql("DESCRIBE TABLE EXTENDED {0}".format(name)).collect()
    return parse_describe([(r.col_name, r.data_type) for r in rows])


def resolve_tables(spark, catalog, db, includes, excludes):
    rows = spark.sql("SHOW TABLES IN {0}".format(table_ident(catalog, db))).collect()
    names = sorted(set(r["tableName"] for r in rows if not r.asDict().get("isTemporary")))
    return match_tables(names, includes, excludes)


# ── Metrics ─────────────────────────────────────────────────────────────────────────


def count_rows(spark, name, part_cols, predicate):
    where = " WHERE {0}".format(predicate) if predicate else ""
    if not part_cols:
        count = spark.sql("SELECT COUNT(*) AS c FROM {0}{1}".format(name, where)).first()["c"]
        return {"row_count": count, "partition_rows": None}
    # The grouped count costs the same as a plain COUNT(*) and also gives rows per partition.
    exprs = ["CAST({0} AS STRING)".format(quote_ident(c)) for c in part_cols]
    select = ", ".join("{0} AS p{1}".format(e, i) for i, e in enumerate(exprs))
    sql = "SELECT {0}, COUNT(*) AS c FROM {1}{2} GROUP BY {3}".format(select, name, where, ", ".join(exprs))
    partition_rows = {}
    for r in spark.sql(sql).collect():
        values = [r["p{0}".format(i)] for i in range(len(part_cols))]
        key = "/".join(
            "{0}={1}".format(c, HIVE_NULL if values[i] is None else values[i]) for i, c in enumerate(part_cols)
        )
        partition_rows[key] = r["c"]
    return {"row_count": sum(partition_rows.values()), "partition_rows": partition_rows}


def registered_partitions(spark, name, part_cols, predicate):
    specs = set(normalize_partition_spec(r[0]) for r in spark.sql("SHOW PARTITIONS {0}".format(name)).collect())
    return sorted(filter_partition_keys(spark, specs, part_cols, predicate))


def storage_metrics(spark, location, part_cols, provider, predicate):
    jvm = spark._jvm
    root = jvm.org.apache.hadoop.fs.Path(location)
    fs = root.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(root):
        return {"location_exists": False, "data_file_count": 0, "data_bytes": 0, "storage_partitions": {}}
    root_path = fs.makeQualified(root).toUri().getPath().rstrip("/")
    per_partition = {}
    loose_files = loose_bytes = 0
    files = fs.listFiles(root, True)
    while files.hasNext():
        status = files.next()
        key = classify_data_file(status.getPath().toUri().getPath()[len(root_path):], len(part_cols), provider)
        if key is None:
            continue
        if key:
            stats = per_partition.setdefault(key, [0, 0])
            stats[0] += 1
            stats[1] += status.getLen()
        else:
            loose_files += 1
            loose_bytes += status.getLen()
    if predicate:
        keep = filter_partition_keys(spark, per_partition, part_cols, predicate)
        per_partition = dict((k, v) for k, v in per_partition.items() if k in keep)
        loose_files = loose_bytes = 0
    return {
        "location_exists": True,
        "data_file_count": loose_files + sum(v[0] for v in per_partition.values()),
        "data_bytes": loose_bytes + sum(v[1] for v in per_partition.values()),
        "files_outside_partition_dirs": loose_files,
        "storage_partitions": per_partition,
    }


def collect_table(spark, pid, catalog, db, table, partition_filter, collect_storage=True):
    name = table_ident(catalog, db, table)
    rec = {"pair": pid, "database": db, "table": table, "status": "OK", "error": None, "notes": []}
    started = time.time()
    try:
        info = describe_table(spark, name)
        rec["table_type"] = (info.get("type") or "").upper()
        rec["provider"] = (info.get("provider") or "").lower()
        rec["location"] = info.get("location") or ""
        if "VIEW" in rec["table_type"]:
            rec["status"] = "VIEW"
            return rec

        part_cols = info["partition_columns"]
        transforms = info["partition_transforms"]
        if transforms:
            rec["notes"].append(
                "non-identity partitioning {0}: partition-level metrics cover identity columns only".format(transforms)
            )
        types = [(f.name.lower(), f.dataType.simpleString()) for f in spark.table(name).schema.fields]
        type_by_name = dict(types)
        rec["partition_columns"] = part_cols
        rec["schema"] = [{"name": n, "type": t} for n, t in types if n not in part_cols]
        rec["partition_schema"] = [{"name": c, "type": type_by_name.get(c, "string")} for c in part_cols]

        predicate = partition_filter if (part_cols or transforms) else ""
        if partition_filter and not predicate:
            rec["notes"].append("partition_filter ignored: table is not partitioned")
        rec["partition_filter"] = predicate

        rec.update(count_rows(spark, name, part_cols, predicate))

        rec["registered_partitions"] = None
        if part_cols and rec["provider"] != "iceberg":
            try:
                rec["registered_partitions"] = registered_partitions(spark, name, part_cols, predicate)
            except Exception as e:
                rec["notes"].append("SHOW PARTITIONS failed: {0}".format(short_error(e, 200)))

        if collect_storage and rec["location"] and predicate and not part_cols:
            rec["notes"].append("storage metrics skipped: partition_filter cannot be mapped to directories")
        elif collect_storage and rec["location"]:
            rec.update(storage_metrics(spark, rec["location"], part_cols, rec["provider"], predicate))
            if not rec["location_exists"]:
                rec["notes"].append("table location does not exist: {0}".format(rec["location"]))
            if rec["registered_partitions"] is not None:
                registered = set(rec["registered_partitions"])
                rec["unregistered_partitions"] = sorted(k for k in rec["storage_partitions"] if k not in registered)
    except Exception as e:
        rec["status"] = "ERROR"
        rec["error"] = short_error(e)
    finally:
        rec["collect_seconds"] = round(time.time() - started, 1)
    return rec


def describe_record(rec):
    label = "{0}.{1}".format(rec["database"], rec["table"])
    if rec["status"] != "OK":
        return "{0}: {1} {2}".format(label, rec["status"], rec.get("error") or "").rstrip()
    parts = ["rows={0:,}".format(rec["row_count"])]
    if rec.get("registered_partitions") is not None:
        parts.append("partitions={0:,}".format(len(rec["registered_partitions"])))
    if "data_file_count" in rec:
        parts.append("files={0:,} ({1})".format(rec["data_file_count"], fmt_bytes(rec["data_bytes"])))
    return "{0}: {1} [{2}s]".format(label, " ".join(parts), rec["collect_seconds"])


def collect_side(spark, cfg, emit=log):
    """Collect one side. cfg keys: side, label, catalog, run_label, database_pairs,
    collect_storage_metrics, parallelism."""
    # A partition_filter is pushed down to the Hive metastore, which rejects it on DATE
    # partition keys; this makes Spark list the partitions and filter them itself instead.
    # Spark versions without the setting accept and ignore the unknown key.
    spark.conf.set("spark.sql.hive.metastorePartitionPruningFallbackOnException", "true")

    side = cfg["side"]
    db_key = "source_db" if side == "source" else "dest_db"
    catalog = cfg.get("catalog") or ""
    records, jobs = [], []
    for pair in cfg["database_pairs"]:
        pid, db = pair_id(pair), pair[db_key]
        try:
            tables = resolve_tables(spark, catalog, db, pair.get("tables"), pair.get("exclude"))
        except Exception as e:
            records.append({"pair": pid, "database": db, "table": None, "status": "ERROR",
                            "error": "cannot list tables in {0}: {1}".format(db, short_error(e))})
            emit("[{0}] {1}: cannot list tables: {2}".format(side, db, short_error(e)))
            continue
        emit("[{0}] {1}: {2} table(s)".format(side, db, len(tables)))
        jobs += [(pid, db, t, pair.get("partition_filter") or "") for t in tables]

    collect_storage = cfg.get("collect_storage_metrics", True)

    def run(job):
        pid, db, table, pf = job
        return collect_table(spark, pid, catalog, db, table, pf, collect_storage)

    # contextlib.closing: Python 2's ThreadPool has no __enter__/__exit__.
    with contextlib.closing(ThreadPool(max(1, int(cfg.get("parallelism") or 8)))) as pool:
        for i, rec in enumerate(pool.imap_unordered(run, jobs), 1):
            records.append(rec)
            emit("  [{0} {1}/{2}] {3}".format(side, i, len(jobs), describe_record(rec)))

    errors = sum(1 for r in records if r["status"] == "ERROR")
    emit("[{0}] {1} record(s), {2} error(s)".format(side, len(records), errors))
    return {
        "side": side,
        "label": cfg.get("label") or side,
        "catalog": catalog,
        "run_label": cfg.get("run_label") or "",
        "collected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "spark_version": spark.version,
        "python_version": sys.version.split()[0],
        "database_pairs": cfg["database_pairs"],
        "tables": records,
    }


# ── Comparison ──────────────────────────────────────────────────────────────────────

SEVERITY = ["ERROR", "FAIL", "MISSING_ON_DEST", "WARN", "EXTRA_ON_DEST", "SKIPPED_VIEW", "PASS"]


def compare_columns(src, dest, kind):
    s = dict((c["name"], c["type"].lower()) for c in src or [])
    d = dict((c["name"], c["type"].lower()) for c in dest or [])
    diffs = []
    for name, stype in s.items():
        if name not in d:
            diffs.append("Missing {0} on dest: {1}".format(kind, name))
        elif d[name] != stype:
            diffs.append("Type mismatch for {0} {1}: {2} (source) vs {3} (dest)".format(kind, name, stype, d[name]))
    diffs += ["Extra {0} on dest: {1}".format(kind, name) for name in d if name not in s]
    return diffs


def partition_diffs(s, d):
    out = []
    src_rows, dest_rows = s.get("partition_rows"), d.get("partition_rows")
    if src_rows is not None and dest_rows is not None:
        for key in sorted(set(src_rows) | set(dest_rows)):
            sr, dr = src_rows.get(key), dest_rows.get(key)
            if sr != dr:
                issue = (
                    "ROWS_MISSING_ON_DEST" if dr is None
                    else "ROWS_EXTRA_ON_DEST" if sr is None
                    else "ROW_COUNT_MISMATCH"
                )
                out.append({"partition": key, "issue": issue, "source_rows": sr, "dest_rows": dr})

    src_files, dest_files = s.get("storage_partitions"), d.get("storage_partitions")
    if src_files is not None and dest_files is not None:
        for key in sorted(set(src_files) | set(dest_files)):
            sf, df = src_files.get(key), dest_files.get(key)
            if sf != df:
                issue = (
                    "FILES_MISSING_ON_DEST" if df is None
                    else "FILES_EXTRA_ON_DEST" if sf is None
                    else "FILE_STATS_MISMATCH"
                )
                out.append({
                    "partition": key, "issue": issue,
                    "source_files": sf[0] if sf else None, "dest_files": df[0] if df else None,
                    "source_bytes": sf[1] if sf else None, "dest_bytes": df[1] if df else None,
                })

    for side, rec in (("source", s), ("dest", d)):
        out += [
            {"partition": key, "issue": "NOT_REGISTERED_ON_" + side.upper()}
            for key in rec.get("unregistered_partitions") or []
        ]
    return out


def compare_table(s, d):
    """Returns (summary fields, partition diff rows) for a table present on at least one side."""
    if s and s["status"] == "VIEW":
        return {"status": "SKIPPED_VIEW", "issues": "source is a view"}, []
    if s is None:
        return {"status": "EXTRA_ON_DEST", "issues": "table exists only on dest"}, []
    if s["status"] == "ERROR":
        return {"status": "ERROR", "issues": "source: {0}".format(s["error"])}, []
    if d is None:
        return {"status": "MISSING_ON_DEST", "issues": "table not found on dest"}, []
    if d["status"] == "ERROR":
        return {"status": "ERROR", "issues": "dest: {0}".format(d["error"])}, []
    if d["status"] == "VIEW":
        return {"status": "FAIL", "issues": "source is a table but dest is a view"}, []

    fails, warns = [], []
    if s["row_count"] != d["row_count"]:
        fails.append("Row count mismatch: source={0:,}, dest={1:,}".format(s["row_count"], d["row_count"]))
    schema_diffs = compare_columns(s.get("schema"), d.get("schema"), "column")
    part_schema_diffs = compare_columns(s.get("partition_schema"), d.get("partition_schema"), "partition column")
    if schema_diffs:
        fails.append("Schema: " + "; ".join(schema_diffs[:3]) + (" ..." if len(schema_diffs) > 3 else ""))
    if part_schema_diffs:
        fails.append("Partition schema: " + "; ".join(part_schema_diffs[:3]))

    sp, dp = s.get("registered_partitions"), d.get("registered_partitions")
    if sp is not None and dp is not None and len(sp) != len(dp):
        warns.append("Partition count mismatch: source={0:,}, dest={1:,}".format(len(sp), len(dp)))
    if d.get("unregistered_partitions"):
        warns.append(
            "{0:,} partition dir(s) with data on dest are not registered: "
            "run MSCK REPAIR TABLE".format(len(d["unregistered_partitions"]))
        )
    if "data_file_count" in s and "data_file_count" in d:
        if s["data_file_count"] != d["data_file_count"]:
            warns.append("Data file count mismatch: source={0:,}, dest={1:,}".format(
                s["data_file_count"], d["data_file_count"]))
        if s["data_bytes"] != d["data_bytes"]:
            warns.append("Data size mismatch: source={0:,} B, dest={1:,} B".format(s["data_bytes"], d["data_bytes"]))

    summary = {
        "status": "FAIL" if fails else "WARN" if warns else "PASS",
        "issues": "; ".join(fails + warns),
        "schema_differences": "; ".join(schema_diffs),
        "partition_schema_differences": "; ".join(part_schema_diffs),
    }
    return summary, partition_diffs(s, d)


def compare_snapshots(src_snap, dest_snap):
    """Returns (summary rows, partition diff rows, warnings), summary sorted worst first."""
    warnings = []
    if src_snap["database_pairs"] != dest_snap["database_pairs"]:
        warnings.append("database_pairs differed between the two collections; tables may show as missing/extra")

    def index(snap):
        tables, db_errors = {}, {}
        for r in snap["tables"]:
            if r["table"] is None:
                db_errors[r["pair"]] = r["error"]
            else:
                tables[(r["pair"], r["table"].lower())] = r
        return tables, db_errors

    src, src_db_errors = index(src_snap)
    dest, dest_db_errors = index(dest_snap)

    summary_rows, diff_rows = [], []
    for pid, err in src_db_errors.items():
        summary_rows.append({"pair": pid, "table": "*", "status": "ERROR", "issues": "source: {0}".format(err)})
    for pid, err in dest_db_errors.items():
        summary_rows.append({"pair": pid, "table": "*", "status": "ERROR", "issues": "dest: {0}".format(err)})

    for key in sorted(set(src) | set(dest)):
        pid = key[0]
        s, d = src.get(key), dest.get(key)
        if d is None and pid in dest_db_errors:
            continue  # already reported once for the whole database
        result, diffs = compare_table(s, d)
        s_, d_ = s or {}, d or {}
        notes = ["source: " + n for n in s_.get("notes", [])] + ["dest: " + n for n in d_.get("notes", [])]
        sp, dp = s_.get("registered_partitions"), d_.get("registered_partitions")
        row = {
            "pair": pid,
            "table": (s or d)["table"],
            "source_rows": s_.get("row_count"),
            "dest_rows": d_.get("row_count"),
            "source_partitions": None if sp is None else len(sp),
            "dest_partitions": None if dp is None else len(dp),
            "source_files": s_.get("data_file_count"),
            "dest_files": d_.get("data_file_count"),
            "source_bytes": s_.get("data_bytes"),
            "dest_bytes": d_.get("data_bytes"),
            "partition_issues": len(diffs),
            "notes": "; ".join(notes),
            "source_location": s_.get("location"),
            "dest_location": d_.get("location"),
        }
        row.update(result)
        summary_rows.append(row)
        for diff in diffs:
            diff_row = {"pair": pid, "table": row["table"]}
            diff_row.update(diff)
            diff_rows.append(diff_row)

    summary_rows.sort(key=lambda r: (SEVERITY.index(r["status"]), r["pair"], r["table"]))
    return summary_rows, diff_rows, warnings


# ── CLI (edge node) ─────────────────────────────────────────────────────────────────


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Collect migration validation metrics into a JSON snapshot")
    ap.add_argument("--config", required=True, help="JSON: side, label, catalog, database_pairs, ...")
    ap.add_argument("--output", required=True, help="local path of the snapshot JSON to write")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    with open(args.config) as f:
        cfg = json.load(f)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("migration-validation-metrics").enableHiveSupport().getOrCreate()
    try:
        snapshot = collect_side(spark, cfg)
    finally:
        spark.stop()
    with open(args.output, "w") as f:
        json.dump(snapshot, f)
    log("{0} {1}".format(SNAPSHOT_MARKER, args.output))


if __name__ == "__main__":
    main()
