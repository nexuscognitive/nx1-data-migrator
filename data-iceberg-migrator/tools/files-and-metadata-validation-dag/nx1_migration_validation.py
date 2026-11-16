"""NX1 migration validation (DPC1.0 -> DPC2.0): files + metadata in one Airflow 3 DAG.

This single file replaces validation_report.py and migration_validation_metrics.ipynb. The only other
script in the flow is migration_metrics_collector_v2.py, which you run on the DPC1.0 edge node: it
collects the source metrics and uploads source_metrics.json to S3. Nothing in Airflow connects to the
edge node.

    Branch A, metadata (the notebook)
        wait_for_source_snapshot -> load_source_snapshot -> collect_dest_metrics -> compare_metadata
    Branch B, files (validation_report.py), one mapped task per bucket pair
        get_bucket_pairs -> validate_bucket_pair[...] -> summarize_s3_validation

The branches are independent, and the DAG run fails if either finds a problem. Reports are written
to S3 before the failure is raised.

Trigger parameters
  Required:  work_bucket, bucket_pairs
  Optional (leave empty to use the default): run_label, work_prefix, source_snapshot_key,
             max_snapshot_age_hours, collect_storage_metrics, parallelism, hadoop_user, fail_on_warn
  The defaults are in DEFAULTS below.

Requirements
  * migration_metrics_collector.py in the same folder as this file (V2 needs it on the edge node too).
    It is the shared measuring/comparison code, so source and destination are measured identically.
  * Python packages on the worker: boto3, pyspark (for collect_dest_metrics), and the worker's Spark
    configuration (spark-defaults / hive-site.xml) pointing at the DPC2.0 metastore and S3.
  * Airflow connections (S3: login = access key, password = secret key,
    extra = {"endpoint_url": "https://host:10443", "verify": false}; verify may be a CA bundle path):
        nx1_s3_source   source S3 tenant      (validation_report.py's SRC_*)
        nx1_s3_dest     destination S3 tenant (DST_*), also holds snapshots and reports
        nx1_dpc2_spark  OPTIONAL. extra = {"master": "yarn", "spark_conf": {...}, "hadoop_user": "..."}
                        added to the Spark session. Not needed if the worker's own Spark config is enough.
"""
from __future__ import annotations

import csv
import html
import io
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

from airflow.sdk import Param, dag, task

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)  # so `import migration_metrics_collector` works in tasks

S3_SRC_CONN = "nx1_s3_source"
S3_DST_CONN = "nx1_s3_dest"
SPARK_CONN = "nx1_dpc2_spark"

# DAG owner. Also used as HADOOP_USER_NAME for the DPC2.0 Spark session unless the hadoop_user param is set
# (not applied if the owner is left as "airflow"). Must be a user that can read the tables.
DAG_OWNER = "airflow"

# Defaults for the optional trigger parameters. An empty/null value in the trigger form means "use this".
DEFAULTS = {
    "run_label": "dpc1-to-dpc2",
    "work_prefix": "migration-validation",
    "source_snapshot_key": "",  # empty = <work_prefix>/<run_label>/source_metrics.json
    "max_snapshot_age_hours": 48,
    "collect_storage_metrics": True,
    "parallelism": 8,
    "hadoop_user": "",  # empty = the DAG owner
    "fail_on_warn": False,
}

GB = 1024 * 1024 * 1024
FAIL_STATUSES = {"ERROR", "FAIL", "MISSING_ON_DEST"}
REPORT_MAX_PARTITION_ROWS = 500  # HTML only; partition_diffs.csv has every row

SUMMARY_COLS = [
    "pair", "table", "status", "issues", "source_rows", "dest_rows", "source_partitions", "dest_partitions",
    "source_files", "dest_files", "source_bytes", "dest_bytes", "partition_issues", "schema_differences",
    "partition_schema_differences", "notes", "source_location", "dest_location",
]
DIFF_COLS = ["pair", "table", "partition", "issue", "source_rows", "dest_rows",
             "source_files", "dest_files", "source_bytes", "dest_bytes"]
STATUS_COLORS = {
    "PASS": "#d4edda", "WARN": "#fff3cd", "FAIL": "#f8d7da", "ERROR": "#f8d7da",
    "MISSING_ON_DEST": "#f8d7da", "EXTRA_ON_DEST": "#e2e3e5", "SKIPPED_VIEW": "#e2e3e5",
}


# ── Shared helpers ──────────────────────────────────────────────────────────────────


def _p(params, name):
    """Value of an optional parameter, falling back to DEFAULTS when it is empty/null."""
    value = params.get(name)
    return DEFAULTS[name] if value is None or value == "" else value


def _run_prefix(params):
    return "%s/%s" % (_p(params, "work_prefix").strip("/"), _p(params, "run_label"))


def _source_key(params):
    return _p(params, "source_snapshot_key") or "%s/source_metrics.json" % _run_prefix(params)


def _connection(conn_id):
    from airflow.sdk import BaseHook

    return BaseHook.get_connection(conn_id)


def _endpoint(conn_id):
    extra = _connection(conn_id).extra_dejson or {}
    return extra.get("endpoint_url") or _connection(conn_id).host


def _s3_client(conn_id):
    """boto3 client for an S3 tenant, configured from an Airflow connection."""
    import boto3
    import urllib3
    from botocore.config import Config

    conn = _connection(conn_id)
    extra = conn.extra_dejson or {}
    endpoint = extra.get("endpoint_url")
    if not endpoint and conn.host:
        if "://" in conn.host:
            endpoint = conn.host
        else:
            endpoint = "%s://%s%s" % (extra.get("scheme", "https"), conn.host, ":%s" % conn.port if conn.port else "")
    verify = extra.get("verify", True)
    if verify is False:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        verify=verify,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      retries={"max_attempts": 10, "mode": "standard"}),
    )


def _read_object(client, bucket, key):
    """Object body as text, or None if it does not exist."""
    from botocore.exceptions import ClientError

    try:
        return client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def _write_object(client, bucket, key, text, content_type="application/json"):
    client.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType=content_type)


def _utc_stamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ── Branch A helpers (notebook logic) ───────────────────────────────────────────────


def _parse_source_snapshot(text, where):
    """Same checks as the notebook's load_source_snapshot."""
    if text is None:
        raise FileNotFoundError("No source snapshot at %s: check the path and that the file was uploaded" % where)
    try:
        snap = json.loads(text)
    except ValueError as e:
        raise ValueError("The source snapshot %s is not valid JSON (partial upload?): %s" % (where, e))
    problems = []
    if not isinstance(snap, dict):
        problems.append("the top level is not a JSON object")
    else:
        if snap.get("side") != "source":
            problems.append("'side' is %r, expected 'source'" % snap.get("side"))
        if not isinstance(snap.get("tables"), list):
            problems.append("'tables' is missing or not a list")
        if not snap.get("database_pairs"):
            problems.append("'database_pairs' is missing or empty")
    if problems:
        raise ValueError("%s is not a collector snapshot: %s" % (where, "; ".join(problems)))
    return snap


def _dest_collector_config(src_snap, params):
    # Databases, tables and partition filters come from the source snapshot, so both sides cover the same set.
    return {
        "side": "dest",
        "label": "DPC2.0",
        "catalog": "",  # default (Hive) catalog; DPC2.0 tables are read as <db>.<table>
        "run_label": _p(params, "run_label"),
        "database_pairs": src_snap["database_pairs"],
        "collect_storage_metrics": bool(_p(params, "collect_storage_metrics")),
        "parallelism": int(_p(params, "parallelism")),
    }


def _spark_session(hadoop_user=""):
    from pyspark.sql import SparkSession

    extra = {}
    try:
        extra = _connection(SPARK_CONN).extra_dejson or {}
    except Exception as e:  # the connection is optional
        print("No usable Airflow connection %r (%s: %s); using the worker's own Spark configuration"
              % (SPARK_CONN, type(e).__name__, e))
    user = hadoop_user or extra.get("hadoop_user")
    if user:
        # Must be set before Spark starts its JVM. Same as `env HADOOP_USER_NAME=<user>` on the command line.
        os.environ["HADOOP_USER_NAME"] = user
        print("[dest] running Spark as HADOOP_USER_NAME=%s" % user)
    builder = SparkSession.builder.appName("nx1-migration-validation-dest").enableHiveSupport()
    if extra.get("master"):
        builder = builder.master(extra["master"])
    for k, v in (extra.get("spark_conf") or {}).items():
        builder = builder.config(k, v)
    return builder.getOrCreate()


def _csv_text(rows, cols):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore", restval="")
    writer.writeheader()
    for r in rows:
        writer.writerow({k: ("" if v is None else v) for k, v in r.items()})
    return buf.getvalue()


def _html_table(rows, cols, fmt_bytes):
    head = "".join("<th>%s</th>" % html.escape(c) for c in cols)
    body = []
    for r in rows:
        cells = []
        for c in cols:
            v = r.get(c)
            if v is None:
                text = ""
            elif c.endswith("_bytes"):
                text = fmt_bytes(int(v))
            else:
                text = str(v)
            style = ""
            if c == "status" and text in STATUS_COLORS:
                style = ' style="background:%s;font-weight:bold"' % STATUS_COLORS[text]
            cells.append("<td%s>%s</td>" % (style, html.escape(text)))
        body.append("<tr>%s</tr>" % "".join(cells))
    return "<table><tr>%s</tr>%s</table>" % (head, "".join(body))


def _render_report(run_label, src_snap, dest_snap, summary, diffs, counts, fmt_bytes):
    counts_html = " &nbsp; ".join("<b>%s</b>: %s" % (html.escape(k), v) for k, v in counts.most_common())
    note = ", first %d shown, see partition_diffs.csv" % REPORT_MAX_PARTITION_ROWS if len(diffs) > REPORT_MAX_PARTITION_ROWS else ""
    diff_html = _html_table(diffs[:REPORT_MAX_PARTITION_ROWS], DIFF_COLS, fmt_bytes) if diffs else "<p>None.</p>"
    return """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Migration validation: %(run)s</title>
<style>
 body { font-family: sans-serif; font-size: 13px; margin: 24px; }
 table { border-collapse: collapse; margin-bottom: 24px; }
 th, td { border: 1px solid #ccc; padding: 4px 8px; vertical-align: top; }
 th { background: #f2f2f2; }
</style></head><body>
<h1>Migration validation: %(src)s &rarr; %(dst)s</h1>
<p>Run label <b>%(run)s</b>. Source collected %(src_at)s, dest collected %(dst_at)s.</p>
<p>%(counts)s</p>
<h2>Tables</h2>
%(summary)s
<h2>Partition differences (%(ndiff)s%(note)s)</h2>
%(diffs)s
</body></html>""" % {
        "run": html.escape(run_label),
        "src": html.escape(src_snap["label"]), "dst": html.escape(dest_snap["label"]),
        "src_at": html.escape(src_snap["collected_at"]), "dst_at": html.escape(dest_snap["collected_at"]),
        "counts": counts_html, "summary": _html_table(summary, SUMMARY_COLS, fmt_bytes),
        "ndiff": "{:,}".format(len(diffs)), "note": note, "diffs": diff_html,
    }


# ── Branch B helpers (validation_report.py logic) ───────────────────────────────────


def _normalize_bucket_pairs(value):
    """Return a list of pair dicts.

    Accepts a list of dicts, a JSON string, or a list of JSON strings (the trigger form's array field
    hands over strings, not dicts).
    """
    pairs = []

    def add(v):
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return
            try:
                v = json.loads(v)
            except ValueError:
                raise ValueError("bucket_pairs entry is not valid JSON: %r" % v[:200])
        if isinstance(v, list):
            for x in v:
                add(x)
        elif isinstance(v, dict):
            pairs.append(v)
        else:
            raise ValueError("bucket_pairs entries must be objects, got %r" % (v,))

    add(value)
    if not pairs:
        raise ValueError("bucket_pairs is empty")
    allowed = {"src_bucket", "src_prefix", "dst_bucket", "dst_prefix"}
    for p in pairs:
        unknown = set(p) - allowed
        if unknown:
            raise ValueError("Unknown key(s) %s in bucket pair %r. Allowed keys: %s" % (sorted(unknown), p, sorted(allowed)))
        for k in ("src_bucket", "dst_bucket"):
            if not p.get(k):
                raise ValueError("Bucket pair is missing '%s': %r" % (k, p))
    return pairs


def _bucket_stats(client, bucket, prefix):
    count = 0
    total = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/"):
                continue
            count += 1
            total += obj["Size"]
    return count, total


def _format_table(rows):
    headers = ["Source", "Destination", "Src Objects", "Dst Objects", "Obj Diff",
               "Src Size (GB)", "Dst Size (GB)", "Size Diff (GB)", "Result"]
    body = []
    for r in rows:
        if r["result"] == "ERROR":
            body.append([r["source"], r["destination"], "-", "-", "-", "-", "-", "-", "ERROR"])
            continue
        body.append([
            r["source"], r["destination"],
            "{:,}".format(r["src_objects"]), "{:,}".format(r["dst_objects"]),
            "{:,}".format(r["src_objects"] - r["dst_objects"]),
            "{:,.3f}".format(r["src_bytes"] / GB), "{:,.3f}".format(r["dst_bytes"] / GB),
            "{:,.3f}".format((r["src_bytes"] - r["dst_bytes"]) / GB),
            r["result"],
        ])
    widths = [max(len(str(x[i])) for x in [headers] + body) for i in range(len(headers))]
    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    def line(cells):
        return "| " + " | ".join(str(c).ljust(w) for c, w in zip(cells, widths)) + " |"

    return "\n".join([sep, line(headers), sep] + [line(b) for b in body] + [sep])


# ── DAG ─────────────────────────────────────────────────────────────────────────────


@dag(
    dag_id="nx1_migration_validation",
    schedule=None,  # trigger manually, or from the migration DAG once the copy finishes
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": DAG_OWNER},
    tags=["nx1", "migration", "validation"],
    params={
        # ── Required ──
        "work_bucket": Param(
            type="string", minLength=1,
            description="REQUIRED. Bucket on the destination tenant for snapshots and reports",
        ),
        "bucket_pairs": Param(
            type="array",
            description='REQUIRED. Buckets to compare, e.g. [{"src_bucket":"a","src_prefix":"","dst_bucket":"b","dst_prefix":""}]. '
                        "Enter as single-line JSON.",
        ),
        # ── Optional: leave empty to use the default ──
        "run_label": Param(
            DEFAULTS["run_label"], type=["null", "string"],
            description="Optional. Groups snapshots/reports under work_prefix/run_label. Default: %s" % DEFAULTS["run_label"],
        ),
        "work_prefix": Param(
            DEFAULTS["work_prefix"], type=["null", "string"],
            description="Optional. No leading/trailing slash. Default: %s" % DEFAULTS["work_prefix"],
        ),
        "source_snapshot_key": Param(
            DEFAULTS["source_snapshot_key"], type=["null", "string"],
            description="Optional. Key of the snapshot V2 uploaded. Default: <work_prefix>/<run_label>/source_metrics.json",
        ),
        "max_snapshot_age_hours": Param(
            DEFAULTS["max_snapshot_age_hours"], type=["null", "integer"],
            description="Optional. Ignore a source snapshot older than this. Default: %s" % DEFAULTS["max_snapshot_age_hours"],
        ),
        "collect_storage_metrics": Param(
            DEFAULTS["collect_storage_metrics"], type=["null", "boolean"],
            description="Optional. Count files/bytes on storage. Default: %s" % DEFAULTS["collect_storage_metrics"],
        ),
        "parallelism": Param(
            DEFAULTS["parallelism"], type=["null", "integer"],
            description="Optional. Tables collected in parallel. Default: %s" % DEFAULTS["parallelism"],
        ),
        "hadoop_user": Param(
            DEFAULTS["hadoop_user"], type=["null", "string"],
            description="Optional. User for the DPC2.0 Spark session (HADOOP_USER_NAME). Default: the DAG owner (%s)" % DAG_OWNER,
        ),
        "fail_on_warn": Param(
            DEFAULTS["fail_on_warn"], type=["null", "boolean"],
            description="Optional. Also fail on WARN tables (file/partition count differences). Default: %s" % DEFAULTS["fail_on_warn"],
        ),
    },
)
def nx1_migration_validation():
    # ── Branch A: metadata validation ───────────────────────────────────────────────

    @task.sensor(poke_interval=300, timeout=12 * 3600, mode="reschedule")
    def wait_for_source_snapshot(params) -> bool:
        from botocore.exceptions import ClientError

        bucket = params["work_bucket"]
        key = _source_key(params)
        try:
            head = _s3_client(S3_DST_CONN).head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                print("Waiting: s3://%s/%s does not exist yet" % (bucket, key))
                return False
            raise
        age_h = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds() / 3600
        limit = _p(params, "max_snapshot_age_hours")
        if age_h > limit:
            print("Waiting: snapshot is %.1f h old (limit %s h); re-run migration_metrics_collector_v2.py on the edge node"
                  % (age_h, limit))
            return False
        print("Found s3://%s/%s (%.1f h old)" % (bucket, key, age_h))
        return True

    @task
    def load_source_snapshot(params) -> dict:
        """Read and check the source snapshot, and keep a copy as <run>/source_metrics.json."""
        bucket = params["work_bucket"]
        run_prefix = _run_prefix(params)
        src_key = _source_key(params)
        client = _s3_client(S3_DST_CONN)

        text = _read_object(client, bucket, src_key)
        snap = _parse_source_snapshot(text, "s3://%s/%s" % (bucket, src_key))
        print("Source snapshot: %s collected %s (Spark %s), %d record(s), %d database pair(s)" % (
            snap["label"], snap["collected_at"], snap.get("spark_version", "?"),
            len(snap["tables"]), len(snap["database_pairs"])))
        canonical = "%s/source_metrics.json" % run_prefix
        if src_key != canonical:
            _write_object(client, bucket, canonical, text)
            print("Copied to s3://%s/%s" % (bucket, canonical))
        return {"records": len(snap["tables"]), "database_pairs": len(snap["database_pairs"])}

    @task
    def collect_dest_metrics(params) -> dict:
        """Collect DPC2.0 for the same database pairs as the source snapshot -> dest_metrics.json."""
        import migration_metrics_collector as mmc
        from airflow.sdk import get_current_context

        bucket = params["work_bucket"]
        run_prefix = _run_prefix(params)
        client = _s3_client(S3_DST_CONN)
        src_snap = _parse_source_snapshot(
            _read_object(client, bucket, "%s/source_metrics.json" % run_prefix), "the copied source snapshot")

        owner = getattr(get_current_context().get("task"), "owner", None)
        hadoop_user = _p(params, "hadoop_user") or (owner if owner and owner != "airflow" else "")
        spark = _spark_session(hadoop_user)
        try:
            visible = sorted(str(r[0]) for r in spark.sql("SHOW DATABASES").collect())
            print("[dest] spark %s, catalogImplementation=%s, metastore uri=%s" % (
                spark.version, spark.conf.get("spark.sql.catalogImplementation", "?"),
                spark.sparkContext._jsc.hadoopConfiguration().get("hive.metastore.uris")))
            print("[dest] databases visible to this Spark session: %s" % visible)
            wanted = {p["dest_db"].lower() for p in src_snap["database_pairs"]}
            missing = sorted(wanted - {v.lower() for v in visible})
            if missing:
                raise RuntimeError(
                    "Database(s) %s are not visible to the Spark session on this worker (it sees %s). "
                    "The worker's Spark is not pointed at the DPC2.0 metastore; set hive.metastore.uris "
                    "(spark-defaults.conf/hive-site.xml on the worker, or spark_conf in the %s connection)."
                    % (missing, visible, SPARK_CONN))
            dest_cfg = _dest_collector_config(src_snap, params)
            print("[dest] collecting database pairs=%s" % [
                (p.get("dest_db"), p.get("tables")) for p in dest_cfg["database_pairs"]])
            dest_snap = mmc.collect_side(spark, dest_cfg, emit=print)
        finally:
            spark.stop()
        key = "%s/dest_metrics.json" % run_prefix
        _write_object(client, bucket, key, json.dumps(dest_snap))
        print("[dest] snapshot saved: s3://%s/%s" % (bucket, key))
        return {"records": len(dest_snap["tables"])}

    @task
    def compare_metadata(params) -> dict:
        """Compare the two snapshots and write summary.csv, partition_diffs.csv and report.html."""
        import migration_metrics_collector as mmc

        bucket = params["work_bucket"]
        run_prefix = _run_prefix(params)
        client = _s3_client(S3_DST_CONN)
        snaps = {}
        for side in ("source", "dest"):
            key = "%s/%s_metrics.json" % (run_prefix, side)
            text = _read_object(client, bucket, key)
            if text is None:
                raise FileNotFoundError("Missing snapshot s3://%s/%s" % (bucket, key))
            snaps[side] = json.loads(text)
        src_snap, dest_snap = snaps["source"], snaps["dest"]

        summary, diffs, warnings = mmc.compare_snapshots(src_snap, dest_snap)
        for w in warnings:
            print("WARNING: %s" % w)
        counts = Counter(r["status"] for r in summary)
        print("Status counts: %s" % dict(counts))

        report_prefix = "%s/report_%s" % (run_prefix, _utc_stamp())
        _write_object(client, bucket, report_prefix + "/summary.csv", _csv_text(summary, SUMMARY_COLS), "text/csv")
        _write_object(client, bucket, report_prefix + "/partition_diffs.csv", _csv_text(diffs, DIFF_COLS), "text/csv")
        _write_object(
            client, bucket, report_prefix + "/report.html",
            _render_report(_p(params, "run_label"), src_snap, dest_snap, summary, diffs, counts, mmc.fmt_bytes),
            "text/html")
        report = "s3://%s/%s/" % (bucket, report_prefix)
        print("Report: %s" % report)

        bad = set(FAIL_STATUSES) | ({"WARN"} if _p(params, "fail_on_warn") else set())
        failing = ["%s.%s [%s]" % (r["pair"], r["table"], r["status"]) for r in summary if r["status"] in bad]
        if failing:
            raise ValueError("Metadata validation failed for %d table(s), e.g. %s. Report: %s" % (len(failing), failing[:5], report))
        return {"counts": dict(counts), "report": report}

    # ── Branch B: S3 -> S3 object validation ────────────────────────────────────────

    @task
    def get_bucket_pairs(params) -> list:
        return _normalize_bucket_pairs(params["bucket_pairs"])

    @task
    def validate_bucket_pair(pair: dict) -> dict:
        """One bucket pair: object count and total size on both tenants. S3 errors become an ERROR row."""
        from botocore.exceptions import ClientError

        src_bucket = pair["src_bucket"]
        src_prefix = pair.get("src_prefix", "")
        dst_bucket = pair["dst_bucket"]
        dst_prefix = pair.get("dst_prefix", src_prefix)
        if (src_bucket, src_prefix) == (dst_bucket, dst_prefix) and _endpoint(S3_SRC_CONN) == _endpoint(S3_DST_CONN):
            print("WARNING: source and destination are the same bucket/prefix on the same endpoint, "
                  "so this comparison will always MATCH. Fine for a connectivity test, meaningless for validation.")
        row = {
            "source": "s3://%s/%s" % (src_bucket, src_prefix),
            "destination": "s3://%s/%s" % (dst_bucket, dst_prefix),
            "src_objects": None, "dst_objects": None, "src_bytes": None, "dst_bytes": None,
            "result": "ERROR", "error": None,
        }
        try:
            row["src_objects"], row["src_bytes"] = _bucket_stats(_s3_client(S3_SRC_CONN), src_bucket, src_prefix)
            row["dst_objects"], row["dst_bytes"] = _bucket_stats(_s3_client(S3_DST_CONN), dst_bucket, dst_prefix)
        except ClientError as e:
            row["error"] = str(e)[:500]
            print("[ERROR] %s -> %s: %s" % (row["source"], row["destination"], e))
            return row
        matched = row["src_objects"] == row["dst_objects"] and row["src_bytes"] == row["dst_bytes"]
        row["result"] = "MATCH" if matched else "MISMATCH"
        return row

    @task
    def summarize_s3_validation(results, params) -> dict:
        rows = list(results)
        print(_format_table(rows))
        key = "%s/s3_object_validation_%s.json" % (_run_prefix(params), _utc_stamp())
        _write_object(_s3_client(S3_DST_CONN), params["work_bucket"], key, json.dumps(rows, indent=2))
        print("Report: s3://%s/%s" % (params["work_bucket"], key))
        bad = ["%s -> %s (%s)" % (r["source"], r["destination"], r["result"]) for r in rows if r["result"] != "MATCH"]
        if bad or not rows:
            raise ValueError("S3 object validation failed: %s" % (bad or "no bucket pairs configured"))
        return {"pairs": len(rows)}

    # ── Wiring ──────────────────────────────────────────────────────────────────────

    wait_for_source_snapshot() >> load_source_snapshot() >> collect_dest_metrics() >> compare_metadata()

    summarize_s3_validation(validate_bucket_pair.expand(pair=get_bucket_pairs()))


nx1_migration_validation()
