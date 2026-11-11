# -*- coding: utf-8 -*-
"""Source-side collector, V2: collect the snapshot AND copy it to S3.

migration_metrics_collector.py is left unchanged. This script reuses its collection code, so the
JSON it produces is identical, and adds one step at the end: copying the snapshot to an S3 location
that the DPC2.0 JupyterHub can also read. Use it on the DPC1.0 edge node, where SSH from JupyterHub
is not available:

    spark-submit --master 'local[*]' migration_metrics_collector_v2.py \\
        --config source_config.json \\
        --output source_metrics.json \\
        --s3-output s3a://<bucket>/<prefix>/dpc1-to-dpc2/source_metrics.json \\
        --upload-method aws --s3-endpoint https://<s3 endpoint>

Upload methods (--upload-method):
    hadoop  `hadoop fs -put` (default). Uses the edge node's Hadoop S3A settings.
    aws     `aws s3 cp` (AWS CLI). Needs the `aws` command on the edge node.
    auto    try hadoop; if that fails, try aws.

Credentials are never passed as command-line arguments (they would show up in `ps`). Export them
before running; both methods read them:

    export AWS_ACCESS_KEY_ID=...
    export AWS_SECRET_ACCESS_KEY=...
    export AWS_SESSION_TOKEN=...      # only if you have one

Hadoop's S3A credential chain includes these variables, so they also fix "No AWS Credentials
provided" errors from `hadoop fs`. Extra `hadoop fs` options (e.g. a bucket-specific endpoint) can be
put in MIGRATION_HADOOP_S3_OPTS. For the aws method, a custom CA bundle can be set with AWS_CA_BUNDLE
and a named profile with --aws-profile.

The local file given by --output is always written first and kept. If every upload method fails, the
script exits with code 2 and prints the manual fallback: upload that local file to S3 yourself and
point the validation notebook at it. The notebook accepts the file whichever script produced it.

Like the original, this file stays Python 2.7 and Spark 2.x compatible (no f-strings). The original
collector must sit in the same folder.

Example for: source_config.json
{
  "side": "source",
  "label": "DPC1.0",
  "catalog": "",
  "run_label": "dpc1-to-dpc2",
  "database_pairs": [
    {
      "source_db": "YOUR_SOURCE_DB",
      "dest_db": "YOUR_DEST_DB",
      "tables": ["table_a", "table_b"],
      "exclude": [],
      "partition_filter": ""
    }
  ],
  "collect_storage_metrics": true,
  "parallelism": 8
}
"""
from __future__ import print_function

import argparse
import json
import os
import re
import shlex
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import migration_metrics_collector as mmc  # noqa: E402

S3_COPIED_MARKER = "SNAPSHOT_COPIED_TO_S3"
HADOOP_OPTS_ENV = "MIGRATION_HADOOP_S3_OPTS"
EXIT_COPY_FAILED = 2
METHODS = ("hadoop", "aws", "auto")


def normalize_s3_uri(uri, local_output):
    """s3:// and s3n:// become s3a://; a trailing '/' means a folder, so the local file name is added."""
    uri = re.sub(r"^s3n?://", "s3a://", (uri or "").strip())
    if not uri.startswith("s3a://") or len(uri) <= len("s3a://"):
        raise ValueError("--s3-output must be an s3a:// (or s3://) URI, got: {0!r}".format(uri))
    if uri.endswith("/"):
        uri += os.path.basename(local_output)
    return uri


def split_bucket_key(s3_uri):
    """('bucket', 'key/with/slashes') from an s3a:// URI; the key must name an object."""
    bucket, _, key = s3_uri[len("s3a://"):].partition("/")
    if not bucket or not key:
        raise ValueError("--s3-output must include a bucket and an object key: {0!r}".format(s3_uri))
    return bucket, key


def run_command(cmd):
    """Run a command; returns (exit code, combined output). Credentials come from the environment."""
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except OSError as e:
        return 127, "could not run {0!r}: {1}".format(cmd[0], e)
    output = proc.communicate()[0]
    if not isinstance(output, str):
        output = output.decode("utf-8", "replace")
    return proc.returncode, output


def run_steps(tool, steps):
    for name, cmd in steps:
        mmc.log("[s3] {0} {1} ...".format(tool, name))
        rc, output = run_command(cmd)
        if rc != 0:
            raise RuntimeError("{0} {1} failed (exit {2}): {3}".format(tool, name, rc, output.strip()[-1000:]))


# ── hadoop ──────────────────────────────────────────────────────────────────────────


def hadoop_s3_opts(endpoint=None):
    """Extra `hadoop fs` options. Never printed: they may contain connection details."""
    opts = shlex.split(os.environ.get(HADOOP_OPTS_ENV, ""))
    if endpoint:
        opts.append("-Dfs.s3a.endpoint={0}".format(endpoint))
    return opts


def copy_with_hadoop(local_path, s3_uri, endpoint=None):
    """mkdir -p, put -f, then check the object exists, the same pattern the DAG uses for checkpoints."""
    base = ["hadoop", "fs"] + hadoop_s3_opts(endpoint)
    run_steps("hadoop fs", [
        ("mkdir", base + ["-mkdir", "-p", s3_uri.rsplit("/", 1)[0]]),
        ("put", base + ["-put", "-f", local_path, s3_uri]),
        ("verify", base + ["-test", "-e", s3_uri]),
    ])


# ── aws cli ─────────────────────────────────────────────────────────────────────────


def copy_with_aws(local_path, s3_uri, endpoint=None, profile=None):
    """aws s3 cp, then head-object to check it arrived. S3 has no folders, so no mkdir step."""
    bucket, key = split_bucket_key(s3_uri)
    extra = []
    if endpoint:
        extra += ["--endpoint-url", endpoint]
    if profile:
        extra += ["--profile", profile]
    if not (os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY")) and not profile:
        mmc.log("[s3] note: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are not set; "
                "relying on the aws CLI's own credential chain")
    run_steps("aws", [
        ("cp", ["aws", "s3", "cp", local_path, "s3://{0}/{1}".format(bucket, key)] + extra),
        ("verify", ["aws", "s3api", "head-object", "--bucket", bucket, "--key", key] + extra),
    ])


# ── dispatch ────────────────────────────────────────────────────────────────────────


def upload(local_path, s3_uri, method, endpoint=None, profile=None):
    """Copy with the chosen method. Returns the method that worked; raises if none did."""
    order = ["hadoop", "aws"] if method == "auto" else [method]
    errors = []
    for m in order:
        try:
            if m == "hadoop":
                copy_with_hadoop(local_path, s3_uri, endpoint)
            else:
                copy_with_aws(local_path, s3_uri, endpoint, profile)
            return m
        except Exception as e:
            errors.append("{0}: {1}".format(m, e))
            mmc.log("[s3] {0} upload failed".format(m))
    raise RuntimeError("\n".join(errors))


def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description="Collect source migration validation metrics and copy the JSON snapshot to S3"
    )
    ap.add_argument("--config", required=True, help="JSON: side, label, catalog, database_pairs, ...")
    ap.add_argument("--output", required=True, help="local path of the snapshot JSON (always written, kept)")
    ap.add_argument("--s3-output", required=True,
                    help="s3a:// URI to copy the snapshot to (a trailing / means a folder)")
    ap.add_argument("--upload-method", choices=METHODS, default="hadoop",
                    help="hadoop (default), aws, or auto (hadoop, then aws)")
    ap.add_argument("--s3-endpoint", default=None,
                    help="S3 endpoint URL, used by both methods (no credentials here)")
    ap.add_argument("--aws-profile", default=None, help="named AWS CLI profile (aws method only)")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    s3_uri = normalize_s3_uri(args.s3_output, args.output)
    if args.upload_method != "hadoop":
        split_bucket_key(s3_uri)  # fail before the long Spark run, not after
    with open(args.config) as f:
        cfg = json.load(f)
    if cfg.get("side") != "source":
        sys.exit("V2 is the source-side collector: the config must have \"side\": \"source\"")

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("migration-validation-metrics-v2").enableHiveSupport().getOrCreate()
    try:
        snapshot = mmc.collect_side(spark, cfg)
    finally:
        spark.stop()
    with open(args.output, "w") as f:
        json.dump(snapshot, f)
    mmc.log("{0} {1}".format(mmc.SNAPSHOT_MARKER, args.output))

    try:
        used = upload(args.output, s3_uri, args.upload_method, args.s3_endpoint, args.aws_profile)
    except Exception as e:
        mmc.log("S3 COPY FAILED:\n{0}".format(e))
        mmc.log("The snapshot is intact at {0}. Fallback: upload it to S3 yourself, then give that "
                "location to the validation notebook (SOURCE_SNAPSHOT_PATH).".format(args.output))
        sys.exit(EXIT_COPY_FAILED)
    mmc.log("{0} {1} (via {2})".format(S3_COPIED_MARKER, s3_uri, used))


if __name__ == "__main__":
    main()
