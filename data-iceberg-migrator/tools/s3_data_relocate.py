"""
Move bucket/<table>/ -> bucket/<dest_database>/<table>/ (same bucket, server-side copy, then delete source).

xlsx columns: database | table | dest_database | bucket | endpoint (optional) | partition_filter (optional)
partition_filter (optional): prefix under the table, e.g. "dt=2026-01-01/"

Credentials: none passed in code. boto3 uses whatever is already configured in the environment
(AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars, ~/.aws/credentials, or AWS_PROFILE).

Usage:
    python s3_data_relocate.py tables.xlsx [--dry-run]
"""
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config
from openpyxl import load_workbook


def main():
    dry_run = "--dry-run" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        sys.exit(__doc__)
    workers = 16
    ca_bundle = os.environ.get("CA_BUNDLE") or True

    def move(s3, bucket, src_key, dst_key):
        s3.copy({"Bucket": bucket, "Key": src_key}, bucket, dst_key)  # handles >5 GB objects
        src_size = s3.head_object(Bucket=bucket, Key=src_key)["ContentLength"]
        if s3.head_object(Bucket=bucket, Key=dst_key)["ContentLength"] != src_size:
            raise RuntimeError("size mismatch, source not deleted")
        s3.delete_object(Bucket=bucket, Key=src_key)

    rows = load_workbook(args[0]).active.iter_rows(values_only=True)
    header = [str(h).strip().lower() if h is not None else "" for h in next(rows)]
    need = ["table", "dest_database", "bucket"]  # endpoint and partition_filter are optional
    if any(c not in header for c in need):
        sys.exit(f"xlsx header must contain {need}; found {header}")

    for n, row in enumerate(rows, start=2):
        r = dict(zip(header, row))
        table, dest_db, bucket = (r[c] for c in need)
        endpoint = str(r.get("endpoint") or "").strip() or None  # empty -> AWS_ENDPOINT_URL env var / boto3 default
        part = r.get("partition_filter")
        if not any(row):
            continue
        if not (table and dest_db and bucket):
            print(f"row {n}: SKIPPED, table/dest_database/bucket must be filled: {r}")
            continue
        if "*" in str(table):
            print(f"row {n}: SKIPPED, wildcards are not supported in table ('{table}'); list each table")
            continue
        bucket = re.sub(r"^s3[an]?://", "", str(bucket).strip(), flags=re.I).strip("/")  # s3a://b, s3://b, b
        if "/" in bucket:
            print(f"row {n}: SKIPPED, bucket must be a bucket name only, got '{bucket}'")
            continue
        table, dest_db, part = str(table).strip("/"), str(dest_db).strip("/"), (part or "").lstrip("/")
        src_prefix, dst_prefix = f"{table}/", f"{dest_db}/{table}/"
        s3 = boto3.client(
            "s3", endpoint_url=endpoint, verify=ca_bundle,
            # S3_ACCESS_KEY / S3_SECRET_KEY if set, otherwise boto3's default lookup (AWS_* env vars, ~/.aws)
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 10, "mode": "standard"},
                          max_pool_connections=workers * 2),
        )
        print(f"== {bucket}/{src_prefix}{part} -> {bucket}/{dst_prefix}{part}")
        keys = [o["Key"] for p in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=src_prefix + part)
                for o in p.get("Contents", [])]
        failed = 0
        with ThreadPoolExecutor(workers) as pool:
            futs = {}
            for k in keys:
                dst = dst_prefix + k[len(src_prefix):]
                if dry_run:
                    print("DRY", k, "->", dst)
                else:
                    futs[pool.submit(move, s3, bucket, k, dst)] = k
            for f, k in futs.items():
                try:
                    f.result()
                except Exception as e:
                    failed += 1
                    print("FAILED", k, e)
        print(f"   {len(keys)} object(s), {failed} failed")


if __name__ == "__main__":
    main()
