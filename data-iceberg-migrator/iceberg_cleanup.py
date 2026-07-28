"""
Iceberg cleanup remediation script (standalone — NOT an Airflow DAG).

Drops Iceberg tables (metadata-only, **NO PURGE** — the underlying data files are
preserved) and removes their ``metadata/`` folder, returning the S3 location to a
plain Parquet/ORC directory that DAG 1 can recreate as a Hive external table.

WHEN TO USE
-----------
If DAG 2 was run with **in-place** migration, the destination table (same name as
the DAG 1 Hive table) is now an Iceberg table. DAG 1 with
``migration_recreate_tables=true`` deliberately REFUSES to drop an Iceberg table
(it would destroy the in-place-migrated table). Run this script first to cleanly
remove the Iceberg layer, then re-run DAG 1 and DAG 2.

Remediation sequence::

    iceberg_cleanup.py --apply   ->   DAG 1 (migration_recreate_tables=true)   ->   DAG 2 (in-place)

Snapshot migrations (DAG 2 with inplace=false) do NOT need this: the snapshot
Iceberg table lives in a separate ``{db}_iceberg`` database, so the DAG 1 Hive
table keeps its name and DAG 1 recreate is already safe.

SAFETY
------
- Only tables that are actually Iceberg are touched; Hive tables are skipped.
- ``DROP TABLE`` is metadata-only (**no PURGE**) — data files are kept.
- Only the ``metadata/`` folder is deleted; partition data directories are left
  intact. **The data referenced by the table remains on S3.**
- Defaults to a DRY RUN; pass ``--apply`` to make changes.

USAGE
-----
Run with the same Spark configuration the DAGs use (Hive metastore + S3A creds),
e.g. via ``spark-submit`` on the cluster::

    spark-submit iceberg_cleanup.py --tables db1.tbl_a,db1.tbl_b            # dry run
    spark-submit iceberg_cleanup.py --tables db1.tbl_a,db1.tbl_b --apply    # execute

    # convenience: prefix a bare table list with a single database
    spark-submit iceberg_cleanup.py --database db1 --tables tbl_a,tbl_b --apply
"""

import argparse
import logging
import sys

logger = logging.getLogger("iceberg_cleanup")


def is_iceberg_table(spark, full_name: str) -> bool:
    """True if ``full_name`` (``db.table``) is an Iceberg table.

    Detected via ``DESCRIBE FORMATTED`` (Iceberg sets ``Provider = iceberg``).
    Returns False when the table cannot be described (missing / not accessible).
    """
    try:
        rows = spark.sql(f"DESCRIBE FORMATTED {full_name}").collect()
    except Exception:
        return False
    for r in rows:
        col = (r.col_name or "").strip().lower()
        val = (r.data_type or "").strip().lower()
        if col == "provider" and "iceberg" in val:
            return True
        if col.startswith("table properties") and "iceberg" in val:
            return True
    return False


def table_exists(spark, full_name: str) -> bool:
    """True if ``full_name`` exists in the metastore."""
    try:
        spark.sql(f"DESCRIBE {full_name}")
        return True
    except Exception:
        return False


def get_table_location(spark, full_name: str) -> str | None:
    """Return the table's storage location from ``DESCRIBE FORMATTED``, or None."""
    try:
        rows = spark.sql(f"DESCRIBE FORMATTED {full_name}").collect()
    except Exception:
        return None
    for r in rows:
        col = (r.col_name or "").strip().rstrip(":").lower()
        if col == "location":
            loc = (r.data_type or "").strip()
            return loc or None
    return None


def delete_metadata_folder(spark, location: str) -> bool:
    """Delete the ``metadata/`` folder under ``location`` via Hadoop FileSystem.

    Only the ``metadata/`` folder is removed — data files elsewhere under the
    location are untouched. Returns True if the folder existed and was deleted,
    False if there was no ``metadata/`` folder to remove.
    """
    metadata_path = location.rstrip("/") + "/metadata"
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(location), spark._jsc.hadoopConfiguration()
    )
    path = jvm.org.apache.hadoop.fs.Path(metadata_path)
    if not fs.exists(path):
        logger.info(f"[cleanup] No metadata/ folder at {metadata_path} — nothing to delete")
        return False
    fs.delete(path, True)  # recursive
    logger.info(f"[cleanup] Deleted metadata folder: {metadata_path}")
    return True


def cleanup_table(spark, full_name: str, dry_run: bool = True) -> dict:
    """Clean up a single Iceberg table: drop (no purge) + remove metadata folder.

    Returns a result dict with a ``status`` of:
      - ``SKIPPED_NOT_FOUND``    — table does not exist
      - ``SKIPPED_NOT_ICEBERG``  — table exists but is not Iceberg (left untouched)
      - ``DRY_RUN``              — would be cleaned (no changes made)
      - ``CLEANED``              — dropped (no purge) and metadata folder removed
      - ``FAILED``               — an error occurred
    """
    result = {"table": full_name, "status": None, "location": None, "error": None}

    if not table_exists(spark, full_name):
        result["status"] = "SKIPPED_NOT_FOUND"
        logger.warning(f"[cleanup] {full_name}: not found — skipping")
        return result

    if not is_iceberg_table(spark, full_name):
        result["status"] = "SKIPPED_NOT_ICEBERG"
        logger.info(f"[cleanup] {full_name}: not an Iceberg table — skipping (nothing to clean)")
        return result

    location = get_table_location(spark, full_name)
    result["location"] = location

    if dry_run:
        result["status"] = "DRY_RUN"
        logger.info(
            f"[cleanup] DRY RUN — would DROP (no purge) {full_name} and delete "
            f"metadata folder at {location or '<unknown location>'}"
        )
        return result

    try:
        # Metadata-only drop; NO PURGE — data files are preserved.
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        logger.info(f"[cleanup] Dropped Iceberg table (no purge): {full_name}")

        if location:
            delete_metadata_folder(spark, location)
        else:
            logger.warning(
                f"[cleanup] {full_name}: could not resolve location — table dropped "
                f"but metadata folder (if any) was not deleted; remove it manually"
            )
        result["status"] = "CLEANED"
    except Exception as e:  # noqa: BLE001 - report per-table, continue with the rest
        result["status"] = "FAILED"
        result["error"] = str(e)[:2000]
        logger.error(f"[cleanup] {full_name}: FAILED — {result['error']}")

    return result


def cleanup_tables(spark, tables, dry_run: bool = True) -> list:
    """Run :func:`cleanup_table` for each fully-qualified table name."""
    results = []
    for full_name in tables:
        results.append(cleanup_table(spark, full_name, dry_run=dry_run))
    return results


def resolve_tables(database: str | None, tables_arg: str) -> list:
    """Turn ``--database`` + ``--tables`` into a list of ``db.table`` names."""
    names = [t.strip() for t in (tables_arg or "").split(",") if t.strip()]
    resolved = []
    for name in names:
        if "." in name:
            resolved.append(name)
        elif database:
            resolved.append(f"{database}.{name}")
        else:
            raise ValueError(
                f"Table '{name}' is not fully qualified (db.table) and no "
                f"--database was provided"
            )
    return resolved


def build_spark(app_name: str = "iceberg_cleanup"):
    """Create a Hive-enabled SparkSession (inherits the cluster's Spark config)."""
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description=(
            "Drop Iceberg tables (metadata only, no purge) and remove their "
            "metadata/ folder so DAG 1 can recreate them as Hive tables. "
            "Data files are preserved."
        )
    )
    parser.add_argument(
        "--tables",
        required=True,
        help="Comma-separated table names (db.table, or bare names with --database)",
    )
    parser.add_argument(
        "--database",
        default=None,
        help="Optional database to qualify bare table names in --tables",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually drop tables and delete metadata folders (default: dry run)",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = parse_args(sys.argv[1:] if argv is None else argv)
    dry_run = not args.apply

    tables = resolve_tables(args.database, args.tables)
    if not tables:
        logger.error("No tables to process")
        return 2

    logger.info(
        f"iceberg_cleanup starting | mode={'DRY RUN' if dry_run else 'APPLY'} | "
        f"tables={tables}"
    )
    if dry_run:
        logger.info("DRY RUN — no changes will be made. Re-run with --apply to execute.")

    spark = build_spark()
    try:
        results = cleanup_tables(spark, tables, dry_run=dry_run)
    finally:
        spark.stop()

    logger.info("=" * 60)
    logger.info("iceberg_cleanup summary")
    for r in results:
        logger.info(f"  {r['table']}: {r['status']}" + (f" — {r['error']}" if r.get("error") else ""))
    logger.info("=" * 60)

    failed = [r for r in results if r["status"] == "FAILED"]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
