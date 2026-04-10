import json
import sys
import fnmatch
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("table_discovery_{run_id}_{src_db}_{dest_db}_{dest_bucket_slug}") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

src_db = "{src_db}"
dest_db = "{dest_db}"
dest_bucket = "{dest_bucket}"

tokens = json.loads('{tokens_json_escaped}')

def resolve_tokens(spark, db, tokens):
    resolved = []
    seen = set()

    for tok in tokens:
        if tok == '*':
            rows = spark.sql("SHOW TABLES IN {0}".format(db)).collect()
            for r in rows:
                t = r.tableName
                if t not in seen:
                    seen.add(t)
                    resolved.append(t)
        elif '*' in tok:
            rows = spark.sql(
                "SHOW TABLES IN {0} LIKE '{1}'".format(db, tok)
            ).collect()
            for r in rows:
                t = r.tableName
                if t not in seen:
                    seen.add(t)
                    resolved.append(t)
        else:
            if tok not in seen:
                seen.add(tok)
                resolved.append(tok)

    return resolved

table_list = resolve_tokens(spark, src_db, tokens)

metadata = []

for tbl in table_list:
    loc = None
    table_type = "UNKNOWN"
    input_format = None
    serde_properties = {}
    in_serde_section = False
    try:
        desc_df = spark.sql(
            "DESCRIBE FORMATTED {0}.{1}".format(src_db, tbl)
        )
        desc_rows = desc_df.collect()

        for row in desc_rows:
            col_name = (row.col_name or "").strip().rstrip(":").lower()
            data_type = (row.data_type or "").strip()

            if col_name == "location":
                loc = data_type
            elif col_name in ("type", "table type"):
                table_type = data_type.replace("_TABLE", "")
            elif col_name == "inputformat":
                input_format = data_type
            elif col_name in (
                "storage properties",
                "storage desc params",
                "serde library",
                "serialization lib",
                "parameters",
            ):
                raw = data_type.strip("[]")
                for pair in raw.split(", "):
                    if "=" in pair:
                        k, _, v = pair.partition("=")
                        k = k.strip()
                        v = v.strip()
                        if k in ("field.delim", "escape.delim", "null.format", "field delimiter"):
                            serde_properties[k] = v

        source_total_size = 0
        source_file_count = 0
        if loc:
            try:
                from py4j.java_gateway import java_import
                java_import(spark._jvm, "org.apache.hadoop.fs.*")

                fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark._jvm.java.net.URI(loc),
                    spark._jsc.hadoopConfiguration()
                )
                path = spark._jvm.org.apache.hadoop.fs.Path(loc)

                if fs.exists(path):
                    content_summary = fs.getContentSummary(path)
                    source_total_size = int(content_summary.getLength())
                    source_file_count = int(content_summary.getFileCount())
            except Exception:
                pass

        file_format = "PARQUET"
        if input_format:
            if "parquet" in input_format.lower():
                file_format = "PARQUET"
            elif "orc" in input_format.lower():
                file_format = "ORC"
            elif "avro" in input_format.lower():
                file_format = "AVRO"
            elif "text" in input_format.lower():
                file_format = "TEXTFILE"

        row_count = 0
        try:
            row_count = spark.sql(
                "SELECT COUNT(*) as c FROM {0}.{1}".format(src_db, tbl)
            ).collect()[0].c
        except:
            pass

        partition_cols_from_describe = []
        in_partition_section = False
        for row in desc_rows:
            col_name = (row.col_name or "").strip()
            if col_name == "# Partition Information":
                in_partition_section = True
                continue
            if in_partition_section and col_name == "# col_name":
                continue
            if in_partition_section and col_name.startswith("#"):
                break
            if in_partition_section and col_name:
                partition_cols_from_describe.append(col_name)

        partition_definition = len(partition_cols_from_describe) > 0
        partition_columns = ",".join(partition_cols_from_describe)

        partitions = []
        registered_partition_count = 0
        try:
            parts_df = spark.sql(
                "SHOW PARTITIONS {0}.{1}".format(src_db, tbl)
            )
            partitions = [row.partition for row in parts_df.collect()]
            registered_partition_count = len(partitions)
        except:
            pass

        is_partitioned = partition_definition
        unregistered_partitions = partition_definition and registered_partition_count == 0

        schema_df = spark.sql(
            "DESCRIBE {0}.{1}".format(src_db, tbl)
        )
        schema = []
        for row in schema_df.collect():
            col_name = row.col_name.strip() if row.col_name else ""
            data_type = row.data_type.strip() if row.data_type else ""

            if col_name.startswith("#") or col_name == "" or col_name == "col_name":
                break

            schema.append({"name": col_name, "type": data_type})

        s3_location = "{0}/{1}/{2}".format(dest_bucket, dest_db, tbl)

        metadata.append({
            "source_database": src_db,
            "source_table": tbl,
            "dest_database": dest_db,
            "dest_bucket": dest_bucket,
            "source_location": loc or "",
            "s3_location": s3_location,
            "file_format": file_format,
            "schema": schema,
            "partitions": partitions,
            "partition_columns": partition_columns,
            "partition_count": len(partitions),
            "row_count": row_count,
            "is_partitioned": is_partitioned,
            "unregistered_partitions": unregistered_partitions,
            "table_type": table_type,
            "source_total_size_bytes": source_total_size,
            "serde_properties": serde_properties,
            "source_file_count": source_file_count
        })

    except Exception as e:
        metadata.append({
            "source_database": src_db,
            "source_table": tbl,
            "dest_database": dest_db,
            "dest_bucket": dest_bucket,
            "source_location": "",
            "s3_location": dest_bucket + "/" + dest_db + "/" + tbl,
            "file_format": "PARQUET",
            "schema": [],
            "partitions": [],
            "partition_columns": "",
            "partition_count": 0,
            "row_count": 0,
            "is_partitioned": False,
            "unregistered_partitions": False,
            "table_type": "UNKNOWN",
            "source_total_size_bytes": 0,
            "source_file_count": 0,
            "serde_properties": serde_properties,
            "error": str(e)[:500]
        })

print ("===JSON_START===")
sys.stdout.flush()
print (json.dumps(metadata))
sys.stdout.flush()
print ("===JSON_END===")
sys.stdout.flush()

spark.stop()
