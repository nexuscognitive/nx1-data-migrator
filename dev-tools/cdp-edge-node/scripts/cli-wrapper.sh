#!/bin/bash
# /usr/bin entry point for the Hadoop, Hive and Spark CLIs, as CDP's parcel
# alternatives provide: each command sets its own environment, so it works in any
# SSH session, with or without a login profile. Installed once and symlinked
# under each command name; a symlink straight to /opt/... would break Hadoop's
# launcher, which finds its home folder from its own path.
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
export HIVE_HOME=/opt/hive
export HIVE_CONF_DIR="${HIVE_CONF_DIR:-/etc/hive/conf}"
export SPARK_HOME=/opt/spark
export SPARK_CONF_DIR="${SPARK_CONF_DIR:-/etc/spark/conf}"

cmd="$(basename "$0")"
case "$cmd" in
    hadoop|hdfs|mapred)
        exec "$HADOOP_HOME/bin/$cmd" "$@" ;;
    hive|beeline)
        exec "$HIVE_HOME/bin/$cmd" "$@" ;;
    spark-submit|pyspark|spark-shell)
        # The hadoop2.7 build of Spark 2.4.8 also gets the Hadoop 3 jars, as on the HDP node.
        SPARK_DIST_CLASSPATH="$("$HADOOP_HOME/bin/hadoop" classpath)"
        export SPARK_DIST_CLASSPATH
        export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
        export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-python3}"
        exec "$SPARK_HOME/bin/$cmd" "$@" ;;
    *)
        echo "cli-wrapper: unknown command '$cmd'" >&2
        exit 127 ;;
esac
