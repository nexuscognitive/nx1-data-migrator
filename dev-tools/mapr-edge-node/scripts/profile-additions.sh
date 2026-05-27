#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export HIVE_CONF_DIR=/opt/hive/conf
export SPARK_CONF_DIR=/opt/spark/conf
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SPARK_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
export PYSPARK_PYTHON=python2.7
export PYSPARK_DRIVER_PYTHON=python2.7

# S3A classpath — Hadoop 2.7.7 bundled jars in tools/lib.
# Required for both 'hadoop fs' and 'hadoop distcp' to write to s3a:// URIs.
export HADOOP_CLASSPATH=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar:$HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar:$HADOOP_HOME/share/hadoop/tools/lib/jackson-core-2.2.3.jar:$HADOOP_HOME/share/hadoop/tools/lib/jackson-databind-2.2.3.jar:$HADOOP_HOME/share/hadoop/tools/lib/jackson-annotations-2.2.3.jar:$HADOOP_HOME/share/hadoop/tools/lib/jackson-core-asl-1.9.13.jar:$HADOOP_HOME/share/hadoop/tools/lib/jackson-mapper-asl-1.9.13.jar

# Heap limits — keeps client JVMs within the pod's memory budget
export HADOOP_HEAPSIZE=512
export HADOOP_CLIENT_OPTS="-Xmx512m"

# MapR ticket file location — required by the DAG's cluster_login task
export MAPR_TICKETFILE_LOCATION=/tmp/maprticket