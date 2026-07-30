#!/bin/bash
set -e

mkdir -p /hadoop/data/namenode /hadoop/data/datanode /hadoop/data/tmp

if [ ! -f /hadoop/data/namenode/current/VERSION ]; then
    $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
fi

$HADOOP_HOME/sbin/start-dfs.sh
sleep 5

$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/hive/warehouse
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /tmp
$HADOOP_HOME/bin/hdfs dfs -chmod g+w /tmp
$HADOOP_HOME/bin/hdfs dfs -chmod g+w /user/hive/warehouse

rm -rf /hadoop/data/metastore_db
$HIVE_HOME/bin/schematool -initSchema -dbType derby

$HADOOP_HOME/sbin/stop-dfs.sh