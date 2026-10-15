#!/bin/bash
# First boot only: format HDFS and initialise the metastore schema. The shared HDFS
# folders are ensured by entrypoint.sh on every boot.
set -euo pipefail

mkdir -p /hadoop/data/namenode /hadoop/data/datanode

hdfs namenode -format -force -nonInteractive

"$HIVE_HOME/bin/schematool" -initSchema -dbType derby
