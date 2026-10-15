#!/bin/bash
# First boot only: format HDFS, create the shared folders, initialise the metastore schema.
set -euo pipefail

mkdir -p /hadoop/data/namenode /hadoop/data/datanode

hdfs namenode -format -force -nonInteractive
hdfs --daemon start namenode
hdfs --daemon start datanode

# The NameNode's own principal is the HDFS superuser.
kinit -kt /etc/security/keytabs/hdfs.keytab "hdfs/$(hostname -f)@CDP.LOCAL"
hdfs dfsadmin -safemode wait

# /tmp: DistCp staging and the DAG's distcp_log_root. /tmp/hive: Hive scratch.
# The warehouse is world-writable so svc_migration can seed tables.
hdfs dfs -mkdir -p /tmp/hive /user/hive/warehouse /user/svc_migration
hdfs dfs -chmod 1777 /tmp /tmp/hive /user/hive/warehouse
hdfs dfs -chown svc_migration /user/svc_migration
kdestroy

"$HIVE_HOME/bin/schematool" -initSchema -dbType derby

hdfs --daemon stop datanode
hdfs --daemon stop namenode
