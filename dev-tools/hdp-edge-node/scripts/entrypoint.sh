#!/bin/bash
set -e

service ssh start

mkdir -p ~/.ssh
cat >> ~/.ssh/config << 'SSHEOF'
Host *
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
SSHEOF
chmod 600 ~/.ssh/config

if [ ! -f /hadoop/data/.bootstrapped ]; then
    bash /bootstrap-hdfs.sh
    touch /hadoop/data/.bootstrapped
fi

# Hadoop 3.x requires explicit user env vars for all daemon scripts
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Start Hive metastore thrift first — it takes exclusive ownership of the
# embedded Derby database. HiveServer2 and PySpark then connect to it via
# thrift, avoiding the XSDB6 Derby lock conflict.
#
# JAVA_TOOL_OPTIONS=-Djava.security.egd=file:/dev/./urandom prevents the JVM
# from blocking on /dev/random for entropy in Docker, which causes HiveServer2
# to hang silently with zero log output.
echo "Starting Hive metastore thrift service..."
JAVA_TOOL_OPTIONS="-Djava.security.egd=file:/dev/./urandom" \
  $HIVE_HOME/bin/hive --service metastore &
METASTORE_PID=$!

echo "Waiting for Hive metastore to start on port 9083..."
for i in $(seq 1 60); do
    if netstat -tlnp 2>/dev/null | grep -q 9083; then
        echo "Metastore ready."
        break
    fi
    sleep 2
done

echo "Starting HiveServer2..."
JAVA_TOOL_OPTIONS="-Djava.security.egd=file:/dev/./urandom" \
  $HIVE_HOME/bin/hiveserver2 \
  --hiveconf hive.metastore.uris=thrift://localhost:9083 &
HS2_PID=$!

echo "Waiting for HiveServer2 to start..."
for i in $(seq 1 30); do
    if $HIVE_HOME/bin/beeline -u "jdbc:hive2://localhost:10000" \
        -e "SHOW DATABASES;" > /dev/null 2>&1; then
        break
    fi
    sleep 2
done

echo "==============================="
echo "HDP-equivalent edge node ready!"
echo "Hadoop: 3.1.1 (simulates HDP 3.1.1)"
echo "Hive:   3.1.0"
echo "Spark:  2.4.8"
echo "SSH:  ssh root@localhost -p 2224  (password: root)"
echo "HDFS: http://localhost:9872"
echo "YARN: http://localhost:8090"
echo "==============================="
tail -f /dev/null