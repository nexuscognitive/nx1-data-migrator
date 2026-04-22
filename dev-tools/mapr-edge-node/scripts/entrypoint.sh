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

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

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
echo "MapR-equivalent edge node ready!"
echo "Hadoop: 2.7.7 (simulates MapR 2.7.0-mapr-1808)"
echo "Hive:   1.2.2"
echo "Spark:  2.3.4"
echo "SSH:  ssh root@localhost -p 2223  (password: root)"
echo "HDFS: http://localhost:9871"
echo "YARN: http://localhost:8089"
echo "==============================="
tail -f /dev/null