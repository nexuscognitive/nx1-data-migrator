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
$HIVE_HOME/bin/schematool -dbType derby -initSchema --verbose || true
$HIVE_HOME/bin/hiveserver2 &

echo "==============================="
echo "Container is ready!"
echo "SSH:  ssh root@localhost -p 2222  (password: root)"
echo "HDFS: http://localhost:9870"
echo "YARN: http://localhost:8088"
echo "==============================="
tail -f /dev/null
s