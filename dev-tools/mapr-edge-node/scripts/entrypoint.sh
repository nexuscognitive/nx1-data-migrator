#!/bin/bash
set -e

if [ -f /etc/ssh/external-keys/authorized_keys ]; then
    cat /etc/ssh/external-keys/authorized_keys >> /root/.ssh/authorized_keys
    chmod 0600 /root/.ssh/authorized_keys
fi

# ── Start sshd directly — 'service ssh start' is unreliable in K8s ───────────
/usr/sbin/sshd

# Wait until sshd is actually listening before proceeding
for i in $(seq 1 30); do
    if nc -z localhost 22; then
        echo "sshd ready on port 22"
        break
    fi
    [ $i -eq 30 ] && echo "ERROR: sshd never became ready" && exit 1
    sleep 1
done

mkdir -p ~/.ssh
cat >> ~/.ssh/config << 'SSHEOF'
Host *
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
SSHEOF
chmod 600 ~/.ssh/config

# ── Pin the NameNode's advertised hostname to this pod's own IP ──────────────
# fs.defaultFS points at the headless-service FQDN so the DataNode registers a
# routable address (not 127.0.0.1) for cross-namespace clients. But start-dfs.sh
# below runs before this pod is Ready, and a headless service publishes no DNS
# record for a not-Ready pod — so the FQDN is unresolvable at boot and HDFS
# never starts (start-dfs.sh can't reach the namenode host). Map the FQDN to our
# own IP in /etc/hosts first so start-up is self-contained; cluster DNS takes
# over for external clients once the pod becomes Ready.
NN_HOST=$(sed -n 's#.*hdfs://\([^:/]*\).*#\1#p' "$HADOOP_CONF_DIR/core-site.xml" | head -1)
if [ -n "$NN_HOST" ] && [ "$NN_HOST" != "localhost" ] && ! grep -qF "$NN_HOST" /etc/hosts; then
    POD_IP=$(hostname -i | awk '{print $1}')
    echo "$POD_IP $NN_HOST" >> /etc/hosts
    echo "Pinned $NN_HOST -> $POD_IP in /etc/hosts for boot"
fi

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