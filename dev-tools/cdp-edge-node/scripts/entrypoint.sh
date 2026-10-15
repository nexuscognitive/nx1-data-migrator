#!/bin/bash
set -euo pipefail

wait_for_port() {  # port, attempts (2s apart)
    for _ in $(seq 1 "$2"); do
        if netstat -ltn 2>/dev/null | grep -q ":$1 "; then
            return 0
        fi
        sleep 2
    done
    echo "ERROR: nothing listening on port $1" >&2
    return 1
}

rm -f /tmp/edge-ready

FQDN="$(hostname -f)"
for f in /etc/hadoop/conf/core-site.xml /etc/hadoop/conf/hdfs-site.xml /etc/hive/conf/hive-site.xml; do
    sed -i "s#@EDGE_FQDN@#${FQDN}#g" "$f"
done

# Addresses for the shared seeding scripts (dev-tools/fixtures), read by login shells.
cat > /etc/profile.d/cdp-edge-seed.sh <<EOF
export EDGE_NAMENODE='hdfs://${FQDN}:9000'
export EDGE_BEELINE_URL='jdbc:hive2://${FQDN}:10000/default'
EOF

if [ -n "${SVC_MIGRATION_PASSWORD:-}" ]; then
    echo "svc_migration:${SVC_MIGRATION_PASSWORD}" | chpasswd
else
    echo "WARNING: SVC_MIGRATION_PASSWORD is not set; svc_migration cannot log in over SSH" >&2
fi

# sshd first, as on the MapR node: the liveness probe watches port 22 while a slow
# first boot runs. The readiness marker below gates traffic instead.
/usr/sbin/sshd
wait_for_port 22 15

if [ ! -f /hadoop/data/.bootstrapped ]; then
    bash /bootstrap-hdfs.sh
    touch /hadoop/data/.bootstrapped
fi

hdfs --daemon start namenode
hdfs --daemon start datanode
hdfs dfsadmin -safemode wait

JAVA_TOOL_OPTIONS="-Djava.security.egd=file:/dev/./urandom" \
    nohup hive --service metastore > /var/log/hive-metastore.log 2>&1 &
wait_for_port 9083 60

JAVA_TOOL_OPTIONS="-Djava.security.egd=file:/dev/./urandom" \
    nohup "$HIVE_HOME/bin/hiveserver2" > /var/log/hiveserver2.log 2>&1 &
wait_for_port 10000 90

touch /tmp/edge-ready
echo "==============================="
echo "CDP edge node ready on ${FQDN}"
echo "Hadoop 3.1.1, Hive 3.1.3, Spark 2.4.8 (simulates CDP Private Cloud Base 7.1.x)"
echo "==============================="
tail -f /dev/null
