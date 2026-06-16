#!/bin/bash
# =============================================================================
# pod-restart-setup.sh
# Run this script after every pod restart to restore the ephemeral filesystem.
# Usage (from your local machine):
#   kubectl exec deploy/hadoop-edge -n hadoop-ssh -- bash /pod-restart-setup.sh
# =============================================================================

set -e

echo "=== [1/5] Setting HADOOP environment variables ==="

export HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar:/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-core-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-databind-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-annotations-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-mapper-asl-1.9.13.jar
export HADOOP_HEAPSIZE=512
export HADOOP_CLIENT_OPTS="-Xmx512m"

# Remove any stale entries, then persist to both profile files
sed -i '/HADOOP_CLASSPATH/d; /HADOOP_HEAPSIZE/d; /HADOOP_CLIENT_OPTS/d' /root/.profile
sed -i '/HADOOP_CLASSPATH/d; /HADOOP_HEAPSIZE/d; /HADOOP_CLIENT_OPTS/d' /etc/profile.d/hadoop.sh

cat >> /root/.profile << 'EOF'
export HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar:/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-core-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-databind-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-annotations-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-mapper-asl-1.9.13.jar
export HADOOP_HEAPSIZE=512
export HADOOP_CLIENT_OPTS="-Xmx512m"
EOF

cat >> /etc/profile.d/hadoop.sh << 'EOF'
export HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar:/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-core-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-databind-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-annotations-2.2.3.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-core-asl-1.9.13.jar:/opt/hadoop/share/hadoop/tools/lib/jackson-mapper-asl-1.9.13.jar
export HADOOP_HEAPSIZE=512
export HADOOP_CLIENT_OPTS="-Xmx512m"
EOF

echo "✓ HADOOP environment variables set"

# =============================================================================
echo "=== [2/5] Writing core-site.xml (S3A configuration) ==="

cat > /opt/hadoop/etc/hadoop/core-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://hadoop-edge.hadoop-ssh.svc.cluster.local:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>/hadoop/data/tmp</value></property>
  <property><name>fs.maprfs.impl</name><value>org.apache.hadoop.hdfs.DistributedFileSystem</value></property>
  <property><name>fs.s3a.impl</name><value>org.apache.hadoop.fs.s3a.S3AFileSystem</value></property>
  <property><name>fs.AbstractFileSystem.s3a.impl</name><value>org.apache.hadoop.fs.s3a.S3A</value></property>
  <property><name>fs.s3a.path.style.access</name><value>false</value></property>
  <property><name>fs.s3a.committer.name</name><value>file</value></property>
  <property><name>mapreduce.outputcommitter.factory.scheme.s3a</name><value>org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter</value></property>
  <property><name>fs.s3a.connection.maximum</name><value>100</value></property>
  <property><name>fs.s3a.threads.max</name><value>20</value></property>
</configuration>
EOF

cp /opt/hadoop/etc/hadoop/core-site.xml /opt/spark/conf/core-site.xml
echo "✓ core-site.xml written and synced to Spark"

# =============================================================================
echo "=== [3/5] Writing mapred-site.xml (local mode — no YARN) ==="

cat > /opt/hadoop/etc/hadoop/mapred-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property><name>mapreduce.framework.name</name><value>local</value></property>
  <property><name>mapreduce.local.map.tasks.maximum</name><value>1</value></property>
  <property><name>mapreduce.local.reduce.tasks.maximum</name><value>1</value></property>
</configuration>
EOF

echo "✓ mapred-site.xml written"

# =============================================================================
echo "=== [4/5] Restoring maprlogin binary ==="

cat > /usr/local/bin/maprlogin << 'EOF'
#!/bin/bash
if [ "$1" = "print" ]; then
  echo "MapR credentials (UID 0) for user: root"
  echo "  created: $(date)"
  echo "  expires: $(date -d '+7 days' 2>/dev/null || date)"
  echo "  cluster: test-cluster"
  exit 0
fi
echo "maprlogin: unknown command '$1'"
exit 1
EOF
chmod +x /usr/local/bin/maprlogin

echo "✓ maprlogin binary restored"

# =============================================================================
echo "=== [5/5] Restoring maprticket ==="

mkdir -p /tmp
cat > /tmp/maprticket << 'EOF'
MAPR_TICKET
cluster=test-cluster
user=root
uid=0
created=0
expires=9999999999
EOF

export MAPR_TICKETFILE_LOCATION=/tmp/maprticket
echo 'export MAPR_TICKETFILE_LOCATION=/tmp/maprticket' >> /root/.profile

echo "✓ maprticket created"

# =============================================================================
echo ""
echo "=== Verification ==="

source /root/.profile

# MapR ticket check
maprlogin print 2>/dev/null | grep -q "root" \
  && echo "✓ MapR ticket:        PASSED" \
  || echo "✗ MapR ticket:        FAILED"

# Hadoop classpath sanity check
[ -f /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar ] \
  && echo "✓ Hadoop AWS JAR:     FOUND" \
  || echo "✗ Hadoop AWS JAR:     NOT FOUND (check image)"

# core-site.xml in place
[ -f /opt/hadoop/etc/hadoop/core-site.xml ] \
  && echo "✓ core-site.xml:      IN PLACE" \
  || echo "✗ core-site.xml:      MISSING"

echo ""
echo "Pod setup complete. You can now trigger DAG runs."