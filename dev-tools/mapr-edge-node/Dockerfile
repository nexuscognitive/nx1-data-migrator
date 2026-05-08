FROM ubuntu:18.04

# MapR Hadoop 2.7.0-mapr-1808 equivalent:
# - Hadoop 2.7.7 (same 2.7.x base, latest patch)
# - Hive 1.2.2   (MapR MEP 5.0 / mapr-1808 ships Hive 1.2)
# - Spark 2.3.4  (MapR MEP 5.0 ships Spark 2.3)
LABEL mapr.hadoop.version="2.7.0-mapr-1808" \
    mapr.hadoop.oss.equivalent="2.7.7" \
    mapr.hive.version="1.2.2" \
    mapr.spark.version="2.3.4"

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_HOME=/opt/hive
ENV SPARK_HOME=/opt/spark
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV HIVE_CONF_DIR=/opt/hive/conf
ENV SPARK_CONF_DIR=/opt/spark/conf
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SPARK_HOME/bin:$PATH

RUN apt-get update && apt-get install -y \
    openjdk-8-jdk python2.7 python-pip ssh openssh-server \
    wget curl net-tools vim procps \
    && rm -rf /var/lib/apt/lists/*

# Hadoop 2.7.7 — closest OSS to MapR 2.7.0-mapr-1808
RUN wget -q --tries=5 --waitretry=10 \
    https://archive.apache.org/dist/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz \
    && tar -xzf hadoop-2.7.7.tar.gz -C /opt/ \
    && mv /opt/hadoop-2.7.7 /opt/hadoop \
    && rm hadoop-2.7.7.tar.gz

# Hive 1.2.2 — matches MapR MEP 5.0 (mapr-1808) Hive version
RUN wget -q --tries=5 --waitretry=10 \
    https://archive.apache.org/dist/hive/hive-1.2.2/apache-hive-1.2.2-bin.tar.gz \
    && tar -xzf apache-hive-1.2.2-bin.tar.gz -C /opt/ \
    && mv /opt/apache-hive-1.2.2-bin /opt/hive \
    && rm apache-hive-1.2.2-bin.tar.gz

# Spark 2.3.4 — matches MapR MEP 5.0 Spark version, built for Hadoop 2.7
RUN wget -q --tries=5 --waitretry=10 \
    https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz \
    && tar -xzf spark-2.3.4-bin-hadoop2.7.tgz -C /opt/ \
    && mv /opt/spark-2.3.4-bin-hadoop2.7 /opt/spark \
    && rm spark-2.3.4-bin-hadoop2.7.tgz

COPY conf/core-site.xml   $HADOOP_HOME/etc/hadoop/core-site.xml
COPY conf/hdfs-site.xml   $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY conf/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
COPY conf/yarn-site.xml   $HADOOP_HOME/etc/hadoop/yarn-site.xml
COPY conf/hive-site.xml   $HIVE_HOME/conf/hive-site.xml

COPY scripts/entrypoint.sh        /entrypoint.sh
COPY scripts/bootstrap-hdfs.sh   /bootstrap-hdfs.sh
COPY scripts/profile-additions.sh /etc/profile.d/hadoop.sh
COPY scripts/setup-test-data.sh   /setup-test-data.sh
COPY scripts/pod-restart-setup.sh /pod-restart-setup.sh
COPY scripts/test-discover.sh     /test-discover.sh
COPY scripts/test-distcp.sh       /test-distcp.sh

RUN chmod +x /entrypoint.sh /bootstrap-hdfs.sh /etc/profile.d/hadoop.sh \
    /setup-test-data.sh /pod-restart-setup.sh /test-discover.sh /test-distcp.sh

RUN mkdir -p /opt/spark/conf \
    && cp /opt/hive/conf/hive-site.xml /opt/spark/conf/hive-site.xml \
    && cp $HADOOP_HOME/etc/hadoop/core-site.xml $SPARK_HOME/conf/core-site.xml

RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh \
    && echo 'export HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"' \
    >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

RUN mkdir -p /run/sshd /root/.ssh \
    && ssh-keygen -A \
    && echo "root:root" | chpasswd \
    && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa \
    && cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys \
    && chmod 0600 /root/.ssh/authorized_keys \
    && echo "StrictHostKeyChecking no" >> /root/.ssh/config \
    && echo "UserKnownHostsFile /dev/null" >> /root/.ssh/config

# Fake maprlogin binary — allows the DAG's cluster_login task to pass
# its MapR ticket check without a real MapR installation
RUN cat > /usr/local/bin/maprlogin << 'MAPREOF' \
    && echo '#!/bin/bash' > /usr/local/bin/maprlogin \
    && echo 'if [ "$1" = "print" ]; then' >> /usr/local/bin/maprlogin \
    && echo '  echo "MapR credentials (UID 0) for user: root"' >> /usr/local/bin/maprlogin \
    && echo '  echo "  created: $(date)"' >> /usr/local/bin/maprlogin \
    && echo '  echo "  expires: $(date -d '"'"'+7 days'"'"' 2>/dev/null || date)"' >> /usr/local/bin/maprlogin \
    && echo '  echo "  cluster: test-cluster"' >> /usr/local/bin/maprlogin \
    && echo '  exit 0' >> /usr/local/bin/maprlogin \
    && echo 'fi' >> /usr/local/bin/maprlogin \
    && echo 'exit 1' >> /usr/local/bin/maprlogin \
    && chmod +x /usr/local/bin/maprlogin

# Bake the MapR ticket file so it survives across SSH sessions
RUN mkdir -p /tmp \
    && printf 'MAPR_TICKET\ncluster=test-cluster\nuser=root\nuid=0\ncreated=0\nexpires=9999999999\n' \
    > /tmp/maprticket

RUN printf '\n# Hadoop/Hive/Spark environment (MapR-equivalent)\n' >> /root/.profile \
    && echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> /root/.profile \
    && echo 'export HADOOP_HOME=/opt/hadoop' >> /root/.profile \
    && echo 'export HIVE_HOME=/opt/hive' >> /root/.profile \
    && echo 'export SPARK_HOME=/opt/spark' >> /root/.profile \
    && echo 'export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop' >> /root/.profile \
    && echo 'export HIVE_CONF_DIR=/opt/hive/conf' >> /root/.profile \
    && echo 'export SPARK_CONF_DIR=/opt/spark/conf' >> /root/.profile \
    && echo 'export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SPARK_HOME/bin:$PATH' >> /root/.profile \
    && echo 'export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH' >> /root/.profile \
    && echo 'export PYSPARK_PYTHON=python2.7' >> /root/.profile \
    && echo 'export PYSPARK_DRIVER_PYTHON=python2.7' >> /root/.profile \
    && echo 'export HADOOP_CLASSPATH=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar:$HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar:$HADOOP_HOME/share/hadoop/tools/lib/jackson-core-2.2.3.jar:$HADOOP_HOME/share/hadoop/tools/lib/jackson-databind-2.2.3.jar:$HADOOP_HOME/share/hadoop/tools/lib/jackson-annotations-2.2.3.jar:$HADOOP_HOME/share/hadoop/tools/lib/jackson-core-asl-1.9.13.jar:$HADOOP_HOME/share/hadoop/tools/lib/jackson-mapper-asl-1.9.13.jar' >> /root/.profile \
    && echo 'export HADOOP_HEAPSIZE=512' >> /root/.profile \
    && echo 'export HADOOP_CLIENT_OPTS="-Xmx512m"' >> /root/.profile \
    && echo 'export MAPR_TICKETFILE_LOCATION=/tmp/maprticket' >> /root/.profile

EXPOSE 22 8088 9870 10000
ENTRYPOINT ["/entrypoint.sh"]