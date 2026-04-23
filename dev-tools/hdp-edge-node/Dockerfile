FROM ubuntu:20.04

LABEL hdp.hadoop.version="3.1.1" \
    hdp.hive.version="3.1.0" \
    hdp.spark.version="2.4.8"

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
    openjdk-8-jdk python3 python3-pip ssh openssh-server \
    wget curl net-tools vim procps \
    && rm -rf /var/lib/apt/lists/*

# Hadoop 3.1.1 — matches HDP 3.1.1
RUN wget -q --tries=5 --waitretry=10 \
    https://archive.apache.org/dist/hadoop/common/hadoop-3.1.1/hadoop-3.1.1.tar.gz \
    && tar -xzf hadoop-3.1.1.tar.gz -C /opt/ \
    && mv /opt/hadoop-3.1.1 /opt/hadoop \
    && rm hadoop-3.1.1.tar.gz

# Hive 3.1.0 — matches HDP 3.1.1
RUN wget -q --tries=5 --waitretry=10 \
    https://archive.apache.org/dist/hive/hive-3.1.0/apache-hive-3.1.0-bin.tar.gz \
    && tar -xzf apache-hive-3.1.0-bin.tar.gz -C /opt/ \
    && mv /opt/apache-hive-3.1.0-bin /opt/hive \
    && rm apache-hive-3.1.0-bin.tar.gz

# Spark 2.4.8 built for Hadoop 2.7 — SPARK_DIST_CLASSPATH points Spark at
# Hadoop 3.x JARs at runtime so it uses the correct Hadoop version.
RUN wget -q --tries=5 --waitretry=10 \
    https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz \
    && tar -xzf spark-2.4.8-bin-hadoop2.7.tgz -C /opt/ \
    && mv /opt/spark-2.4.8-bin-hadoop2.7 /opt/spark \
    && rm spark-2.4.8-bin-hadoop2.7.tgz

# Fix Guava conflict: Hive 3.1.0 ships guava-19.0.jar but Hadoop 3.1.1 requires
# guava-27.0-jre.jar. Without this fix Hive fails on startup with:
#   NoSuchMethodError: com.google.common.base.Preconditions.checkArgument
RUN rm -f /opt/hive/lib/guava-*.jar \
    && cp /opt/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar /opt/hive/lib/

COPY conf/core-site.xml   $HADOOP_HOME/etc/hadoop/core-site.xml
COPY conf/hdfs-site.xml   $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY conf/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
COPY conf/yarn-site.xml   $HADOOP_HOME/etc/hadoop/yarn-site.xml
COPY conf/hive-site.xml   $HIVE_HOME/conf/hive-site.xml

COPY scripts/entrypoint.sh        /entrypoint.sh
COPY scripts/bootstrap-hdfs.sh   /bootstrap-hdfs.sh
COPY scripts/profile-additions.sh /etc/profile.d/hadoop.sh
COPY scripts/setup-test-data.sh   /setup-test-data.sh
COPY scripts/test-discover.sh     /test-discover.sh
COPY scripts/test-distcp.sh       /test-distcp.sh

RUN chmod +x /entrypoint.sh /bootstrap-hdfs.sh /etc/profile.d/hadoop.sh \
    /setup-test-data.sh /test-discover.sh /test-distcp.sh

# Copy hive-site.xml into Spark conf so PySpark reads hive.metastore.uris
# automatically at startup — no PYSPARK_SUBMIT_ARGS needed.
RUN mkdir -p /opt/spark/conf \
    && cp /opt/hive/conf/hive-site.xml /opt/spark/conf/hive-site.xml

RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh \
    && echo 'export HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"' \
    >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Hadoop 3.x requires explicit user env vars for all daemon scripts.
# Hadoop 2.x inferred these automatically — this is the most common cause of
# start-dfs.sh / start-yarn.sh failing silently on Hadoop 3.x.
RUN echo "export HDFS_NAMENODE_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh \
    && echo "export HDFS_DATANODE_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh \
    && echo "export HDFS_SECONDARYNAMENODE_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh \
    && echo "export YARN_RESOURCEMANAGER_USER=root" >> $HADOOP_HOME/etc/hadoop/yarn-env.sh \
    && echo "export YARN_NODEMANAGER_USER=root" >> $HADOOP_HOME/etc/hadoop/yarn-env.sh

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

RUN printf '\n# Hadoop/Hive/Spark environment (HDP-equivalent)\n' >> /root/.profile \
    && echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> /root/.profile \
    && echo 'export HADOOP_HOME=/opt/hadoop' >> /root/.profile \
    && echo 'export HIVE_HOME=/opt/hive' >> /root/.profile \
    && echo 'export SPARK_HOME=/opt/spark' >> /root/.profile \
    && echo 'export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop' >> /root/.profile \
    && echo 'export HIVE_CONF_DIR=/opt/hive/conf' >> /root/.profile \
    && echo 'export SPARK_CONF_DIR=/opt/spark/conf' >> /root/.profile \
    && echo 'export HDFS_NAMENODE_USER=root' >> /root/.profile \
    && echo 'export HDFS_DATANODE_USER=root' >> /root/.profile \
    && echo 'export HDFS_SECONDARYNAMENODE_USER=root' >> /root/.profile \
    && echo 'export YARN_RESOURCEMANAGER_USER=root' >> /root/.profile \
    && echo 'export YARN_NODEMANAGER_USER=root' >> /root/.profile \
    && echo 'export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SPARK_HOME/bin:$PATH' >> /root/.profile \
    && echo 'export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH' >> /root/.profile \
    && echo 'export PYSPARK_PYTHON=python3' >> /root/.profile \
    && echo 'export PYSPARK_DRIVER_PYTHON=python3' >> /root/.profile \
    && echo 'export SPARK_DIST_CLASSPATH=$(hadoop classpath)' >> /root/.profile

EXPOSE 22 8088 9870 10000
ENTRYPOINT ["/entrypoint.sh"]