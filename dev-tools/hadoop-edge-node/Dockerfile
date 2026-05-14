FROM ubuntu:18.04

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

RUN wget -q --tries=5 --waitretry=10 \
    https://dlcdn.apache.org/hadoop/common/hadoop-2.10.2/hadoop-2.10.2.tar.gz \
    || wget -q --tries=5 \
    https://archive.apache.org/dist/hadoop/common/hadoop-2.10.2/hadoop-2.10.2.tar.gz \
    && tar -xzf hadoop-2.10.2.tar.gz -C /opt/ \
    && mv /opt/hadoop-2.10.2 /opt/hadoop \
    && rm hadoop-2.10.2.tar.gz

RUN wget -q --tries=5 --waitretry=10 \
    https://dlcdn.apache.org/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz \
    || wget -q --tries=5 \
    https://archive.apache.org/dist/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz \
    && tar -xzf apache-hive-2.3.9-bin.tar.gz -C /opt/ \
    && mv /opt/apache-hive-2.3.9-bin /opt/hive \
    && rm apache-hive-2.3.9-bin.tar.gz

RUN wget -q --tries=5 --waitretry=10 \
    https://dlcdn.apache.org/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz \
    || wget -q --tries=5 \
    https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz \
    && tar -xzf spark-2.4.8-bin-hadoop2.7.tgz -C /opt/ \
    && mv /opt/spark-2.4.8-bin-hadoop2.7 /opt/spark \
    && rm spark-2.4.8-bin-hadoop2.7.tgz

COPY conf/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY conf/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY conf/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
COPY conf/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml
COPY conf/hive-site.xml $HIVE_HOME/conf/hive-site.xml
COPY scripts/entrypoint.sh /entrypoint.sh
COPY scripts/bootstrap-hdfs.sh /bootstrap-hdfs.sh
COPY scripts/profile-additions.sh /etc/profile.d/hadoop.sh

RUN chmod +x /entrypoint.sh /bootstrap-hdfs.sh /etc/profile.d/hadoop.sh

# Copy hive-site.xml to Spark conf so PySpark with enableHiveSupport()
# finds the correct metastore and warehouse without any DAG-level config
RUN mkdir -p /opt/spark/conf \
    && cp /opt/hive/conf/hive-site.xml /opt/spark/conf/hive-site.xml

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

# Bake full environment into ~/.profile so non-interactive SSH sessions
# (used by Airflow SSHHook) find all tools and conf dirs automatically
RUN printf '\n# Hadoop/Hive/Spark environment\n' >> /root/.profile \
    && echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> /root/.profile \
    && echo 'export HADOOP_HOME=/opt/hadoop' >> /root/.profile \
    && echo 'export HIVE_HOME=/opt/hive' >> /root/.profile \
    && echo 'export SPARK_HOME=/opt/spark' >> /root/.profile \
    && echo 'export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop' >> /root/.profile \
    && echo 'export HIVE_CONF_DIR=/opt/hive/conf' >> /root/.profile \
    && echo 'export SPARK_CONF_DIR=/opt/spark/conf' >> /root/.profile \
    && echo 'export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SPARK_HOME/bin:$PATH' >> /root/.profile \
    && echo 'export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH' >> /root/.profile \
    && echo 'export PYSPARK_PYTHON=python2.7' >> /root/.profile \
    && echo 'export PYSPARK_DRIVER_PYTHON=python2.7' >> /root/.profile

EXPOSE 22 8088 9870 10000
ENTRYPOINT ["/entrypoint.sh"]
