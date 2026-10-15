# CDP-style edge node (Hadoop 3.1.1 / Hive 3.1.3 / Spark 2.4.8 as in CDP Private Cloud
# Base 7.1.x). Spec: docs/superpowers/specs/2026-10-14-cdp-kerberos-edge-node-design.md
FROM ubuntu:18.04

LABEL cdp.simulated="7.1.x" hadoop.version="3.1.1" hive.version="3.1.3" spark.version="2.4.8"

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_HOME=/opt/hive
ENV SPARK_HOME=/opt/spark
ENV HADOOP_CONF_DIR=/etc/hadoop/conf
ENV HIVE_CONF_DIR=/etc/hive/conf
ENV SPARK_CONF_DIR=/etc/spark/conf
# No /opt/*/bin on PATH: as on CDP, the CLIs are reached through the /usr/bin wrappers,
# so `docker exec` / `kubectl exec` sessions behave like SSH sessions.

RUN apt-get update && apt-get install -y \
    openjdk-8-jdk python3 openssh-server net-tools procps wget curl vim \
    && rm -rf /var/lib/apt/lists/*

# archive.apache.org throttles heavily; any full Apache mirror can stand in (e.g.
# https://mirrors.huaweicloud.com/apache). The checksums are Apache's own, so a mirror
# can't change what gets installed.
ARG APACHE_MIRROR=https://archive.apache.org/dist

RUN wget -q --tries=5 --waitretry=10 \
    "${APACHE_MIRROR}/hadoop/common/hadoop-3.1.1/hadoop-3.1.1.tar.gz" \
    && echo "f837fe260587f71629aad1f4fb6719274e948111dc96ffc5a8e26f27deac5602  hadoop-3.1.1.tar.gz" | sha256sum -c - \
    && tar -xzf hadoop-3.1.1.tar.gz -C /opt/ && mv /opt/hadoop-3.1.1 /opt/hadoop \
    && rm hadoop-3.1.1.tar.gz

RUN wget -q --tries=5 --waitretry=10 \
    "${APACHE_MIRROR}/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz" \
    && echo "0c9b6a6359a7341b6029cc9347435ee7b379f93846f779d710b13f795b54bb16  apache-hive-3.1.3-bin.tar.gz" | sha256sum -c - \
    && tar -xzf apache-hive-3.1.3-bin.tar.gz -C /opt/ && mv /opt/apache-hive-3.1.3-bin /opt/hive \
    && rm apache-hive-3.1.3-bin.tar.gz

RUN wget -q --tries=5 --waitretry=10 \
    "${APACHE_MIRROR}/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz" \
    && echo "752c4d4d8fe1d72f5ba01f40d22df35698585bd17ed4749f6065b0039ff40db7ff8ea87dc0fb5b1ec03871e427a002581ec12f486392b92b88643d4243908e55  spark-2.4.8-bin-hadoop2.7.tgz" | sha512sum -c - \
    && tar -xzf spark-2.4.8-bin-hadoop2.7.tgz -C /opt/ && mv /opt/spark-2.4.8-bin-hadoop2.7 /opt/spark \
    && rm spark-2.4.8-bin-hadoop2.7.tgz

# Hive ships guava 19. If Hadoop's guava is newer (27 on later 3.1.x builds), Hive must
# use Hadoop's or it fails with NoSuchMethodError on Preconditions.checkArgument.
RUN HADOOP_GUAVA="$(ls /opt/hadoop/share/hadoop/common/lib/guava-*.jar)" \
    && case "$(basename "$HADOOP_GUAVA")" in \
         guava-2[0-9]*) rm -f /opt/hive/lib/guava-*.jar && cp "$HADOOP_GUAVA" /opt/hive/lib/ ;; \
       esac

# krb5-admin-server only for kadmin.local (principals are managed locally; kadmind never runs).
RUN apt-get update && apt-get install -y krb5-kdc krb5-admin-server krb5-user \
    && rm -rf /var/lib/apt/lists/*
COPY conf/krb5.conf /etc/krb5.conf
COPY conf/kdc.conf /etc/krb5kdc/kdc.conf
# The KDC database and keytabs live on the data volume (spec section 4).
RUN rm -rf /var/lib/krb5kdc /etc/security/keytabs \
    && mkdir -p /etc/security \
    && ln -s /hadoop/data/krb5kdc /var/lib/krb5kdc \
    && ln -s /hadoop/data/keytabs /etc/security/keytabs

# Config lives where CDP keeps it.
RUN mkdir -p /etc/hadoop/conf /etc/hive/conf /etc/spark/conf \
    && cp -r /opt/hadoop/etc/hadoop/. /etc/hadoop/conf/ \
    && cp -r /opt/hive/conf/. /etc/hive/conf/ \
    && ln -s /etc/hive/conf/hive-site.xml /etc/spark/conf/hive-site.xml
COPY conf/core-site.xml conf/hdfs-site.xml conf/mapred-site.xml /etc/hadoop/conf/
COPY conf/hive-site.xml /etc/hive/conf/
RUN { echo "export JAVA_HOME=${JAVA_HOME}"; \
      echo 'export HADOOP_OPTIONAL_TOOLS=hadoop-aws'; \
      echo 'export HDFS_NAMENODE_USER=root'; \
      echo 'export HDFS_DATANODE_USER=root'; \
    } >> /etc/hadoop/conf/hadoop-env.sh

COPY scripts/cli-wrapper.sh /usr/lib/cdp-edge/cli-wrapper
RUN chmod 755 /usr/lib/cdp-edge/cli-wrapper \
    && for c in hadoop hdfs mapred hive beeline spark-submit pyspark spark-shell; do \
         ln -s /usr/lib/cdp-edge/cli-wrapper "/usr/bin/$c"; \
       done
COPY scripts/profile-additions.sh /etc/profile.d/hadoop.sh

RUN useradd -m -s /bin/bash svc_migration \
    && printf '%s\n' '' \
       '# Service-account ticket from its keytab: the edge node holds the ticket, the DAG only checks it.' \
       '( flock 9; klist -s || kinit -kt /etc/security/keytabs/svc_migration.keytab svc_migration@CDP.LOCAL ) 9>/tmp/.kinit.svc_migration.lock' \
       >> /home/svc_migration/.profile

RUN mkdir -p /run/sshd && ssh-keygen -A \
    && sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin no/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config

COPY scripts/entrypoint.sh /entrypoint.sh
COPY scripts/bootstrap-hdfs.sh /bootstrap-hdfs.sh
COPY scripts/test-kerberos.sh /test-kerberos.sh
COPY scripts/kdc-init.sh /kdc-init.sh
COPY --from=fixtures scenarios.yaml /scenarios.yaml
COPY --from=fixtures setup-test-data.sh /setup-test-data.sh
COPY --from=fixtures setup-distcp-test-data.sh /setup-distcp-test-data.sh
RUN chmod 755 /entrypoint.sh /bootstrap-hdfs.sh /test-kerberos.sh /kdc-init.sh \
    /setup-test-data.sh /setup-distcp-test-data.sh

EXPOSE 22 9870 10000
ENTRYPOINT ["/entrypoint.sh"]
