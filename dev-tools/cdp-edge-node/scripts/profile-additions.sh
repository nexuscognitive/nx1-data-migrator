# Interactive-shell conveniences, installed as /etc/profile.d/hadoop.sh. The DAG
# must not depend on anything here: a client's edge node may not have it. The
# /usr/bin wrappers carry their own environment.
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HIVE_HOME=/opt/hive
export HIVE_CONF_DIR=/etc/hive/conf
export SPARK_HOME=/opt/spark
export SPARK_CONF_DIR=/etc/spark/conf
