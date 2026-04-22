#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export HIVE_CONF_DIR=/opt/hive/conf
export SPARK_CONF_DIR=/opt/spark/conf
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SPARK_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
export PYSPARK_PYTHON=python2.7
export PYSPARK_DRIVER_PYTHON=python2.7