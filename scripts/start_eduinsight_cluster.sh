#!/bin/bash
# Run this once every time you reopen Ubuntu to work on EduInsight.
set -e
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$HOME/spark
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin

echo "--- Starting SSH (required by Hadoop daemon scripts) ---"
sudo service ssh start

echo "--- Starting HDFS daemons ---"
$HADOOP_HOME/sbin/start-dfs.sh

sleep 3
echo "--- Status check ---"
$JAVA_HOME/bin/jps

echo ""
echo "If you see NameNode, DataNode, and SecondaryNameNode above, HDFS is ready."
echo "Spark needs no separate startup — it connects to HDFS on demand."
