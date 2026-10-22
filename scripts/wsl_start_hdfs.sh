#!/bin/bash
set -e
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

sudo service ssh start
sleep 1

if [ ! -d /home/aryan/hadoop-data/namenode/current ]; then
  echo "--- Formatting NameNode ---"
  yes | $HADOOP_HOME/bin/hdfs namenode -format
fi

echo "--- Starting HDFS daemons ---"
$HADOOP_HOME/sbin/start-dfs.sh

sleep 3
echo "--- jps ---"
$JAVA_HOME/bin/jps

echo "--- HDFS report ---"
$HADOOP_HOME/bin/hdfs dfsadmin -report 2>&1 | head -20
