#!/bin/bash
# Run this to cleanly shut down HDFS before closing Ubuntu (optional, but tidy).
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

echo "--- Stopping HDFS daemons ---"
$HADOOP_HOME/sbin/stop-dfs.sh
echo "--- Done ---"
