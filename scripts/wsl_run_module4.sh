#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$HOME/spark
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin
export PYTHONUNBUFFERED=1

WIN_PROJECT="/mnt/c/Users/ARYAN BHUTANI/Desktop/BDA-Project"
cp "$WIN_PROJECT/scripts/spark_module4_cluster.py" ~/spark_module4_cluster.py

if ! $JAVA_HOME/bin/jps | grep -q NameNode; then
  echo "HDFS not running - starting it in this session"
  sudo service ssh start
  $HADOOP_HOME/sbin/start-dfs.sh
fi
$HADOOP_HOME/bin/hdfs dfsadmin -safemode wait

$SPARK_HOME/bin/spark-submit --master "local[*]" ~/spark_module4_cluster.py 2>/tmp/spark_m4_stderr.log
rc=$?
if [ $rc -ne 0 ]; then
  echo "SPARK JOB FAILED (exit $rc). Last stderr lines:"
  grep -v -E "^\s+at |INFO " /tmp/spark_m4_stderr.log | tail -30
fi
cp /home/aryan/module4_stats.json /home/aryan/module4_points.json "$WIN_PROJECT/eda/" 2>/dev/null
exit $rc
