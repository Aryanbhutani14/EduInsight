#!/bin/bash
# Load the FULL MOOCCubeX files (not just the CS subset) into HDFS under /eduinsight/full
set -e
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

WIN_PROJECT="/mnt/c/Users/ARYAN BHUTANI/Desktop/BDA-Project"
FILES="course.json video.json concept.json concept-video.txt concept-course.txt video_id-ccid.txt course-field.json"

if ! $JAVA_HOME/bin/jps | grep -q NameNode; then
  echo "HDFS not running - starting it in this session"
  sudo service ssh start
  $HADOOP_HOME/sbin/start-dfs.sh
fi
$HADOOP_HOME/bin/hdfs dfsadmin -safemode wait

echo "--- Copying full dataset into the WSL filesystem ---"
mkdir -p ~/data/full
for f in $FILES; do
  cp "$WIN_PROJECT/$f" ~/data/full/
  echo "copied $f"
done

echo "--- Uploading to HDFS /eduinsight/full ---"
hdfs dfs -mkdir -p /eduinsight/full
for f in $FILES; do
  hdfs dfs -put -f ~/data/full/$f /eduinsight/full/
  echo "uploaded $f"
done
hdfs dfs -ls -h /eduinsight/full
hdfs dfs -du -s -h /eduinsight/full
