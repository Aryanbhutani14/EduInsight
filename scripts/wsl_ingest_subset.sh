#!/bin/bash
set -e
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

WIN_PROJECT="/mnt/c/Users/ARYAN BHUTANI/Desktop/BDA-Project"

echo "--- Copying subset_cs from Windows mount into WSL-native filesystem ---"
mkdir -p ~/data/subset_cs
cp -v "$WIN_PROJECT/subset_cs/"* ~/data/subset_cs/
ls -la ~/data/subset_cs/

echo "--- Creating HDFS directory structure ---"
hdfs dfs -mkdir -p /eduinsight/subset_cs

echo "--- Uploading subset_cs files to HDFS ---"
for f in course.json video.json concept.json concept-video.txt concept-course.txt video_id-ccid.txt course-field.json; do
  hdfs dfs -put -f ~/data/subset_cs/$f /eduinsight/subset_cs/
  echo "uploaded: $f"
done

echo "--- HDFS listing ---"
hdfs dfs -ls -h /eduinsight/subset_cs

echo "--- Total size in HDFS ---"
hdfs dfs -du -s -h /eduinsight/subset_cs
