#!/bin/bash
set -e
# Remove everything from the marker line onward, then re-append a correct block
MARKER="# --- EduInsight Hadoop/Spark env ---"
LINE=$(grep -n "$MARKER" ~/.bashrc | head -1 | cut -d: -f1)
if [ -n "$LINE" ]; then
  head -n $((LINE - 1)) ~/.bashrc > ~/.bashrc.new
  mv ~/.bashrc.new ~/.bashrc
fi

cat >> ~/.bashrc << 'BASHRC_EOF'

# --- EduInsight Hadoop/Spark env ---
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$HOME/spark
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin
BASHRC_EOF

echo "FIXED_BASHRC_TAIL:"
tail -8 ~/.bashrc
