#!/bin/bash
set -e
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$HOME/spark
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin

echo "--- Extracting Spark ---"
if [ ! -d ~/spark ]; then
  tar -xzf ~/downloads/spark-3.5.9-bin-hadoop3.tgz -C ~/
  mv ~/spark-3.5.9-bin-hadoop3 ~/spark
fi
echo "Spark extracted: $(ls ~/spark/bin | head -3)"

echo "--- Setting hadoop-env.sh JAVA_HOME ---"
sed -i 's|^# export JAVA_HOME=.*|export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64|' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
grep "^export JAVA_HOME" $HADOOP_HOME/etc/hadoop/hadoop-env.sh

echo "--- Writing core-site.xml ---"
cat > $HADOOP_HOME/etc/hadoop/core-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/home/aryan/hadoop-data/tmp</value>
  </property>
</configuration>
EOF

echo "--- Writing hdfs-site.xml ---"
cat > $HADOOP_HOME/etc/hadoop/hdfs-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/home/aryan/hadoop-data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/home/aryan/hadoop-data/datanode</value>
  </property>
</configuration>
EOF

echo "--- Writing mapred-site.xml ---"
cat > $HADOOP_HOME/etc/hadoop/mapred-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
</configuration>
EOF

echo "--- Writing yarn-site.xml ---"
cat > $HADOOP_HOME/etc/hadoop/yarn-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
EOF

mkdir -p /home/aryan/hadoop-data/tmp /home/aryan/hadoop-data/namenode /home/aryan/hadoop-data/datanode

echo "--- CONFIG DONE ---"
