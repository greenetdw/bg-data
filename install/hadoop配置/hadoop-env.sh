#!/bin/bash

export JAVA_HOME=/usr/local/jdk
export HADOOP_PID_IDR=/home/hadoop/bd/hadoop-2.5.0-cdh5.3.6/hdfs/tmp

if [ "$HADOOP_CLASSPATH" ];then
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/.../hbase-xxx/lib
else
    export HADOOP_CLASSPATH=/.../hbase-xxx/lib
fi