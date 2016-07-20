#!/usr/bin/env bash

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
echo "submit path: ${SUBMIT}"

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"
echo "jar path: ${JAR_PATH}"

HDFS="/opt/hadoop/bin/hdfs dfs"
echo "hdfs path: ${HDFS}"

CURL="/usr/local/bin/curl"
echo "curl path: ${CURL}"