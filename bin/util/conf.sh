#!/usr/bin/env bash

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
echo "submit path:${SUBMIT}"

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"
echo "jar path:${JAR_PATH}"