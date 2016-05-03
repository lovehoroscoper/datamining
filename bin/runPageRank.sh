#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

RESULT_DIR="/user/digu/pageRankScore"
hdfs dfs -test -e ${RESULT_DIR}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR} exists"
    hdfs dfs -rm -r ${RESULT_DIR}
fi
echo "result dir: ${RESULT_DIR}"

INPUT_DIR="/user/digu/itemGraphDiff"
echo "input dir: ${INPUT_DIR}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.ml.pagerank.PageRank						\
	"${JAR_PATH}"												\
	"${RESULT_DIR}"												\
	"${INPUT_DIR}"												\