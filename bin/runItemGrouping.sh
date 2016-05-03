#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

RESULT_DIR="/user/digu/itemGroupWithTitle"
echo "result dir: ${RESULT_DIR}"

INPUT_DIR="/user/digu/modularityWithTitle"
echo "input dir: ${INPUT_DIR}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.ml.louvain.Grouping							\
	"${JAR_PATH}"												\
	"${INPUT_DIR}"												\
	"${RESULT_DIR}"												\

CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB20=`date -d "${CUR_DATE} -20 day" +"%Y-%m-%d"`
RESULT_DIR_SUB=${RESULT_DIR}/data/${DAY_SUB20}
hdfs dfs -test -e ${RESULT_DIR_SUB}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_SUB} exists"
    hdfs dfs -rm -r ${RESULT_DIR_SUB}
fi

RESULT_DIR_SUB=${RESULT_DIR}/index/${DAY_SUB20}
hdfs dfs -test -e ${RESULT_DIR_SUB}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_SUB} exists"
    hdfs dfs -rm -r ${RESULT_DIR_SUB}
fi