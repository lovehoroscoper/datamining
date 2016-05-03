#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

INPUT_PATH="/user/digu/userWordPrefer"
echo "input path: ${INPUT_PATH}"

OUTPUT_PATH="/user/digu/wordSim"
echo "output path: ${OUTPUT_PATH}"

OUTPUT_GROUP_PATH="/user/digu/wordSimGroup"
echo "output group path: ${OUTPUT_PATH}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	16g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.ml.nlp.WordSim  							\
	"${JAR_PATH}"												\
	"${INPUT_PATH}"												\
	"${OUTPUT_PATH}"											\
	"${OUTPUT_GROUP_PATH}"										\

CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB20=`date -d "${CUR_DATE} -20 day" +"%Y-%m-%d"`

RESULT_DIR_SUB=${OUTPUT_GROUP_PATH}/${DAY_SUB20}
hdfs dfs -test -e ${RESULT_DIR_SUB}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_SUB} exists"
    hdfs dfs -rm -r ${RESULT_DIR_SUB}
fi

RESULT_DIR_SUB=${OUTPUT_PATH}/${DAY_SUB20}
hdfs dfs -test -e ${RESULT_DIR_SUB}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_SUB} exists"
    hdfs dfs -rm -r ${RESULT_DIR_SUB}
fi