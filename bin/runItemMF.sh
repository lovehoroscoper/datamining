#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

INPUT_PATH="/user/digu/userItemPrefer"
echo "input path ${INPUT_PATH}"

OUTPUT_PATH="/user/digu/itemMF"
hdfs dfs -test -e ${OUTPUT_PATH}
if [ $? -eq 0 ] ;then
    echo "${OUTPUT_PATH} exists"
    hdfs dfs -rm -r ${OUTPUT_PATH}
fi
echo "output path ${OUTPUT_PATH}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm   									\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.ml.mf.ItemMF								\
	"${JAR_PATH}"												\
	"${INPUT_PATH}"												\
	"${OUTPUT_PATH}"											\