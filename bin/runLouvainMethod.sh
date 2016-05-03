#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB3=`date -d "${CUR_DATE} -3 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
DAY_SUB15=`date -d "${CUR_DATE} -15 day" +"%Y-%m-%d"`
YESTERDAY=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d %H:%M:%S"`

RESULT_DIR="/user/digu/modularityWithTitle"
hdfs dfs -test -e ${RESULT_DIR}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR} exists"
    hdfs dfs -rm -r ${RESULT_DIR}
fi
echo "result dir: ${RESULT_DIR}"

DATA_DIR="/user/digu/itemBigraphSim/resultUnionGroup"
for k in $( seq 1 10 )
do
	DAY_SUB=`date -d "${CUR_DATE} -${k} day" +"%Y-%m-%d"`
	FILE_PATH=${DATA_DIR}/${DAY_SUB}
	hdfs dfs -test -e ${FILE_PATH}/"_SUCCESS"
	if [ $? -eq 0 ] ;then
    	echo "${FILE_PATH} exists"
    	break
	fi
done

INPUT_DIR=${FILE_PATH}
echo "input dir: ${INPUT_DIR}"

DATA_DIR="/user/digu/itemSim"
for k in $( seq 1 10 )
do
	DAY_SUB=`date -d "${CUR_DATE} -${k} day" +"%Y-%m-%d"`
	FILE_PATH=${DATA_DIR}/${DAY_SUB}
	hdfs dfs -test -e ${FILE_PATH}/"_SUCCESS"
	if [ $? -eq 0 ] ;then
    	echo "${FILE_PATH} exists"
    	break
	fi
done

INPUT_DIR_SUPPLEMENT=${FILE_PATH}
echo "input dir supplement: ${INPUT_DIR_SUPPLEMENT}"

N="50"
echo "N: ${N}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	32											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.ml.louvain.Main								\
	"${JAR_PATH}"												\
	"${RESULT_DIR}"												\
	"${INPUT_DIR}"												\
	"${INPUT_DIR_SUPPLEMENT}"									\
	"${N}"								                    	\