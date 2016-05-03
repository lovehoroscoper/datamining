#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB2=`date -d "${CUR_DATE} -2 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
DAY_SUB15=`date -d "${CUR_DATE} -15 day" +"%Y-%m-%d"`
DAY_SUB16=`date -d "${CUR_DATE} -16 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
DAY_SUB31=`date -d "${CUR_DATE} -31 day" +"%Y-%m-%d"`
BIZDATE=${DAY_SUB1}
BIZDATE_SUB1=${DAY_SUB2}
BIZDATE_SUB30=${DAY_SUB31}

echo "bizdate: ${BIZDATE}"
echo "bizdate_sub1: ${BIZDATE_SUB1}"
echo "bizdate_sub30: ${BIZDATE_SUB30}"

DATA_DIR="/user/digu/itemGroupWithTitle/data"
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

RESULT_DIR_CURRENT_USED="/user/digu/itemGroupCurrentUsed/data"
hdfs dfs -test -e ${RESULT_DIR_CURRENT_USED}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_CURRENT_USED} exists"
    hdfs dfs -rm -r ${RESULT_DIR_CURRENT_USED}
fi
echo "result dir current used: ${RESULT_DIR_CURRENT_USED}"
hdfs dfs -cp ${FILE_PATH} ${RESULT_DIR_CURRENT_USED}

GENE_DIR_SUB=${RESULT_DIR_CURRENT_USED}
echo "gene dir: ${GENE_DIR_SUB}"

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
	--class com.mgj.usergeneperfer.UserGenePrefer				\
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB1}"											\
	"${BIZDATE_SUB30}"											\
	"${GENE_DIR_SUB}"											\
