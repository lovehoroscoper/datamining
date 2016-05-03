#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB2=`date -d "${CUR_DATE} -2 day" +"%Y-%m-%d"`
DAY_SUB3=`date -d "${CUR_DATE} -3 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
DAY_SUB15=`date -d "${CUR_DATE} -15 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
DAY_SUB31=`date -d "${CUR_DATE} -31 day" +"%Y-%m-%d"`
BIZDATE=${DAY_SUB1}
BIZDATE_SUB1=${DAY_SUB2}
BIZDATE_SUB7=${DAY_SUB7}

echo "bizdate: ${BIZDATE}"
echo "bizdate_sub1: ${BIZDATE_SUB1}"
echo "bizdate_sub7: ${BIZDATE_SUB7}"

DATA_DIR="/user/digu/itemSimMerge"
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

ITEM_SIM_PATH=${FILE_PATH}
echo "item sim path: ${ITEM_SIM_PATH}"

ITEM_CTR_PATH="/user/bizdata/ctrRecord/${DAY_SUB2}"
echo "item ctr path: ${ITEM_CTR_PATH}"

# input table.
SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm   									\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.useritemprefer.UserItemPrefer				\
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB1}"											\
	"${BIZDATE_SUB7}"											\
	"${ITEM_SIM_PATH}"											\
	"${ITEM_CTR_PATH}"											\
