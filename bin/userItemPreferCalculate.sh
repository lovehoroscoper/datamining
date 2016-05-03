#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
DAY_SUB15=`date -d "${CUR_DATE} -15 day" +"%Y-%m-%d"`
YESTERDAY=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d %H:%M:%S"`
START=${DAY_SUB10}
END=${DAY_SUB1}

echo "start_date:${START}"
echo "end_date:${END}"
echo "${YESTERDAY}"

# input table.
TABLE_NAME="s_dg_cf_user_click_log_7d"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	7g											\
	--num-executors	10											\
	--executor-cores 4											\
	--executor-memory 7g										\
	--class com.mgj.useritemprefer.UserItemPrefer				\
	"${JAR_PATH}"												\
	"${YESTERDAY}"												\
	"${START}"													\
	"${END}"													\

