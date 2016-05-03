#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
BIZDATE=${DAY_SUB1}
BIZDATE_SUB30=${DAY_SUB30}

echo "bizdate:${BIZDATE}"
echo "bizdate_sub1:${BIZDATE_SUB1}"
echo "bizdate_sub30:${BIZDATE_SUB30}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 8											\
	--executor-memory 7g										\
	--class com.mgj.usershopprefer.ShopAnalysis					\
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB30}"											\

