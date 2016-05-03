#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
START=${DAY_SUB1}
END=${DAY_SUB1}

echo "start date:${START}"
echo "end date:${END}"

N="10"
echo "N:${N}"

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
	--class com.mgj.cf.evaluate.Statistic						\
	"${JAR_PATH}"												\
	"$1"														\
	"${START}"													\
	"${END}"													\
	"${N}"		    											\

