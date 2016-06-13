#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
DAY_SUB1="2016-06-01"
echo "first day:${DAY_SUB1}"

DAY_SUB2="2016-06-02"
echo "second day:${DAY_SUB2}"

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
	--class com.mgj.utils.PersonlizeAnalysisUtil				\
	"${JAR_PATH}"												\
	"${DAY_SUB1}"												\
	"${DAY_SUB2}"												\