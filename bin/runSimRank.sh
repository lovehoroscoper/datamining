#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	24g											\
	--num-executors	32											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.ml.simrank.SimRankV2						\
	"${JAR_PATH}"												\