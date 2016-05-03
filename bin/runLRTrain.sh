#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	16g											\
	--num-executors	16											\
	--executor-cores 4											\
	--executor-memory 7g										\
	--class com.mgj.ml.lr.Train									\
	"${JAR_PATH}"												\