#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	32g											\
	--num-executors	32											\
	--executor-cores 8											\
	--executor-memory 7373m										\
	--class com.mgj.usershopprefer.UserRealShopPrefer			\
	"${JAR_PATH}"												\