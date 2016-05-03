#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

ITER_NUM="30"
BETA="1"
PIT_NUM="500"
MU="10"
MODEL_HDFS_DIR="/user/digu/shopDelivery"
INPUT_HDFS_DIR="/user/digu/userShopPreferSub"

hdfs dfs -test -e ${MODEL_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${MODEL_HDFS_DIR} exists"
    hdfs dfs -rm -r ${MODEL_HDFS_DIR}
fi

echo "iteration number: ${ITER_NUM}"
echo "beta: ${BETA}"
echo "pit number: ${PIT_NUM}"
echo "mu: ${MU}"
echo "model hdfs dir: ${MODEL_HDFS_DIR}"
echo "input hdfs dir: ${INPUT_HDFS_DIR}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	16g											\
	--num-executors	16											\
	--executor-cores 8											\
	--executor-memory 7373m										\
	--class com.mgj.usershopprefer.GuaranteeDelivery			\
	"${JAR_PATH}"												\
	"${ITER_NUM}"												\
	"${BETA}"													\
	"${PIT_NUM}"												\
	"${MU}"														\
	"${INPUT_HDFS_DIR}"											\
	"${MODEL_HDFS_DIR}"											\