#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

ITER_NUM="30"
BETA="1"
PIT_NUM="500"
MU="10"
MODEL_HDFS_DIR="/user/digu/shopDelivery"
INPUT_HDFS_DIR="/user/digu/userShopPreferSub"
remove_hdfs_file ${MODEL_HDFS_DIR}

echo "iteration number: ${ITER_NUM}"
echo "beta: ${BETA}"
echo "pit number: ${PIT_NUM}"
echo "mu: ${MU}"
echo "model hdfs dir: ${MODEL_HDFS_DIR}"
echo "input hdfs dir: ${INPUT_HDFS_DIR}"

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