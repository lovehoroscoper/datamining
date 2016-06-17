#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

START=${DAY_SUB7}
END=${DAY_SUB1}

MODEL_HDFS_DIR="/user/digu/shopWord2VecModel"
CLUSTER_FILE_HDFS_DIR="/user/digu/shopCluster"
VECTOR_SIZE="50"
MIN_COUNT="10"
CLUSTER_NUM="500"
NUM_PARTITIONS="5"
ENTITY_TYPE="shop_id"
BUFFER_SIZE="128m"

remove_hdfs_file ${MODEL_HDFS_DIR}
remove_hdfs_file ${CLUSTER_FILE_HDFS_DIR}

echo "vector size: ${VECTOR_SIZE}"
echo "min count: ${MIN_COUNT}"
echo "cluster number: ${CLUSTER_NUM}"
echo "number of partitions: ${NUM_PARTITIONS}"
echo "entity type: ${ENTITY_TYPE}"
echo "buffer size: ${BUFFER_SIZE}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	32g											\
	--num-executors	32											\
	--executor-cores 4											\
	--executor-memory 7373m										\
	--class com.mgj.ml.w2v.Train								\
	"${JAR_PATH}"												\
	"${MODEL_HDFS_DIR}"											\
	"${CLUSTER_FILE_HDFS_DIR}"									\
	"${START}"													\
	"${END}"													\
	"${VECTOR_SIZE}"											\
	"${MIN_COUNT}"												\
	"${CLUSTER_NUM}"											\
	"${NUM_PARTITIONS}"											\
	"${ENTITY_TYPE}"											\
	"${BUFFER_SIZE}"											\