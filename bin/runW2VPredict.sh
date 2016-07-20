#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

RESULT_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/shopSimRank"
MODEL_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/shopWord2VecModel"
CLUSTER_FILE_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/shopCluster"
ENTITY_TYPE="shop_id"
IS_SCORED="true"
IS_SAME_CATEGORY="true"

remove_hdfs_file ${RESULT_HDFS_DIR}

echo "model: ${MODEL_HDFS_DIR}"
echo "cluster: ${CLUSTER_FILE_HDFS_DIR}"
echo "entity type: ${ENTITY_TYPE}"
echo "is scored: ${IS_SCORED}"
echo "is same category: ${IS_SAME_CATEGORY}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	16g											\
	--num-executors	16											\
	--executor-cores 8											\
	--executor-memory 7373m										\
	--class com.mgj.ml.w2v.Predict								\
	"${JAR_PATH}"												\
	"${MODEL_HDFS_DIR}"											\
	"${CLUSTER_FILE_HDFS_DIR}"									\
	"${RESULT_HDFS_DIR}"										\
	"${ENTITY_TYPE}"											\
	"${IS_SCORED}"												\
	"${IS_SAME_CATEGORY}"										\
