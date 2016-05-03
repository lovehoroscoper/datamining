#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

RESULT_HDFS_DIR="/user/digu/shopSimRank"
MODEL_HDFS_DIR="/user/digu/shopWord2VecModel"
CLUSTER_FILE_HDFS_DIR="/user/digu/shopCluster"
ENTITY_TYPE="shop_id"
IS_SCORED="true"
IS_SAME_CATEGORY="true"

hdfs dfs -test -e ${RESULT_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${RESULT_HDFS_DIR} exists"
    hdfs dfs -rm -r ${RESULT_HDFS_DIR}
fi

echo "model: ${MODEL_HDFS_DIR}"
echo "cluster: ${CLUSTER_FILE_HDFS_DIR}"
echo "entity type: ${ENTITY_TYPE}"
echo "is scored: ${IS_SCORED}"
echo "is same category: ${IS_SAME_CATEGORY}"

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
	--class com.mgj.ml.w2v.Predict								\
	"${JAR_PATH}"												\
	"${MODEL_HDFS_DIR}"											\
	"${CLUSTER_FILE_HDFS_DIR}"									\
	"${RESULT_HDFS_DIR}"										\
	"${ENTITY_TYPE}"											\
	"${IS_SCORED}"												\
	"${IS_SAME_CATEGORY}"										\
