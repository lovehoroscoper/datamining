#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

USER_ITEM_PREFER_MODEL_HDFS_DIR="/user/digu/userItemPreferModel"
hdfs dfs -test -e ${USER_ITEM_PREFER_MODEL_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_ITEM_PREFER_MODEL_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_ITEM_PREFER_MODEL_HDFS_DIR}
fi

USER_ITEM_PREFER_ORDER_MODEL_HDFS_DIR="/user/digu/userItemPreferOrderModel"
hdfs dfs -test -e ${USER_ITEM_PREFER_ORDER_MODEL_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_ITEM_PREFER_ORDER_MODEL_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_ITEM_PREFER_ORDER_MODEL_HDFS_DIR}
fi

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm   									\
	--driver-memory	16g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.useritemprefer.Train						\
	"${JAR_PATH}"												\
	"${USER_ITEM_PREFER_MODEL_HDFS_DIR}"						\
	"${USER_ITEM_PREFER_ORDER_MODEL_HDFS_DIR}"					\