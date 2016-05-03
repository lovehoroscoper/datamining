#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

USER_SHOP_PREFER_MODEL_HDFS_DIR="${1}"
hdfs dfs -test -e ${USER_SHOP_PREFER_MODEL_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_SHOP_PREFER_MODEL_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_SHOP_PREFER_MODEL_HDFS_DIR}
fi
echo "user shop prefer model: ${USER_SHOP_PREFER_MODEL_HDFS_DIR}"

USER_SHOP_PREFER_FEATURE_TABLE="${2}"
echo "user shop prefer feature table: ${USER_SHOP_PREFER_FEATURE_TABLE}"

#USER_SHOP_PREFER_MODEL_HDFS_DIR="/user/digu/userShopPreferModel"
#hdfs dfs -test -e ${USER_SHOP_PREFER_MODEL_HDFS_DIR}
#if [ $? -eq 0 ] ;then
#    echo "${USER_SHOP_PREFER_MODEL_HDFS_DIR} exists"
#    hdfs dfs -rm -r ${USER_SHOP_PREFER_MODEL_HDFS_DIR}
#fi
#
#USER_SHOP_PREFER_ORDER_MODEL_HDFS_DIR="/user/digu/userShopPreferOrderModel"
#hdfs dfs -test -e ${USER_SHOP_PREFER_ORDER_MODEL_HDFS_DIR}
#if [ $? -eq 0 ] ;then
#    echo "${USER_SHOP_PREFER_ORDER_MODEL_HDFS_DIR} exists"
#    hdfs dfs -rm -r ${USER_SHOP_PREFER_ORDER_MODEL_HDFS_DIR}
#fi

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.data   										\
	--driver-memory	8g											\
	--num-executors 16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.usershopprefer.Train						\
	"${JAR_PATH}"												\
	"${USER_SHOP_PREFER_MODEL_HDFS_DIR}"						\
	"${USER_SHOP_PREFER_FEATURE_TABLE}"							\


