#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

USER_CATEGORY_PREFER_MODEL_HDFS_DIR="${1}"
remove_hdfs_file ${USER_CATEGORY_PREFER_MODEL_HDFS_DIR}
echo "user category prefer model: ${USER_CATEGORY_PREFER_MODEL_HDFS_DIR}"

USER_CATEGORY_PREFER_FEATURE_TABLE="${2}"
echo "user category prefer feature table: ${USER_CATEGORY_PREFER_FEATURE_TABLE}"

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

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm   									\
	--driver-memory	8g											\
	--num-executors 16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.usercategoryprefer.Train					\
	"${JAR_PATH}"												\
	"${USER_CATEGORY_PREFER_MODEL_HDFS_DIR}"					\
	"${USER_CATEGORY_PREFER_FEATURE_TABLE}"						\


