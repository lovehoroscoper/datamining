#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

USER_ITEM_PREFER_MODEL_HDFS_DIR="/user/digu/userItemPreferModel"
remove_hdfs_file ${USER_ITEM_PREFER_MODEL_HDFS_DIR}

USER_ITEM_PREFER_ORDER_MODEL_HDFS_DIR="/user/digu/userItemPreferOrderModel"
remove_hdfs_file ${USER_ITEM_PREFER_ORDER_MODEL_HDFS_DIR}

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