#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

ITEM_CF_INVERTED_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/itemCFInverted"
remove_hdfs_file ${ITEM_CF_INVERTED_HDFS_DIR}

# dump to redis
${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	7g											\
	--num-executors	10											\
	--executor-cores 4											\
	--executor-memory 7g										\
	--class com.mgj.cf.dump.ItemCFInvertedDump					\
	"${JAR_PATH}"												\
	"${DAY_SUB30}"												\
	"${DAY_SUB1}"												\