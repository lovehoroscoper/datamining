#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

# date.
START=${DAY_SUB7}
END=${DAY_SUB1}

echo "start_date:${START}"
echo "end_date:${END}"

ITEM_SIM_RESULT_HDFS_DIR="/user/digu/itemSim"
echo "item sim result: ${ITEM_SIM_RESULT_HDFS_DIR}"

ITEM_SIM_GLOBAL_NORMALIZE_RESULT_HDFS_DIR="/user/digu/itemSimGlobalNormalize"
echo "item sim global normalize result: ${ITEM_SIM_GLOBAL_NORMALIZE_RESULT_HDFS_DIR}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	16g											\
	--num-executors	32											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.cf.ItemGraphSimRawFeature					\
	"${JAR_PATH}"												\
	"${START}"													\
	"${END}"													\
	"${ITEM_SIM_RESULT_HDFS_DIR}"								\
	"${ITEM_SIM_GLOBAL_NORMALIZE_RESULT_HDFS_DIR}"				\

remove_hdfs_file ${ITEM_SIM_RESULT_HDFS_DIR} ${DAY_SUB20}
remove_hdfs_file ${ITEM_SIM_GLOBAL_NORMALIZE_RESULT_HDFS_DIR} ${DAY_SUB20}