#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

# date.
ITEM_SIM_MODEL_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/itemSimModel"
echo "item sim model: ${ITEM_SIM_MODEL_HDFS_DIR}"

ITEM_SIM_RESULT_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/itemSim"
echo "item sim result: ${ITEM_SIM_RESULT_HDFS_DIR}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	16g											\
	--num-executors	32											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.cf.ItemGraphSimPredict						\
	"${JAR_PATH}"												\
	"${ITEM_SIM_RESULT_HDFS_DIR}"								\
	"${ITEM_SIM_MODEL_HDFS_DIR}"								\

FILE_PATH=`find_latest_file ${ITEM_SIM_RESULT_HDFS_DIR} ${CUR_DATE} 10`
echo "${FILE_PATH} exists"

RESULT_DIR_CURRENT_USED="${RESULT_PATH_PREFIX}/user/digu/itemSimCurrentUsed"
remove_hdfs_file ${RESULT_DIR_CURRENT_USED}
echo "result dir current used: ${RESULT_DIR_CURRENT_USED}"
${HDFS} -cp ${FILE_PATH} ${RESULT_DIR_CURRENT_USED} &

${CURL} "http://10.15.17.31:10850/dumpData?featureName=itemSim&method=local"
#${CURL} "http://10.19.22.49:10850/dumpData?featureName=itemSim&method=local"
#${CURL} "http://10.15.19.20:10850/dumpData?featureName=itemSim&method=local"
#${CURL} "http://10.19.16.30:10850/dumpData?featureName=itemSim&method=local"
#${CURL} "http://10.15.18.40:10850/dumpData?featureName=itemSim&method=local" &

remove_hdfs_file ${ITEM_SIM_RESULT_HDFS_DIR} ${DAY_SUB20}