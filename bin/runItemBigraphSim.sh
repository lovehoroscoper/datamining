#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

# date.
START=${DAY_SUB2}
END=${DAY_SUB2}

echo "start_date:${START}"
echo "end_date:${END}"

ITEM_SIM_RESULT_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/itemBigraphSim"
echo "item sim result: ${ITEM_SIM_RESULT_HDFS_DIR}"

ITEM_SIM_RESULT_HDFS_DIR_UNION="${RESULT_PATH_PREFIX}/user/digu/itemBigraphSim/resultUnion"
echo "item sim result union: ${ITEM_SIM_RESULT_HDFS_DIR_UNION}"

ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP="${RESULT_PATH_PREFIX}/user/digu/itemBigraphSim/resultUnionGroup"
echo "item sim result union group: ${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP}"

ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP_GLOBAL_NORMALIZE="${RESULT_PATH_PREFIX}/user/digu/itemBigraphSim/resultUnionGroupGlobalNormalize"
echo "item sim result union group global normalize: ${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP_GLOBAL_NORMALIZE}"

N="7"
echo "number of days: ${N}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm           							\
	--driver-memory	32g											\
	--num-executors	64											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.cf.ItemBigraphSim							\
	"${JAR_PATH}"												\
	"${START}"													\
	"${END}"													\
	"${ITEM_SIM_RESULT_HDFS_DIR}"						    	\

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm           							\
	--driver-memory	16g											\
	--num-executors	64											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.cf.ItemBigraphSimUnion						\
	"${JAR_PATH}"												\
	"${ITEM_SIM_RESULT_HDFS_DIR}"	    						\
	"${ITEM_SIM_RESULT_HDFS_DIR_UNION}"							\
	"${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP}"					\
	"${N}"			                                    		\
	"${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP_GLOBAL_NORMALIZE}"  \

RESULT_DIR_CURRENT_USED="${RESULT_PATH_PREFIX}/user/digu/itemSimCurrentUsed"
remove_hdfs_file ${RESULT_DIR_CURRENT_USED}
echo "result dir current used: ${RESULT_DIR_CURRENT_USED}"

FILE_PATH=`find_latest_file ${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP} ${CUR_DATE} 10`
echo "${FILE_PATH} exists"
${HDFS} -cp ${FILE_PATH} ${RESULT_DIR_CURRENT_USED}

${CURL} "http://10.15.17.31:10850/dumpData?featureName=itemSim&method=local"
${CURL} "http://10.15.18.40:10850/dumpData?featureName=itemSim&method=local" &

remove_hdfs_file ${ITEM_SIM_RESULT_HDFS_DIR} ${DAY_SUB20}
remove_hdfs_file ${ITEM_SIM_RESULT_HDFS_DIR_UNION} ${DAY_SUB20}
remove_hdfs_file ${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP} ${DAY_SUB20}