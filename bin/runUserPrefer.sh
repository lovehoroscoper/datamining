#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

BIZDATE=${DAY_SUB1}
BIZDATE_SUB1=${DAY_SUB2}
BIZDATE_SUB30=${DAY_SUB4}

echo "bizdate: ${BIZDATE}"
echo "bizdate_sub1: ${BIZDATE_SUB1}"
echo "bizdate_sub30: ${BIZDATE_SUB30}"

#DATA_DIR="/user/digu/itemGroupWithTitle/data"
#FILE_PATH=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
#
#RESULT_DIR_CURRENT_USED="/user/digu/itemGroupCurrentUsed/data"
#remove_hdfs_file ${RESULT_DIR_CURRENT_USED}
#echo "result dir current used: ${RESULT_DIR_CURRENT_USED}"
#${HDFS} -cp ${FILE_PATH} ${RESULT_DIR_CURRENT_USED}

RESULT_DIR_CURRENT_USED="/user/digu/itemGroupCurrentUsed/data"

# click,order,favor,add_cart
FEATURE_TYPE_LIST="click,order,favor,add_cart"
echo "feature type list: ${FEATURE_TYPE_LIST}"

# click,order
SAMPLE_TYPE_LIST="click"
echo "sample type list: ${SAMPLE_TYPE_LIST}"

ENTITY="to_entity(item_id)"
echo "entity: ${ENTITY}"

ENTITY_MAP_PATH=${RESULT_DIR_CURRENT_USED}
echo "entity map path: ${ENTITY_MAP_PATH}"

ENTITY_SIM_PATH=""
echo "entity sim path: ${ENTITY_SIM_PATH}"

SAMPLE_LIST="s_dg_user_prefer_test"
echo "sample list path: ${SAMPLE_LIST}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	32											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.userprefer.UserPrefer				        \
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB30}"											\
	"${BIZDATE_SUB1}"											\
	"${FEATURE_TYPE_LIST}"										\
	"${SAMPLE_TYPE_LIST}"										\
	"${ENTITY}"											        \
	"${ENTITY_MAP_PATH}"										\
	"${ENTITY_SIM_PATH}"										\
	"${SAMPLE_LIST}"											\


