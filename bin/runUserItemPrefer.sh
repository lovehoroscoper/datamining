#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

# date.
BIZDATE=${DAY_SUB1}
BIZDATE_SUB1=${DAY_SUB2}
BIZDATE_SUB7=${DAY_SUB7}

echo "bizdate: ${BIZDATE}"
echo "bizdate_sub1: ${BIZDATE_SUB1}"
echo "bizdate_sub7: ${BIZDATE_SUB7}"

DATA_DIR="/user/digu/itemSimMerge"
ITEM_SIM_PATH=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "item sim path: ${ITEM_SIM_PATH}"

ITEM_CTR_PATH="/user/bizdata/ctrRecord/${DAY_SUB2}"
echo "item ctr path: ${ITEM_CTR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm   									\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.useritemprefer.UserItemPrefer				\
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB1}"											\
	"${BIZDATE_SUB7}"											\
	"${ITEM_SIM_PATH}"											\
	"${ITEM_CTR_PATH}"											\
