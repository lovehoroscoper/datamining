#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

USER_ITEM_PREFER_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/userItemPrefer"
remove_hdfs_file ${USER_ITEM_PREFER_HDFS_DIR}

USER_ITEM_PREFER_ORDER_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/userItemPreferOrder"
remove_hdfs_file ${USER_ITEM_PREFER_ORDER_HDFS_DIR}

BIZDATE=${DAY_SUB1}
BIZDATE_SUB1=${DAY_SUB2}
BIZDATE_SUB7=${DAY_SUB7}

echo "bizdate: ${BIZDATE}"
echo "bizdate_sub1: ${BIZDATE_SUB1}"
echo "bizdate_sub7: ${BIZDATE_SUB7}"

DATA_DIR="${RESULT_PATH_PREFIX}/user/digu/itemSimMerge"
ITEM_SIM_PATH=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "item sim path: ${ITEM_SIM_PATH}"

USER_ITEM_PREFER_MODEL_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/userItemPreferModel"
echo "user item prefer model path: ${USER_ITEM_PREFER_MODEL_HDFS_DIR}"

USER_ITEM_PREFER_ORDER_MODEL_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/userItemPreferOrderModel"
echo "user item prefer order model path: ${USER_ITEM_PREFER_ORDER_MODEL_HDFS_DIR}"

ITEM_CTR_PATH="${RESULT_PATH_PREFIX}/user/bizdata/ctrRecord/${DAY_SUB1}"
echo "item ctr path: ${ITEM_CTR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm   									\
	--driver-memory	16g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.useritemprefer.Predict						\
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB7}"											\
	"${ITEM_SIM_PATH}"											\
	"${USER_ITEM_PREFER_MODEL_HDFS_DIR}"						\
	"${USER_ITEM_PREFER_HDFS_DIR}"								\
	"${ITEM_CTR_PATH}"											\

#${CURL} "http://10.15.17.31:10850/dumpData?featureName=userItemPrefer&method=local"
#${CURL} "http://10.15.17.31:10850/dumpData?featureName=userItemPreferOrder&method=local"

#${CURL} "http://10.15.18.40:10850/dumpData?featureName=userItemPrefer&method=local" &
#${CURL} "http://10.15.18.40:10850/dumpData?featureName=userItemPreferOrder&method=local" &

#${CURL} "10.15.2.114:12000/Offline?featureName=userItemPrefer" &
#${CURL} "10.17.36.57:12000/Offline?featureName=userItemPrefer" &
#${CURL} "10.17.36.58:12000/Offline?featureName=userItemPrefer" &
#${CURL} "10.11.8.53:12000/Offline?featureName=userItemPrefer" &

# put record
RECORD_PATH="${USER_ITEM_PREFER_HDFS_DIR}Record"
echo "record path: ${RECORD_PATH}"
put_record ${USER_ITEM_PREFER_HDFS_DIR} ${RECORD_PATH} ${CUR_DATE}

# remove record
remove_hdfs_file ${RECORD_PATH} ${DAY_SUB20}
echo "record sub path: ${RECORD_PATH}/${DAY_SUB20}"