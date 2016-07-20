#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

USER_CATEGORY_PREFER_HDFS_DIR="/user/digu/userGBPrefer"
remove_hdfs_file ${USER_CATEGORY_PREFER_HDFS_DIR}

USER_CATEGORY_PREFER_ORDER_HDFS_DIR="/user/digu/userGBPreferOrder"
remove_hdfs_file ${USER_CATEGORY_PREFER_ORDER_HDFS_DIR}

BIZDATE=${DAY_SUB1}
BIZDATE_SUB30=${DAY_SUB30}

echo "bizdate:${BIZDATE}"
echo "bizdate_sub30:${BIZDATE_SUB30}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm   									\
	--driver-memory	8g											\
	--num-executors	32											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.usercategoryprefer.Predict					\
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB30}"											\
	"${USER_CATEGORY_PREFER_HDFS_DIR}"							\
	"${USER_CATEGORY_PREFER_ORDER_HDFS_DIR}"					\

#${CURL} "http://10.15.17.31:10850/dumpData?featureName=userShopPrefer&method=local"
#${CURL} "http://10.15.17.31:10850/dumpData?featureName=userShopPreferOrder&method=local"
#
#${CURL} "http://10.19.22.49:10850/dumpData?featureName=userShopPrefer&method=local"
#${CURL} "http://10.19.22.49:10850/dumpData?featureName=userShopPreferOrder&method=local"
#
#${CURL} "http://10.15.19.20:10850/dumpData?featureName=userShopPrefer&method=local"
#${CURL} "http://10.15.19.20:10850/dumpData?featureName=userShopPreferOrder&method=local"
#
#${CURL} "http://10.19.16.30:10850/dumpData?featureName=userShopPrefer&method=local"
#${CURL} "http://10.19.16.30:10850/dumpData?featureName=userShopPreferOrder&method=local"
#
#${CURL} "http://10.15.18.40:10850/dumpData?featureName=userShopPrefer&method=local" &
#${CURL} "http://10.15.18.40:10850/dumpData?featureName=userShopPreferOrder&method=local" &
#
#${CURL} "http://10.17.109.31:10850/dumpData?featureName=userShopPrefer&method=local" &
#${CURL} "http://10.17.109.31:10850/dumpData?featureName=userShopPreferOrder&method=local" &
#
#${CURL} "http://10.17.109.32:10850/dumpData?featureName=userShopPrefer&method=local" &
#${CURL} "http://10.17.109.32:10850/dumpData?featureName=userShopPreferOrder&method=local" &

${CURL} "10.15.2.114:12000/Offline?featureName=userGBPrefer" &
${CURL} "10.17.36.57:12000/Offline?featureName=userGBPrefer" &
${CURL} "10.17.36.58:12000/Offline?featureName=userGBPrefer" &
${CURL} "10.11.8.53:12000/Offline?featureName=userGBPrefer"

${CURL} "10.15.2.114:12000/Offline?featureName=userGBPreferOrder" &
${CURL} "10.17.36.57:12000/Offline?featureName=userGBPreferOrder" &
${CURL} "10.17.36.58:12000/Offline?featureName=userGBPreferOrder" &
${CURL} "10.11.8.53:12000/Offline?featureName=userGBPreferOrder" &

# put record
RECORD_PATH="${USER_CATEGORY_PREFER_HDFS_DIR}Record"
put_record ${USER_CATEGORY_PREFER_HDFS_DIR} ${RECORD_PATH} ${CUR_DATE}
echo "record path: ${RECORD_PATH}"

# remove record
remove_hdfs_file ${RECORD_PATH} ${DAY_SUB20}
echo "record sub path: ${RECORD_PATH}/${DAY_SUB20}"

# put record
RECORD_PATH="${USER_CATEGORY_PREFER_ORDER_HDFS_DIR}Record"
put_record ${USER_CATEGORY_PREFER_ORDER_HDFS_DIR} ${RECORD_PATH} ${CUR_DATE}
echo "record path: ${RECORD_PATH}"

# remove record
remove_hdfs_file ${RECORD_PATH} ${DAY_SUB20}
echo "record sub path: ${RECORD_PATH}/${DAY_SUB20}"