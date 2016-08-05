#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

USER_SHOP_PREFER_HDFS_DIR="/user/digu/userShopPrefer"
remove_hdfs_file ${USER_SHOP_PREFER_HDFS_DIR}

USER_SHOP_PREFER_ORDER_HDFS_DIR="/user/digu/userShopPreferOrder"
remove_hdfs_file ${USER_SHOP_PREFER_ORDER_HDFS_DIR}

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
	--class com.mgj.usershopprefer.Predict						\
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB30}"											\
	"${USER_SHOP_PREFER_HDFS_DIR}"								\
	"${USER_SHOP_PREFER_ORDER_HDFS_DIR}"						\

#${CURL} "http://10.15.17.31:10850/dumpData?featureName=userShopPrefer&method=local"
#${CURL} "http://10.15.17.31:10850/dumpData?featureName=userShopPreferOrder&method=local"

#${CURL} "http://10.15.18.40:10850/dumpData?featureName=userShopPrefer&method=local" &
#${CURL} "http://10.15.18.40:10850/dumpData?featureName=userShopPreferOrder&method=local" &

#${CURL} "10.15.2.114:12000/Offline?featureName=userShopPrefer" &
#${CURL} "10.17.36.57:12000/Offline?featureName=userShopPrefer" &
#${CURL} "10.17.36.58:12000/Offline?featureName=userShopPrefer" &
#${CURL} "10.11.8.53:12000/Offline?featureName=userShopPrefer"
#
#${CURL} "10.15.2.114:12000/Offline?featureName=userShopPreferOrder" &
#${CURL} "10.17.36.57:12000/Offline?featureName=userShopPreferOrder" &
#${CURL} "10.17.36.58:12000/Offline?featureName=userShopPreferOrder" &
#${CURL} "10.11.8.53:12000/Offline?featureName=userShopPreferOrder" &

# put record
RECORD_PATH="${USER_SHOP_PREFER_HDFS_DIR}Record"
echo "record path: ${RECORD_PATH}"
put_record ${USER_SHOP_PREFER_HDFS_DIR} ${RECORD_PATH} ${CUR_DATE}

# remove record
remove_hdfs_file ${RECORD_PATH} ${DAY_SUB20}
echo "record sub path: ${RECORD_PATH}/${DAY_SUB20}"

# put record
RECORD_PATH="${USER_SHOP_PREFER_ORDER_HDFS_DIR}Record"
echo "record path: ${RECORD_PATH}"
put_record ${USER_SHOP_PREFER_ORDER_HDFS_DIR} ${RECORD_PATH} ${CUR_DATE}

# remove record
remove_hdfs_file ${RECORD_PATH} ${DAY_SUB20}
echo "record sub path: ${RECORD_PATH}/${DAY_SUB20}"