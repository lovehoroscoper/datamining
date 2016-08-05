#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

USER_HISTORY_ITEM_PREFER_HDFS_DIR="/user/digu/userHistoryItemPrefer"

USER_HISTORY_ITEM_PREFER_GROUP_HDFS_DIR="/user/digu/userHistoryItemPreferGroup"
remove_hdfs_file ${USER_HISTORY_ITEM_PREFER_GROUP_HDFS_DIR}

YESTERDAY=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d %H:%M:%S"`
START=${DAY_SUB7}
END=${DAY_SUB1}

echo "start_date:${START}"
echo "end_date:${END}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	7g											\
	--num-executors	10											\
	--executor-cores 4											\
	--executor-memory 7g										\
	--class com.mgj.useritemprefer.UserItemHistoryPrefer		\
	"${JAR_PATH}"												\
	"${YESTERDAY}"												\
	"${START}"													\
	"${END}"													\
	"${USER_HISTORY_ITEM_PREFER_HDFS_DIR}"						\
	"${USER_HISTORY_ITEM_PREFER_GROUP_HDFS_DIR}"				\

#${CURL} "10.15.2.114:12000/Offline?featureName=userHistoryClickPrefer" &
#${CURL} "10.17.36.57:12000/Offline?featureName=userHistoryClickPrefer" &
#${CURL} "10.17.36.58:12000/Offline?featureName=userHistoryClickPrefer" &
#${CURL} "10.11.8.53:12000/Offline?featureName=userHistoryClickPrefer" &

# remove record
remove_hdfs_file ${USER_HISTORY_ITEM_PREFER_HDFS_DIR} ${DAY_SUB20}
echo "record sub path: ${USER_HISTORY_ITEM_PREFER_HDFS_DIR}/${DAY_SUB20}"