#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

USER_HISTORY_ITEM_PREFER_HDFS_DIR="/user/digu/userHistoryItemPrefer"

USER_HISTORY_ITEM_PREFER_GROUP_HDFS_DIR="/user/digu/userHistoryItemPreferGroup"
hdfs dfs -test -e ${USER_HISTORY_ITEM_PREFER_GROUP_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_HISTORY_ITEM_PREFER_GROUP_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_HISTORY_ITEM_PREFER_GROUP_HDFS_DIR}
fi

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB3=`date -d "${CUR_DATE} -3 day" +"%Y-%m-%d"`
DAY_SUB5=`date -d "${CUR_DATE} -3 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
DAY_SUB15=`date -d "${CUR_DATE} -15 day" +"%Y-%m-%d"`
YESTERDAY=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d %H:%M:%S"`
START=${DAY_SUB7}
END=${DAY_SUB1}

echo "start_date:${START}"
echo "end_date:${END}"

# input table.
SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

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

curl "10.15.2.114:12000/Offline?featureName=userHistoryClickPrefer" &
curl "10.17.36.57:12000/Offline?featureName=userHistoryClickPrefer" &
curl "10.17.36.58:12000/Offline?featureName=userHistoryClickPrefer" &
curl "10.11.8.53:12000/Offline?featureName=userHistoryClickPrefer" &
