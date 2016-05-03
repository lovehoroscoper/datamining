#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

USER_ITEM_PREFER_HDFS_DIR="/user/digu/userItemPreferSub"
hdfs dfs -test -e ${USER_ITEM_PREFER_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_ITEM_PREFER_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_ITEM_PREFER_HDFS_DIR}
fi

USER_ITEM_PREFER_ORDER_HDFS_DIR="/user/digu/userItemPreferOrderSub"
hdfs dfs -test -e ${USER_ITEM_PREFER_ORDER_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_ITEM_PREFER_ORDER_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_ITEM_PREFER_ORDER_HDFS_DIR}
fi

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB2=`date -d "${CUR_DATE} -3 day" +"%Y-%m-%d"`
DAY_SUB3=`date -d "${CUR_DATE} -3 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB8=`date -d "${CUR_DATE} -8 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
DAY_SUB15=`date -d "${CUR_DATE} -15 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
BIZDATE=${DAY_SUB2}
BIZDATE_SUB1=${DAY_SUB3}
BIZDATE_SUB7=${DAY_SUB8}

echo "bizdate: ${BIZDATE}"
echo "bizdate_sub1: ${BIZDATE_SUB1}"
echo "bizdate_sub7: ${BIZDATE_SUB7}"

DATA_DIR="/user/digu/itemBigraphSim/resultUnionGroup"
for k in $( seq 1 10 )
do
	DAY_SUB=`date -d "${CUR_DATE} -${k} day" +"%Y-%m-%d"`
	FILE_PATH=${DATA_DIR}/${DAY_SUB}
	hdfs dfs -test -e ${FILE_PATH}/"_SUCCESS"
	if [ $? -eq 0 ] ;then
    	echo "${FILE_PATH} exists"
    	break
	fi
done

ITEM_SIM_PATH=${FILE_PATH}
echo "item sim path: ${ITEM_SIM_PATH}"

USER_ITEM_PREFER_MODEL_HDFS_DIR="/user/digu/userItemPreferModel"
echo "user item prefer model path: ${USER_ITEM_PREFER_MODEL_HDFS_DIR}"

USER_ITEM_PREFER_ORDER_MODEL_HDFS_DIR="/user/digu/userItemPreferOrderModel"
echo "user item prefer order model path: ${USER_ITEM_PREFER_ORDER_MODEL_HDFS_DIR}"

ITEM_CTR_PATH="/user/digu/LTR_FEATURE/old_ctr_score_sub"
echo "item ctr path: ${ITEM_CTR_PATH}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
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