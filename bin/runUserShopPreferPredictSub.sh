#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

USER_SHOP_PREFER_HDFS_DIR="/user/digu/userShopPreferSub"
hdfs dfs -test -e ${USER_SHOP_PREFER_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_SHOP_PREFER_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_SHOP_PREFER_HDFS_DIR}
fi

USER_SHOP_PREFER_ORDER_HDFS_DIR="/user/digu/userShopPreferOrderSub"
hdfs dfs -test -e ${USER_SHOP_PREFER_ORDER_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_SHOP_PREFER_ORDER_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_SHOP_PREFER_ORDER_HDFS_DIR}
fi

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB2=`date -d "${CUR_DATE} -2 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
DAY_SUB15=`date -d "${CUR_DATE} -15 day" +"%Y-%m-%d"`
DAY_SUB16=`date -d "${CUR_DATE} -16 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
DAY_SUB31=`date -d "${CUR_DATE} -31 day" +"%Y-%m-%d"`
BIZDATE=${DAY_SUB2}
BIZDATE_SUB30=${DAY_SUB16}

echo "bizdate:${BIZDATE}"
echo "bizdate_sub30:${BIZDATE_SUB30}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.data   										\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 4											\
	--executor-memory 7373m										\
	--class com.mgj.usershopprefer.Predict						\
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB30}"											\
	"${USER_SHOP_PREFER_HDFS_DIR}"								\
	"${USER_SHOP_PREFER_ORDER_HDFS_DIR}"						\