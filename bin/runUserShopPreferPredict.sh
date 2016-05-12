#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

USER_SHOP_PREFER_HDFS_DIR="/user/digu/userShopPrefer"
hdfs dfs -test -e ${USER_SHOP_PREFER_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_SHOP_PREFER_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_SHOP_PREFER_HDFS_DIR}
fi

USER_SHOP_PREFER_ORDER_HDFS_DIR="/user/digu/userShopPreferOrder"
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
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
BIZDATE=${DAY_SUB1}
BIZDATE_SUB30=${DAY_SUB30}

echo "bizdate:${BIZDATE}"
echo "bizdate_sub30:${BIZDATE_SUB30}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.data   										\
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

curl "http://10.15.17.31:10850/dumpData?featureName=userShopPrefer&method=local"
curl "http://10.15.17.31:10850/dumpData?featureName=userShopPreferOrder&method=local"

curl "http://10.19.22.49:10850/dumpData?featureName=userShopPrefer&method=local"
curl "http://10.19.22.49:10850/dumpData?featureName=userShopPreferOrder&method=local"

curl "http://10.15.19.20:10850/dumpData?featureName=userShopPrefer&method=local"
curl "http://10.15.19.20:10850/dumpData?featureName=userShopPreferOrder&method=local"

curl "http://10.19.16.30:10850/dumpData?featureName=userShopPrefer&method=local"
curl "http://10.19.16.30:10850/dumpData?featureName=userShopPreferOrder&method=local"

curl "http://10.15.18.40:10850/dumpData?featureName=userShopPrefer&method=local" &
curl "http://10.15.18.40:10850/dumpData?featureName=userShopPreferOrder&method=local" &

curl "10.15.2.114:12000/Offline?featureName=userShopPrefer" &
curl "10.17.36.57:12000/Offline?featureName=userShopPrefer" &
curl "10.17.36.58:12000/Offline?featureName=userShopPrefer" &
curl "10.11.8.53:12000/Offline?featureName=userShopPrefer"

curl "10.15.2.114:12000/Offline?featureName=userShopPreferOrder" &
curl "10.17.36.57:12000/Offline?featureName=userShopPreferOrder" &
curl "10.17.36.58:12000/Offline?featureName=userShopPreferOrder" &
curl "10.11.8.53:12000/Offline?featureName=userShopPreferOrder" &
# date and date sub 20
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB20=`date -d "${CUR_DATE} -20 day" +"%Y-%m-%d"`

# put record
RECORD_PATH="${USER_SHOP_PREFER_HDFS_DIR}Record"
echo "record path: ${RECORD_PATH}"
hdfs dfs -test -e ${RECORD_PATH}
if [ $? -eq 0 ] ;then
    echo "${RECORD_PATH} exists"
else
    hdfs dfs -mkdir ${RECORD_PATH}
fi
hdfs dfs -cp ${USER_SHOP_PREFER_HDFS_DIR} ${RECORD_PATH}/${CUR_DATE} &

# remove record
RECORD_SUB_PATH="${RECORD_PATH}/${DAY_SUB20}"
echo "record sub path: ${RECORD_SUB_PATH}"
hdfs dfs -test -e ${RECORD_SUB_PATH}
if [ $? -eq 0 ] ;then
    echo "${RECORD_SUB_PATH} exists"
    hdfs dfs -rm -r ${RECORD_SUB_PATH}
fi

# put record
RECORD_PATH="${USER_SHOP_PREFER_ORDER_HDFS_DIR}Record"
echo "record path: ${RECORD_PATH}"
hdfs dfs -test -e ${RECORD_PATH}
if [ $? -eq 0 ] ;then
    echo "${RECORD_PATH} exists"
else
    hdfs dfs -mkdir ${RECORD_PATH}
fi
hdfs dfs -cp ${USER_SHOP_PREFER_ORDER_HDFS_DIR} ${RECORD_PATH}/${CUR_DATE} &

# remove record
RECORD_SUB_PATH="${RECORD_PATH}/${DAY_SUB20}"
echo "record sub path: ${RECORD_SUB_PATH}"
hdfs dfs -test -e ${RECORD_SUB_PATH}
if [ $? -eq 0 ] ;then
    echo "${RECORD_SUB_PATH} exists"
    hdfs dfs -rm -r ${RECORD_SUB_PATH}
fi