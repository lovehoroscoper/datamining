#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

USER_ITEM_PREFER_HDFS_DIR="/user/digu/userItemPrefer"
hdfs dfs -test -e ${USER_ITEM_PREFER_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_ITEM_PREFER_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_ITEM_PREFER_HDFS_DIR}
fi

USER_ITEM_PREFER_ORDER_HDFS_DIR="/user/digu/userItemPreferOrder"
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
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
DAY_SUB15=`date -d "${CUR_DATE} -15 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
BIZDATE=${DAY_SUB1}
BIZDATE_SUB1=${DAY_SUB2}
BIZDATE_SUB7=${DAY_SUB7}

echo "bizdate: ${BIZDATE}"
echo "bizdate_sub1: ${BIZDATE_SUB1}"
echo "bizdate_sub7: ${BIZDATE_SUB7}"

DATA_DIR="/user/digu/itemSimMerge"
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

ITEM_CTR_PATH="/user/bizdata/ctrRecord/${DAY_SUB1}"
echo "item ctr path: ${ITEM_CTR_PATH}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

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

curl "http://10.15.17.31:10850/dumpData?featureName=userItemPrefer&method=local"
curl "http://10.15.17.31:10850/dumpData?featureName=userItemPreferOrder&method=local"

#curl "http://10.19.22.49:10850/dumpData?featureName=userItemPrefer&method=local"
#curl "http://10.19.22.49:10850/dumpData?featureName=userItemPreferOrder&method=local"
#
#curl "http://10.15.19.20:10850/dumpData?featureName=userItemPrefer&method=local"
#curl "http://10.15.19.20:10850/dumpData?featureName=userItemPreferOrder&method=local"
#
#curl "http://10.19.16.30:10850/dumpData?featureName=userItemPrefer&method=local"
#curl "http://10.19.16.30:10850/dumpData?featureName=userItemPreferOrder&method=local"
#
#curl "http://10.15.18.40:10850/dumpData?featureName=userItemPrefer&method=local" &
#curl "http://10.15.18.40:10850/dumpData?featureName=userItemPreferOrder&method=local" &

curl "10.15.2.114:12000/Offline?featureName=userItemPrefer" &
curl "10.17.36.57:12000/Offline?featureName=userItemPrefer" &
curl "10.17.36.58:12000/Offline?featureName=userItemPrefer" &
curl "10.11.8.53:12000/Offline?featureName=userItemPrefer" &

# date and date sub 20
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB20=`date -d "${CUR_DATE} -20 day" +"%Y-%m-%d"`

# put record
RECORD_PATH="${USER_ITEM_PREFER_HDFS_DIR}Record"
echo "record path: ${RECORD_PATH}"
hdfs dfs -test -e ${RECORD_PATH}
if [ $? -eq 0 ] ;then
    echo "${RECORD_PATH} exists"
else
    hdfs dfs -mkdir ${RECORD_PATH}
fi
hdfs dfs -cp ${USER_ITEM_PREFER_HDFS_DIR} ${RECORD_PATH}/${CUR_DATE} &

# remove record
RECORD_SUB_PATH="${RECORD_PATH}/${DAY_SUB20}"
echo "record sub path: ${RECORD_SUB_PATH}"
hdfs dfs -test -e ${RECORD_SUB_PATH}
if [ $? -eq 0 ] ;then
    echo "${RECORD_SUB_PATH} exists"
    hdfs dfs -rm -r ${RECORD_SUB_PATH}
fi