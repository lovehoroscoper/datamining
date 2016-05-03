#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

ITEM_CF_INVERTED_HDFS_DIR="/user/digu/itemCFInverted"
hdfs dfs -test -e ${ITEM_CF_INVERTED_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${ITEM_CF_INVERTED_HDFS_DIR} exists"
    hdfs dfs -rm -r ${ITEM_CF_INVERTED_HDFS_DIR}
fi

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`

# input table.
SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

# dump to redis
${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	7g											\
	--num-executors	10											\
	--executor-cores 4											\
	--executor-memory 7g										\
	--class com.mgj.cf.dump.ItemCFInvertedDump					\
	"${JAR_PATH}"												\
	"${DAY_SUB30}"												\
	"${DAY_SUB1}"												\

#curl "http://10.11.2.170:10021/ranking/feature/update?featureName=dv_array_score_userbehaviorscore&featureHdfsPath=/user/digu/itemCFInverted"