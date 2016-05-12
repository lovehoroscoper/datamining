#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB3=`date -d "${CUR_DATE} -3 day" +"%Y-%m-%d"`
DAY_SUB5=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
START=${DAY_SUB7}
END=${DAY_SUB1}

echo "start_date:${START}"
echo "end_date:${END}"

ITEM_SIM_RESULT_HDFS_DIR="/user/digu/itemSim"
echo "item sim result: ${ITEM_SIM_RESULT_HDFS_DIR}"

ITEM_SIM_GLOBAL_NORMALIZE_RESULT_HDFS_DIR="/user/digu/itemSimGlobalNormalize"
echo "item sim global normalize result: ${ITEM_SIM_GLOBAL_NORMALIZE_RESULT_HDFS_DIR}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	16g											\
	--num-executors	32											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.cf.ItemGraphSimRawFeature					\
	"${JAR_PATH}"												\
	"${START}"													\
	"${END}"													\
	"${ITEM_SIM_RESULT_HDFS_DIR}"								\
	"${ITEM_SIM_GLOBAL_NORMALIZE_RESULT_HDFS_DIR}"				\


CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB20=`date -d "${CUR_DATE} -20 day" +"%Y-%m-%d"`
RESULT_DIR_SUB=${ITEM_SIM_RESULT_HDFS_DIR}/${DAY_SUB20}
hdfs dfs -test -e ${RESULT_DIR_SUB}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_SUB} exists"
    hdfs dfs -rm -r ${RESULT_DIR_SUB}
fi

RESULT_DIR_SUB=${ITEM_SIM_GLOBAL_NORMALIZE_RESULT_HDFS_DIR}/${DAY_SUB20}
hdfs dfs -test -e ${RESULT_DIR_SUB}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_SUB} exists"
    hdfs dfs -rm -r ${RESULT_DIR_SUB}
fi