#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB2=`date -d "${CUR_DATE} -2 day" +"%Y-%m-%d"`
DAY_SUB3=`date -d "${CUR_DATE} -3 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
START=${DAY_SUB2}
END=${DAY_SUB1}

ITEM_SIM_MODEL_HDFS_DIR="/user/digu/itemSimModel"
hdfs dfs -test -e ${ITEM_SIM_MODEL_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${ITEM_SIM_MODEL_HDFS_DIR} exists"
    hdfs dfs -rm -r ${ITEM_SIM_MODEL_HDFS_DIR}
fi
echo "item sim model: ${ITEM_SIM_MODEL_HDFS_DIR}"

echo "start_date:${START}"
echo "end_date:${END}"

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
	--class com.mgj.cf.ItemGraphSimSample						\
	"${JAR_PATH}"												\
	"${START}"													\
	"${END}"													\
	"${ITEM_SIM_MODEL_HDFS_DIR}"								\
