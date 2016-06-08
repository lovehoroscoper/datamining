#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
ITEM_SIM_MODEL_HDFS_DIR="/user/digu/itemSimModel"
echo "item sim model: ${ITEM_SIM_MODEL_HDFS_DIR}"

ITEM_SIM_RESULT_HDFS_DIR="/user/digu/itemSim"
echo "item sim result: ${ITEM_SIM_RESULT_HDFS_DIR}"

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
	--class com.mgj.cf.ItemGraphSimPredict						\
	"${JAR_PATH}"												\
	"${ITEM_SIM_RESULT_HDFS_DIR}"								\
	"${ITEM_SIM_MODEL_HDFS_DIR}"								\

DATA_DIR=${ITEM_SIM_RESULT_HDFS_DIR}
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

RESULT_DIR_CURRENT_USED="/user/digu/itemSimCurrentUsed"
hdfs dfs -test -e ${RESULT_DIR_CURRENT_USED}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_CURRENT_USED} exists"
    hdfs dfs -rm -r ${RESULT_DIR_CURRENT_USED}
fi
echo "result dir current used: ${RESULT_DIR_CURRENT_USED}"
hdfs dfs -cp ${FILE_PATH} ${RESULT_DIR_CURRENT_USED}

curl "http://10.15.17.31:10850/dumpData?featureName=itemSim&method=local"
#curl "http://10.19.22.49:10850/dumpData?featureName=itemSim&method=local"
#curl "http://10.15.19.20:10850/dumpData?featureName=itemSim&method=local"
#curl "http://10.19.16.30:10850/dumpData?featureName=itemSim&method=local"
#curl "http://10.15.18.40:10850/dumpData?featureName=itemSim&method=local" &

CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB20=`date -d "${CUR_DATE} -20 day" +"%Y-%m-%d"`
RESULT_DIR_SUB=${ITEM_SIM_RESULT_HDFS_DIR}/${DAY_SUB20}
hdfs dfs -test -e ${RESULT_DIR_SUB}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_SUB} exists"
    hdfs dfs -rm -r ${RESULT_DIR_SUB}
fi
