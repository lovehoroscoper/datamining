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
DAY_SUB15=`date -d "${CUR_DATE} -15 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
START=${DAY_SUB2}
END=${DAY_SUB2}

echo "start_date:${START}"
echo "end_date:${END}"

ITEM_SIM_RESULT_HDFS_DIR="/user/digu/itemBigraphSim"
echo "item sim result: ${ITEM_SIM_RESULT_HDFS_DIR}"

ITEM_SIM_RESULT_HDFS_DIR_UNION="/user/digu/itemBigraphSim/resultUnion"
echo "item sim result union: ${ITEM_SIM_RESULT_HDFS_DIR_UNION}"

ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP="/user/digu/itemBigraphSim/resultUnionGroup"
echo "item sim result union group: ${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP}"

ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP_GLOBAL_NORMALIZE="/user/digu/itemBigraphSim/resultUnionGroupGlobalNormalize"
echo "item sim result union group global normalize: ${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP_GLOBAL_NORMALIZE}"

N="7"
echo "number of days: ${N}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm           							\
	--driver-memory	32g											\
	--num-executors	64											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.cf.ItemBigraphSim							\
	"${JAR_PATH}"												\
	"${START}"													\
	"${END}"													\
	"${ITEM_SIM_RESULT_HDFS_DIR}"						    	\

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm           							\
	--driver-memory	16g											\
	--num-executors	64											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.cf.ItemBigraphSimUnion						\
	"${JAR_PATH}"												\
	"${ITEM_SIM_RESULT_HDFS_DIR}"	    						\
	"${ITEM_SIM_RESULT_HDFS_DIR_UNION}"							\
	"${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP}"					\
	"${N}"			                                    		\
	"${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP_GLOBAL_NORMALIZE}"  \

DATA_DIR=${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP}
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

RESULT_DIR_SUB=${ITEM_SIM_RESULT_HDFS_DIR_UNION}/${DAY_SUB20}
hdfs dfs -test -e ${RESULT_DIR_SUB}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_SUB} exists"
    hdfs dfs -rm -r ${RESULT_DIR_SUB}
fi

RESULT_DIR_SUB=${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP}/${DAY_SUB20}
hdfs dfs -test -e ${RESULT_DIR_SUB}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_SUB} exists"
    hdfs dfs -rm -r ${RESULT_DIR_SUB}
fi