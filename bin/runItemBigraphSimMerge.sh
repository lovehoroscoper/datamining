#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`

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

ITEM_BIGRAPH_SIM_PATH=${FILE_PATH}
echo "item bigraph sim path: ${ITEM_BIGRAPH_SIM_PATH}"

DATA_DIR="/user/digu/itemSim"
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

ITEM_SIM_MERGE_RESULT="/user/digu/itemSimMerge"
echo "item sim merge result: ${ITEM_SIM_MERGE_RESULT}"

ITEM_SIM_SEARCH_DUMP_RESULT="/user/digu/itemSimSearchDump"
echo "item sim search dump result: ${ITEM_SIM_SEARCH_DUMP_RESULT}"
hdfs dfs -test -e ${ITEM_SIM_SEARCH_DUMP_RESULT}
if [ $? -eq 0 ] ;then
    echo "${ITEM_SIM_SEARCH_DUMP_RESULT} exists"
    hdfs dfs -rm -r ${ITEM_SIM_SEARCH_DUMP_RESULT}
fi

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
	--class com.mgj.cf.ItemBigraphSimMerge						\
	"${JAR_PATH}"												\
	"${ITEM_SIM_PATH}"											\
	"${ITEM_BIGRAPH_SIM_PATH}"									\
	"${ITEM_SIM_MERGE_RESULT}"						    	    \
	"${ITEM_SIM_SEARCH_DUMP_RESULT}"						    \

curl "http://dc.algo.service.mogujie.org/action/fieldUpdate/doUpdate?id=85"

#DATA_DIR=${ITEM_SIM_MERGE_RESULT}
#for k in $( seq 1 10 )
#do
#	DAY_SUB=`date -d "${CUR_DATE} -${k} day" +"%Y-%m-%d"`
#	FILE_PATH=${DATA_DIR}/${DAY_SUB}
#	hdfs dfs -test -e ${FILE_PATH}/"_SUCCESS"
#	if [ $? -eq 0 ] ;then
#    	echo "${FILE_PATH} exists"
#    	break
#	fi
#done
#
#RESULT_DIR_CURRENT_USED="/user/digu/itemSimCurrentUsed"
#hdfs dfs -test -e ${RESULT_DIR_CURRENT_USED}
#if [ $? -eq 0 ] ;then
#    echo "${RESULT_DIR_CURRENT_USED} exists"
#    hdfs dfs -rm -r ${RESULT_DIR_CURRENT_USED}
#fi
#echo "result dir current used: ${RESULT_DIR_CURRENT_USED}"
#hdfs dfs -cp ${FILE_PATH} ${RESULT_DIR_CURRENT_USED}
#
#curl "http://10.15.17.31:10850/dumpData?featureName=itemSim&method=local"
#curl "http://10.19.22.49:10850/dumpData?featureName=itemSim&method=local"
#curl "http://10.15.19.20:10850/dumpData?featureName=itemSim&method=local"
#curl "http://10.19.16.30:10850/dumpData?featureName=itemSim&method=local"
#curl "http://10.15.18.40:10850/dumpData?featureName=itemSim&method=local" &

CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB20=`date -d "${CUR_DATE} -20 day" +"%Y-%m-%d"`
RESULT_DIR_SUB=${ITEM_SIM_MERGE_RESULT}/${DAY_SUB20}
hdfs dfs -test -e ${RESULT_DIR_SUB}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_SUB} exists"
    hdfs dfs -rm -r ${RESULT_DIR_SUB}
fi