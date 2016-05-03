#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
CUR_DATE=`date  +%Y-%m-%d`
DATA_DIR="/user/digu/itemBigraphSim/resultUnion"
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

# date.
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
	--class com.mgj.cf.ItemGraphSimFeature					    \
	"${JAR_PATH}"												\
	"${ITEM_BIGRAPH_SIM_PATH}"									\
