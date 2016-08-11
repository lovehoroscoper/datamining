#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

DATA_DIR="${RESULT_PATH_PREFIX}/user/digu/itemBigraphSim/resultUnionGroup"
ITEM_BIGRAPH_SIM_PATH=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "item bigraph sim path: ${ITEM_BIGRAPH_SIM_PATH}"

DATA_DIR="${RESULT_PATH_PREFIX}/user/digu/itemSim"
ITEM_SIM_PATH=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "item sim path: ${ITEM_SIM_PATH}"

ITEM_SIM_MERGE_RESULT="${RESULT_PATH_PREFIX}/user/digu/itemSimMergePrice"
echo "item sim merge result: ${ITEM_SIM_MERGE_RESULT}"

ITEM_SIM_SEARCH_DUMP_RESULT="${RESULT_PATH_PREFIX}/user/digu/itemSimPriceSearchDump"
echo "item sim search dump result: ${ITEM_SIM_SEARCH_DUMP_RESULT}"
remove_hdfs_file ${ITEM_SIM_SEARCH_DUMP_RESULT}

PRICE_PATH="${RESULT_PATH_PREFIX}/user/bizdata/finalPrice_it"
echo "price path: ${PRICE_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm           							\
	--driver-memory	32g											\
	--num-executors	64											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.cf.ItemBigraphSimMergePrice					\
	"${JAR_PATH}"												\
	"${ITEM_SIM_PATH}"											\
	"${ITEM_BIGRAPH_SIM_PATH}"									\
	"${ITEM_SIM_MERGE_RESULT}"						    	    \
	"${ITEM_SIM_SEARCH_DUMP_RESULT}"						    \
	"${PRICE_PATH}"						                        \

${CURL} "http://dc.algo.service.mogujie.org/action/fieldUpdate/doUpdate?id=157"
#${CURL} "http://algodc_m.service.mogujie.org/dcAction/execTaskInstance?taskId=51"

#DATA_DIR=${ITEM_SIM_MERGE_RESULT}
#for k in $( seq 1 10 )
#do
#	DAY_SUB=`date -d "${CUR_DATE} -${k} day" +"%Y-%m-%d"`
#	FILE_PATH=${DATA_DIR}/${DAY_SUB}
#	${HDFS} -test -e ${FILE_PATH}/"_SUCCESS"
#	if [ $? -eq 0 ] ;then
#    	echo "${FILE_PATH} exists"
#    	break
#	fi
#done
#
#RESULT_DIR_CURRENT_USED="${RESULT_PATH_PREFIX}/user/digu/itemSimCurrentUsed"
#${HDFS} -test -e ${RESULT_DIR_CURRENT_USED}
#if [ $? -eq 0 ] ;then
#    echo "${RESULT_DIR_CURRENT_USED} exists"
#    ${HDFS} -rm -r ${RESULT_DIR_CURRENT_USED}
#fi
#echo "result dir current used: ${RESULT_DIR_CURRENT_USED}"
#${HDFS} -cp ${FILE_PATH} ${RESULT_DIR_CURRENT_USED}
#
#${CURL} "http://10.15.17.31:10850/dumpData?featureName=itemSim&method=local"
#${CURL} "http://10.19.22.49:10850/dumpData?featureName=itemSim&method=local"
#${CURL} "http://10.15.19.20:10850/dumpData?featureName=itemSim&method=local"
#${CURL} "http://10.19.16.30:10850/dumpData?featureName=itemSim&method=local"
#${CURL} "http://10.15.18.40:10850/dumpData?featureName=itemSim&method=local" &

remove_hdfs_file ${ITEM_SIM_MERGE_RESULT} ${DAY_SUB20}