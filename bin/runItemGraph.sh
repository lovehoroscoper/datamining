#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

DATA_DIR="${RESULT_PATH_PREFIX}/user/digu/itemBigraphSim/resultUnion"
ITEM_BIGRAPH_SIM_PATH=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "item bigraph sim path: ${ITEM_BIGRAPH_SIM_PATH}"

${SUBMIT}														\
	--master yarn   											\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.cf.ItemGraph    							\
	"${JAR_PATH}"												\
	"${ITEM_BIGRAPH_SIM_PATH}"									\

