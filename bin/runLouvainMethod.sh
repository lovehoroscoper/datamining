#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

YESTERDAY=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d %H:%M:%S"`

RESULT_DIR="${RESULT_PATH_PREFIX}/user/digu/modularityWithTitle"
remove_hdfs_file ${RESULT_DIR}
echo "result dir: ${RESULT_DIR}"

DATA_DIR="${RESULT_PATH_PREFIX}/user/digu/itemBigraphSim/resultUnionGroup"
INPUT_DIR=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "input dir: ${INPUT_DIR}"

DATA_DIR="${RESULT_PATH_PREFIX}/user/digu/itemSim"
INPUT_DIR_SUPPLEMENT=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "input dir supplement: ${INPUT_DIR_SUPPLEMENT}"

N="50"
echo "N: ${N}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	32											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.ml.louvain.Main								\
	"${JAR_PATH}"												\
	"${RESULT_DIR}"												\
	"${INPUT_DIR}"												\
	"${INPUT_DIR_SUPPLEMENT}"									\
	"${N}"								                    	\