#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

RESULT_DIR="/user/digu/pageRankScore"
remove_hdfs_file ${RESULT_DIR}
echo "result dir: ${RESULT_DIR}"

INPUT_DIR="/user/digu/itemGraphDiff"
echo "input dir: ${INPUT_DIR}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.ml.pagerank.PageRank						\
	"${JAR_PATH}"												\
	"${RESULT_DIR}"												\
	"${INPUT_DIR}"												\