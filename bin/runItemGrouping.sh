#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

RESULT_DIR="/user/digu/itemGroupWithTitle"
echo "result dir: ${RESULT_DIR}"

INPUT_DIR="/user/digu/modularityWithTitle"
echo "input dir: ${INPUT_DIR}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.ml.louvain.Grouping							\
	"${JAR_PATH}"												\
	"${INPUT_DIR}"												\
	"${RESULT_DIR}"												\

remove_hdfs_file ${RESULT_DIR}/data/${DAY_SUB20}
remove_hdfs_file ${RESULT_DIR}/index/${DAY_SUB20}