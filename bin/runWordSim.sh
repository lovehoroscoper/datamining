#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

INPUT_PATH="${RESULT_PATH_PREFIX}/user/digu/userWordPrefer"
echo "input path: ${INPUT_PATH}"

OUTPUT_PATH="${RESULT_PATH_PREFIX}/user/digu/wordSim"
echo "output path: ${OUTPUT_PATH}"

OUTPUT_GROUP_PATH="${RESULT_PATH_PREFIX}/user/digu/wordSimGroup"
echo "output group path: ${OUTPUT_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	16g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.ml.nlp.WordSim  							\
	"${JAR_PATH}"												\
	"${INPUT_PATH}"												\
	"${OUTPUT_PATH}"											\
	"${OUTPUT_GROUP_PATH}"										\

remove_hdfs_file ${OUTPUT_GROUP_PATH} ${DAY_SUB20}
remove_hdfs_file ${OUTPUT_PATH} ${DAY_SUB20}