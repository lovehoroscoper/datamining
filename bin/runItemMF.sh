#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

INPUT_PATH="/user/digu/userItemPrefer"
echo "input path ${INPUT_PATH}"

OUTPUT_PATH="/user/digu/itemMF"
remove_hdfs_file ${OUTPUT_PATH}
echo "output path ${OUTPUT_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm   									\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.ml.mf.ItemMF								\
	"${JAR_PATH}"												\
	"${INPUT_PATH}"												\
	"${OUTPUT_PATH}"											\