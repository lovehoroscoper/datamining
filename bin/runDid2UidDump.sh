#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

# input table.
OUTPUT_PATH="/user/digu/did2uid"
remove_hdfs_file ${OUTPUT_PATH}
echo "output path: ${OUTPUT_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.data   										\
	--driver-memory	4g											\
	--num-executors	8											\
	--executor-cores 1											\
	--executor-memory 4g										\
	--class com.mgj.bizdata.Did2UidDump         				\
	"${JAR_PATH}"												\
	"${OUTPUT_PATH}"											\

#${CURL} "http://10.15.17.31:10850/dumpData?featureName=did2uid&method=local"
#${CURL} "http://10.15.18.40:10850/dumpData?featureName=did2uid&method=local" &