#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

START=${DAY_SUB2}
END=${DAY_SUB1}

ITEM_SIM_MODEL_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/itemSimModel"
remove_hdfs_file ${ITEM_SIM_MODEL_HDFS_DIR}
echo "item sim model: ${ITEM_SIM_MODEL_HDFS_DIR}"

echo "start_date:${START}"
echo "end_date:${END}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	16g											\
	--num-executors	32											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.cf.ItemGraphSimSample						\
	"${JAR_PATH}"												\
	"${START}"													\
	"${END}"													\
	"${ITEM_SIM_MODEL_HDFS_DIR}"								\
