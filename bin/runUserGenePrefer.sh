#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

BIZDATE=${DAY_SUB1}
BIZDATE_SUB1=${DAY_SUB2}
BIZDATE_SUB30=${DAY_SUB31}

echo "bizdate: ${BIZDATE}"
echo "bizdate_sub1: ${BIZDATE_SUB1}"
echo "bizdate_sub30: ${BIZDATE_SUB30}"

DATA_DIR="/user/digu/itemGroupWithTitle/data"
FILE_PATH=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`

RESULT_DIR_CURRENT_USED="/user/digu/itemGroupCurrentUsed/data"
remove_hdfs_file ${RESULT_DIR_CURRENT_USED}
echo "result dir current used: ${RESULT_DIR_CURRENT_USED}"
${HDFS} -cp ${FILE_PATH} ${RESULT_DIR_CURRENT_USED}

GENE_DIR_SUB=${RESULT_DIR_CURRENT_USED}
echo "gene dir: ${GENE_DIR_SUB}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	32											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.usergeneperfer.UserGenePrefer				\
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB1}"											\
	"${BIZDATE_SUB30}"											\
	"${GENE_DIR_SUB}"											\
