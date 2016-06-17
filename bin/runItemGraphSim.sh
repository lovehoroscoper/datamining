#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

START=${DAY_SUB7}
END=${DAY_SUB1}

echo "start_date:${START}"
echo "end_date:${END}"

CTR_DIFF="0.2"
echo "ctr different:${CTR_DIFF}"

RESULT_DIR="/user/digu/itemGraphSim"
remove_hdfs_file ${RESULT_DIR}
echo "result dir: ${RESULT_DIR}"

RESULT_DIR_V2="/user/digu/itemGraphSimWithTitle"
remove_hdfs_file ${RESULT_DIR_V2}
echo "result dir v2: ${RESULT_DIR_V2}"

DATA_PATH="/home/digu/workspace/data/new_words_v2.txt"
if [ -f "${DATA_PATH}" ]; then
    rm ${DATA_PATH}
fi
hdfs dfs -get /user/yichen/new_words.txt ${DATA_PATH}

for line in `cat ${DATA_PATH}`
do
echo -e $line'\t1'
done > ${DATA_PATH}_temp

cat ${DATA_PATH}_temp > ${DATA_PATH}
rm ${DATA_PATH}_temp
head ${DATA_PATH}
echo "data path: ${DATA_PATH}"

ITEM_CTR="/user/digu/LTR_FEATURE/old_ctr_score_sub"
echo "item ctr: ${ITEM_CTR}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	32											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.cf.ItemGraphSim								\
	"${JAR_PATH}"												\
	"${START}"													\
	"${END}"													\
	"${RESULT_DIR}"												\
	"${DATA_PATH}"												\
	"${CTR_DIFF}"												\
	"${RESULT_DIR_V2}"											\
	"${ITEM_CTR}"												\

