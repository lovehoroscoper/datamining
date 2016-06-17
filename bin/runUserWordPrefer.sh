#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

DATA_PATH="/home/digu/workspace/data/dict"
if [ -f "${DATA_PATH}" ]; then
    # rm ${DATA_PATH}
    echo "${DATA_PATH} exits"
else
    hdfs dfs -get /user/digu/dict ${DATA_PATH}
    for line in `cat ${DATA_PATH}`
    do
        echo -e ${line}'\t1'
    done > ${DATA_PATH}_temp

    cat ${DATA_PATH}_temp > ${DATA_PATH}
    rm ${DATA_PATH}_temp
fi

head ${DATA_PATH}

echo "data path: ${DATA_PATH}"

RESULT_DIR="/user/digu/userWordPrefer"
remove_hdfs_file ${RESULT_DIR}
echo "result dir: ${RESULT_DIR}"

RESULT_IDF_DIR="/user/digu/queryIdf"
echo "result dir: ${RESULT_IDF_DIR}"

BIZDATE=${DAY_SUB1}
BIZDATE_SUB=${DAY_SUB1}

echo "bizdate:${BIZDATE}"
echo "bizdate_sub:${BIZDATE_SUB}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	16g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.ml.nlp.UserWordPrefer						\
	"${JAR_PATH}"												\
	"${DATA_PATH}"												\
	"${RESULT_DIR}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB}"											\
	"${RESULT_IDF_DIR}"											\

remove_hdfs_file ${RESULT_IDF_DIR} ${DAY_SUB20}