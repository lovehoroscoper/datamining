#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
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
hdfs dfs -test -e ${RESULT_DIR}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR} exists"
    hdfs dfs -rm -r ${RESULT_DIR}
fi
echo "result dir: ${RESULT_DIR}"

RESULT_IDF_DIR="/user/digu/queryIdf"
echo "result dir: ${RESULT_IDF_DIR}"

CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -2 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
BIZDATE=${DAY_SUB1}
BIZDATE_SUB=${DAY_SUB1}

echo "bizdate:${BIZDATE}"
echo "bizdate_sub:${BIZDATE_SUB}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

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

DAY_SUB20=`date -d "${CUR_DATE} -20 day" +"%Y-%m-%d"`
RESULT_DIR_SUB=${RESULT_IDF_DIR}/${DAY_SUB20}
hdfs dfs -test -e ${RESULT_DIR_SUB}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_SUB} exists"
    hdfs dfs -rm -r ${RESULT_DIR_SUB}
fi
