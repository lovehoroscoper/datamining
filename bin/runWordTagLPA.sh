#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

WORD_TAG_TRAIN="/user/digu/wordTagTrain"
echo "word tag train: ${WORD_TAG_TRAIN}"

CUR_DATE=`date  +%Y-%m-%d`
DATA_DIR="/user/digu/wordSim"
for k in $( seq 1 10 )
do
	DAY_SUB=`date -d "${CUR_DATE} -${k} day" +"%Y-%m-%d"`
	FILE_PATH=${DATA_DIR}/${DAY_SUB}
	hdfs dfs -test -e ${FILE_PATH}/"_SUCCESS"
	if [ $? -eq 0 ] ;then
    	echo "${FILE_PATH} exists"
    	break
	fi
done

WORD_SIM=${FILE_PATH}
echo "word sim: ${WORD_SIM}"

TAG_TYPE="产品类型-简单,产品类型-统称,产品类型-复合,产品类型修饰词,产品-品牌,产品-型号"
echo "tag type:${TAG_TYPE}"

OUTPUT_PATH="/user/digu/wordTag"
echo "output path: ${OUTPUT_PATH}"
#hdfs dfs -test -e ${OUTPUT_PATH}
#if [ $? -eq 0 ] ;then
#    echo "${OUTPUT_PATH} exists"
#    hdfs dfs -rm -r ${OUTPUT_PATH}
#fi

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
	--class com.mgj.ml.nlp.LPA        							\
	"${JAR_PATH}"												\
	"${WORD_TAG_TRAIN}"											\
	"${WORD_SIM}"											    \
	"${OUTPUT_PATH}"										    \
	"${TAG_TYPE}"									    	    \

CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB20=`date -d "${CUR_DATE} -20 day" +"%Y-%m-%d"`

RESULT_DIR_SUB=${OUTPUT_PATH}/${DAY_SUB20}
hdfs dfs -test -e ${RESULT_DIR_SUB}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR_SUB} exists"
    hdfs dfs -rm -r ${RESULT_DIR_SUB}
fi