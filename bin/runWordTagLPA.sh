#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

WORD_TAG_TRAIN="${RESULT_PATH_PREFIX}/user/digu/wordTagTrain"
echo "word tag train: ${WORD_TAG_TRAIN}"

DATA_DIR="${RESULT_PATH_PREFIX}/user/digu/wordSim"
WORD_SIM=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "word sim: ${WORD_SIM}"

TAG_TYPE="产品类型-简单,产品类型-统称,产品类型-复合,产品类型修饰词,产品-品牌,产品-型号"
echo "tag type:${TAG_TYPE}"

OUTPUT_PATH="${RESULT_PATH_PREFIX}/user/digu/wordTag"
echo "output path: ${OUTPUT_PATH}"

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

remove_hdfs_file ${OUTPUT_PATH} ${DAY_SUB20}