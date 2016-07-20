#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

USER_GENE_PREFER_MODEL_HDFS_DIR="${1}"
remove_hdfs_file ${USER_GENE_PREFER_MODEL_HDFS_DIR}
echo "user gene prefer model: ${USER_GENE_PREFER_MODEL_HDFS_DIR}"

USER_GENE_PREFER_FEATURE_TABLE="${2}"
echo "user gene prefer feature table: ${USER_GENE_PREFER_FEATURE_TABLE}"

#USER_GENE_PREFER_MODEL_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/userGenePreferModel"
#${HDFS} -test -e ${USER_GENE_PREFER_MODEL_HDFS_DIR}
#if [ $? -eq 0 ] ;then
#    echo "${USER_GENE_PREFER_MODEL_HDFS_DIR} exists"
#    ${HDFS} -rm -r ${USER_GENE_PREFER_MODEL_HDFS_DIR}
#fi
#
#USER_GENE_PREFER_ORDER_MODEL_HDFS_DIR="${RESULT_PATH_PREFIX}/user/digu/userGenePreferOrderModel"
#${HDFS} -test -e ${USER_GENE_PREFER_ORDER_MODEL_HDFS_DIR}
#if [ $? -eq 0 ] ;then
#    echo "${USER_GENE_PREFER_ORDER_MODEL_HDFS_DIR} exists"
#    ${HDFS} -rm -r ${USER_GENE_PREFER_ORDER_MODEL_HDFS_DIR}
#fi

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.usergeneperfer.Train						\
	"${JAR_PATH}"												\
	"${USER_GENE_PREFER_MODEL_HDFS_DIR}"						\
	"${USER_GENE_PREFER_FEATURE_TABLE}"	        				\


