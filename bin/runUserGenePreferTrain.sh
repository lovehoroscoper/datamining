#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

USER_GENE_PREFER_MODEL_HDFS_DIR="${1}"
hdfs dfs -test -e ${USER_GENE_PREFER_MODEL_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_GENE_PREFER_MODEL_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_GENE_PREFER_MODEL_HDFS_DIR}
fi
echo "user gene prefer model: ${USER_GENE_PREFER_MODEL_HDFS_DIR}"

USER_GENE_PREFER_FEATURE_TABLE="${2}"
echo "user gene prefer feature table: ${USER_GENE_PREFER_FEATURE_TABLE}"

#USER_GENE_PREFER_MODEL_HDFS_DIR="/user/digu/userGenePreferModel"
#hdfs dfs -test -e ${USER_GENE_PREFER_MODEL_HDFS_DIR}
#if [ $? -eq 0 ] ;then
#    echo "${USER_GENE_PREFER_MODEL_HDFS_DIR} exists"
#    hdfs dfs -rm -r ${USER_GENE_PREFER_MODEL_HDFS_DIR}
#fi
#
#USER_GENE_PREFER_ORDER_MODEL_HDFS_DIR="/user/digu/userGenePreferOrderModel"
#hdfs dfs -test -e ${USER_GENE_PREFER_ORDER_MODEL_HDFS_DIR}
#if [ $? -eq 0 ] ;then
#    echo "${USER_GENE_PREFER_ORDER_MODEL_HDFS_DIR} exists"
#    hdfs dfs -rm -r ${USER_GENE_PREFER_ORDER_MODEL_HDFS_DIR}
#fi

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

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


