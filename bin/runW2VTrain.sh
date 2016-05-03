#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB2=`date -d "${CUR_DATE} -2 day" +"%Y-%m-%d"`
DAY_SUB3=`date -d "${CUR_DATE} -3 day" +"%Y-%m-%d"`
DAY_SUB4=`date -d "${CUR_DATE} -4 day" +"%Y-%m-%d"`
DAY_SUB5=`date -d "${CUR_DATE} -5 day" +"%Y-%m-%d"`
DAY_SUB6=`date -d "${CUR_DATE} -6 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB8=`date -d "${CUR_DATE} -8 day" +"%Y-%m-%d"`
DAY_SUB9=`date -d "${CUR_DATE} -9 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
DAY_SUB11=`date -d "${CUR_DATE} -11 day" +"%Y-%m-%d"`
DAY_SUB12=`date -d "${CUR_DATE} -12 day" +"%Y-%m-%d"`
DAY_SUB13=`date -d "${CUR_DATE} -13 day" +"%Y-%m-%d"`
DAY_SUB14=`date -d "${CUR_DATE} -14 day" +"%Y-%m-%d"`
DAY_SUB15=`date -d "${CUR_DATE} -15 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
DAY_SUB31=`date -d "${CUR_DATE} -31 day" +"%Y-%m-%d"`
START=${DAY_SUB7}
END=${DAY_SUB1}

MODEL_HDFS_DIR="/user/digu/shopWord2VecModel"
CLUSTER_FILE_HDFS_DIR="/user/digu/shopCluster"
VECTOR_SIZE="50"
MIN_COUNT="10"
CLUSTER_NUM="500"
NUM_PARTITIONS="5"
ENTITY_TYPE="shop_id"
BUFFER_SIZE="128m"

hdfs dfs -test -e ${MODEL_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${MODEL_HDFS_DIR} exists"
    hdfs dfs -rm -r ${MODEL_HDFS_DIR}
fi

hdfs dfs -test -e ${CLUSTER_FILE_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${CLUSTER_FILE_HDFS_DIR} exists"
    hdfs dfs -rm -r ${CLUSTER_FILE_HDFS_DIR}
fi

echo "vector size: ${VECTOR_SIZE}"
echo "min count: ${MIN_COUNT}"
echo "cluster number: ${CLUSTER_NUM}"
echo "number of partitions: ${NUM_PARTITIONS}"
echo "entity type: ${ENTITY_TYPE}"
echo "buffer size: ${BUFFER_SIZE}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	32g											\
	--num-executors	32											\
	--executor-cores 4											\
	--executor-memory 7373m										\
	--class com.mgj.ml.w2v.Train								\
	"${JAR_PATH}"												\
	"${MODEL_HDFS_DIR}"											\
	"${CLUSTER_FILE_HDFS_DIR}"									\
	"${START}"													\
	"${END}"													\
	"${VECTOR_SIZE}"											\
	"${MIN_COUNT}"												\
	"${CLUSTER_NUM}"											\
	"${NUM_PARTITIONS}"											\
	"${ENTITY_TYPE}"											\
	"${BUFFER_SIZE}"											\