#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

USER_GENE_PREFER_HDFS_DIR="/user/digu/userGenePreferSub"
hdfs dfs -test -e ${USER_GENE_PREFER_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_GENE_PREFER_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_GENE_PREFER_HDFS_DIR}
fi

USER_GENE_PREFER_ORDER_HDFS_DIR="/user/digu/userGenePreferOrderSub"
hdfs dfs -test -e ${USER_GENE_PREFER_ORDER_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_GENE_PREFER_ORDER_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_GENE_PREFER_ORDER_HDFS_DIR}
fi

GENE_DIR_SUB="/user/digu/itemGroupCurrentUsed/data"
echo "gene dir sub: ${GENE_DIR_SUB}"

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB2=`date -d "${CUR_DATE} -2 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
DAY_SUB15=`date -d "${CUR_DATE} -15 day" +"%Y-%m-%d"`
DAY_SUB16=`date -d "${CUR_DATE} -16 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
DAY_SUB31=`date -d "${CUR_DATE} -31 day" +"%Y-%m-%d"`
BIZDATE=${DAY_SUB2}
BIZDATE_SUB=${DAY_SUB31}

echo "bizdate:${BIZDATE}"
echo "bizdate_sub:${BIZDATE_SUB}"

USER_GENE_PREFER_MODEL_HDFS_DIR="/user/digu/userGenePreferModel"
echo "user gene prefer model path: ${USER_GENE_PREFER_MODEL_HDFS_DIR}"

USER_GENE_PREFER_ORDER_MODEL_HDFS_DIR="/user/digu/userGenePreferOrderModel"
echo "user gene prefer order model path: ${USER_GENE_PREFER_ORDER_MODEL_HDFS_DIR}"

SUCCESS_TAG="/home/digu/isSuccess"
echo "success tag: ${SUCCESS_TAG}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	32											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.usergeneperfer.Predict						\
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB}"											\
	"${USER_GENE_PREFER_HDFS_DIR}"								\
	"${USER_GENE_PREFER_ORDER_HDFS_DIR}"						\
	"${GENE_DIR_SUB}"											\
	"${USER_GENE_PREFER_MODEL_HDFS_DIR}"						\
	"${USER_GENE_PREFER_ORDER_MODEL_HDFS_DIR}"					\
	"${SUCCESS_TAG}"											\

echo "remove success tag"
test -e ${SUCCESS_TAG}
if [ $? -eq 0 ];then
	rm ${SUCCESS_TAG}
fi