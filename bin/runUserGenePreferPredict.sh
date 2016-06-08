#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

USER_GENE_PREFER_HDFS_DIR="/user/digu/userGenePrefer"
hdfs dfs -test -e ${USER_GENE_PREFER_HDFS_DIR}
if [ $? -eq 0 ] ;then
    echo "${USER_GENE_PREFER_HDFS_DIR} exists"
    hdfs dfs -rm -r ${USER_GENE_PREFER_HDFS_DIR}
fi

USER_GENE_PREFER_ORDER_HDFS_DIR="/user/digu/userGenePreferOrder"
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
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
BIZDATE=${DAY_SUB1}
BIZDATE_SUB30=${DAY_SUB30}

echo "bizdate:${BIZDATE}"
echo "bizdate_sub30:${BIZDATE_SUB30}"

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
	"${BIZDATE_SUB30}"											\
	"${USER_GENE_PREFER_HDFS_DIR}"								\
	"${USER_GENE_PREFER_ORDER_HDFS_DIR}"						\
	"${GENE_DIR_SUB}"											\
	"${USER_GENE_PREFER_MODEL_HDFS_DIR}"						\
	"${USER_GENE_PREFER_ORDER_MODEL_HDFS_DIR}"					\
	"${SUCCESS_TAG}"											\

test -e ${SUCCESS_TAG}
if [ $? -eq 0 ];then
	echo "predict success"
	curl "http://10.15.17.31:10850/dumpData?featureName=item_gene&method=local"
	curl "http://10.15.17.31:10850/dumpData?featureName=userGenePrefer&method=local"
	curl "http://10.15.17.31:10850/dumpData?featureName=userGenePreferOrder&method=local"

#	curl "http://10.19.22.49:10850/dumpData?featureName=item_gene&method=local"
#	curl "http://10.19.22.49:10850/dumpData?featureName=userGenePrefer&method=local"
#	curl "http://10.19.22.49:10850/dumpData?featureName=userGenePreferOrder&method=local"
#
#	curl "http://10.15.19.20:10850/dumpData?featureName=item_gene&method=local"
#	curl "http://10.15.19.20:10850/dumpData?featureName=userGenePrefer&method=local"
#	curl "http://10.15.19.20:10850/dumpData?featureName=userGenePreferOrder&method=local"
#
#    curl "http://10.19.16.30:10850/dumpData?featureName=item_gene&method=local"
#	curl "http://10.19.16.30:10850/dumpData?featureName=userGenePrefer&method=local"
#	curl "http://10.19.16.30:10850/dumpData?featureName=userGenePreferOrder&method=local"

#	curl "http://10.15.18.40:10850/dumpData?featureName=item_gene&method=local" &
#	curl "http://10.15.18.40:10850/dumpData?featureName=userGenePrefer&method=local" &
#	curl "http://10.15.18.40:10850/dumpData?featureName=userGenePreferOrder&method=local" &

    # date and date sub 20
	CUR_DATE=`date  +%Y-%m-%d`
	DAY_SUB20=`date -d "${CUR_DATE} -20 day" +"%Y-%m-%d"`

    # put record
	RECORD_PATH="${USER_GENE_PREFER_HDFS_DIR}Record"
    echo "record path: ${RECORD_PATH}"
    hdfs dfs -test -e ${RECORD_PATH}
    if [ $? -eq 0 ] ;then
        echo "${RECORD_PATH} exists"
    else
        hdfs dfs -mkdir ${RECORD_PATH}
    fi
    hdfs dfs -cp ${USER_GENE_PREFER_HDFS_DIR} ${RECORD_PATH}/${CUR_DATE} &

    # remove record
    RECORD_SUB_PATH="${RECORD_PATH}/${DAY_SUB20}"
    echo "record sub path: ${RECORD_SUB_PATH}"
    hdfs dfs -test -e ${RECORD_SUB_PATH}
    if [ $? -eq 0 ] ;then
        echo "${RECORD_SUB_PATH} exists"
        hdfs dfs -rm -r ${RECORD_SUB_PATH}
    fi

    # put record
    RECORD_PATH="${USER_GENE_PREFER_ORDER_HDFS_DIR}Record"
    echo "record path: ${RECORD_PATH}"
    hdfs dfs -test -e ${RECORD_PATH}
    if [ $? -eq 0 ] ;then
        echo "${RECORD_PATH} exists"
    else
        hdfs dfs -mkdir ${RECORD_PATH}
    fi
    hdfs dfs -cp ${USER_GENE_PREFER_ORDER_HDFS_DIR} ${RECORD_PATH}/${CUR_DATE} &

    # remove record
    RECORD_SUB_PATH="${RECORD_PATH}/${DAY_SUB20}"
    echo "record sub path: ${RECORD_SUB_PATH}"
    hdfs dfs -test -e ${RECORD_SUB_PATH}
    if [ $? -eq 0 ] ;then
        echo "${RECORD_SUB_PATH} exists"
        hdfs dfs -rm -r ${RECORD_SUB_PATH}
    fi

    # put record
    RECORD_PATH="/user/digu/itemGeneRecord"
    echo "record path: ${RECORD_PATH}"
    hdfs dfs -test -e ${RECORD_PATH}
    if [ $? -eq 0 ] ;then
        echo "${RECORD_PATH} exists"
    else
        hdfs dfs -mkdir ${RECORD_PATH}
    fi
    hdfs dfs -cp ${GENE_DIR_SUB} ${RECORD_PATH}/${CUR_DATE} &

    # remove record
    RECORD_SUB_PATH="${RECORD_PATH}/${DAY_SUB20}"
    echo "record sub path: ${RECORD_SUB_PATH}"
    hdfs dfs -test -e ${RECORD_SUB_PATH}
    if [ $? -eq 0 ] ;then
        echo "${RECORD_SUB_PATH} exists"
        hdfs dfs -rm -r ${RECORD_SUB_PATH}
    fi
fi

echo "remove success tag"
test -e ${SUCCESS_TAG}
if [ $? -eq 0 ];then
	rm ${SUCCESS_TAG}
fi