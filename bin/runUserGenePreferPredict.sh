#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

USER_GENE_PREFER_HDFS_DIR="/user/digu/userGenePrefer"
remove_hdfs_file ${USER_GENE_PREFER_HDFS_DIR}

USER_GENE_PREFER_ORDER_HDFS_DIR="/user/digu/userGenePreferOrder"
remove_hdfs_file ${USER_GENE_PREFER_ORDER_HDFS_DIR}

GENE_DIR_SUB="/user/digu/itemGroupCurrentUsed/data"
echo "gene dir sub: ${GENE_DIR_SUB}"

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
	curl "http://10.15.17.31:10850/dumpData?featureName=item_gene,userGenePrefer,userGenePreferOrder&method=localList"
#	curl "http://10.15.17.31:10850/dumpData?featureName=item_gene&method=local"
#	curl "http://10.15.17.31:10850/dumpData?featureName=userGenePrefer&method=local"
#	curl "http://10.15.17.31:10850/dumpData?featureName=userGenePreferOrder&method=local"

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

	curl "http://10.15.18.40:10850/dumpData?featureName=item_gene&method=local" &
	curl "http://10.15.18.40:10850/dumpData?featureName=userGenePrefer&method=local" &
	curl "http://10.15.18.40:10850/dumpData?featureName=userGenePreferOrder&method=local" &

    # put record
	RECORD_PATH="${USER_GENE_PREFER_HDFS_DIR}Record"
	put_record ${USER_GENE_PREFER_HDFS_DIR} ${RECORD_PATH} ${CUR_DATE}
    echo "record path: ${RECORD_PATH}"

    # remove record
    remove_hdfs_file ${RECORD_PATH} ${DAY_SUB20}
    echo "record sub path: ${RECORD_PATH}/${DAY_SUB20}"

    # put record
    RECORD_PATH="${USER_GENE_PREFER_ORDER_HDFS_DIR}Record"
    put_record ${USER_GENE_PREFER_ORDER_HDFS_DIR} ${RECORD_PATH} ${CUR_DATE}
    echo "record path: ${RECORD_PATH}"

    # remove record
    remove_hdfs_file ${RECORD_PATH} ${DAY_SUB20}
    echo "record sub path: ${RECORD_PATH}/${DAY_SUB20}"

    # put record
    RECORD_PATH="/user/digu/itemGeneRecord"
    put_record ${GENE_DIR_SUB} ${RECORD_PATH} ${CUR_DATE}

    # remove record
    remove_hdfs_file ${RECORD_PATH} ${DAY_SUB20}
    echo "record sub path: ${RECORD_PATH}/${DAY_SUB20}"
fi

echo "remove success tag"
test -e ${SUCCESS_TAG}
if [ $? -eq 0 ];then
	rm ${SUCCESS_TAG}
fi