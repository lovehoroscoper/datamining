#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

BIZDATE=${DAY_SUB1}
echo "bizdate: ${BIZDATE}"

BIZDATE_SUB1=${DAY_SUB2}
echo "bizdate_sub1: ${BIZDATE_SUB1}"

BIZDATE_SUB30=${DAY_SUB31}
echo "bizdate_sub30: ${BIZDATE_SUB30}"

#DATA_DIR="/user/digu/itemGroupWithTitle/data"
#FILE_PATH=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
#
#RESULT_DIR_CURRENT_USED="/user/digu/itemGroupCurrentUsed/data"
#remove_hdfs_file ${RESULT_DIR_CURRENT_USED}
#echo "result dir current used: ${RESULT_DIR_CURRENT_USED}"
#${HDFS} -cp ${FILE_PATH} ${RESULT_DIR_CURRENT_USED}

RESULT_DIR_CURRENT_USED="/user/digu/itemGroupCurrentUsed/data"

# click,order,favor,add_cart
FEATURE_TYPE_LIST="click,order,favor,add_cart"
echo "feature type list: ${FEATURE_TYPE_LIST}"

# click,order
SAMPLE_TYPE_LIST="click"
echo "sample type list: ${SAMPLE_TYPE_LIST}"

ENTITY="shop_id"
echo "entity: ${ENTITY}"

ENTITY_FEATURE_NAME=""
echo "ENTITY_FEATURE_NAME: ${ENTITY_FEATURE_NAME}"

ENTITY_TABLE_NAME=""
echo "ENTITY_TABLE_NAME: ${ENTITY_TABLE_NAME}"

ENTITY_MAP_PATH=""
#ENTITY_MAP_PATH=${RESULT_DIR_CURRENT_USED}
echo "entity map path: ${ENTITY_MAP_PATH}"

ENTITY_SIM_PATH=""
echo "entity sim path: ${ENTITY_SIM_PATH}"

SAMPLE_LIST="s_dg_user_prefer_test"
echo "sample list path: ${SAMPLE_LIST}"

MODEL_LIST="/user/test/testModel"
echo "model list path: ${MODEL_LIST}"

PREDICT_BIZDATE=${DAY_SUB1}
echo "predict bizdate: ${PREDICT_BIZDATE}"

PREDICT_BIZDATE_SUB=${DAY_SUB3}
echo "predict bizdate_sub: ${PREDICT_BIZDATE_SUB}"

PREDICT_RESULT_LIST="/user/test/testResult"
echo "predict result list: ${PREDICT_RESULT_LIST}"

PREDICT_TABLE_LIST="s_dg_test_result"
echo "predict result list: ${PREDICT_TABLE_LIST}"

FEATURE_NAME_LIST="test_feature"
echo "feature name list: ${FEATURE_NAME_LIST}"

SUCCESS_TAG="/home/digu/isTestSuccess"
echo "success tag: ${SUCCESS_TAG}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	32											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.userprefer.UserPrefer				        \
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB30}"											\
	"${BIZDATE_SUB1}"											\
	"${FEATURE_TYPE_LIST}"										\
	"${SAMPLE_TYPE_LIST}"										\
	"${ENTITY}"											        \
	"${ENTITY_FEATURE_NAME}"								    \
	"${ENTITY_TABLE_NAME}"									    \
	"${ENTITY_MAP_PATH}"										\
	"${ENTITY_SIM_PATH}"										\
	"${SAMPLE_LIST}"											\
	"${MODEL_LIST}"										    	\
	"${PREDICT_BIZDATE}"										\
	"${PREDICT_BIZDATE_SUB}"								    \
	"${PREDICT_RESULT_LIST}"									\
	"${PREDICT_TABLE_LIST}"										\
	"${FEATURE_NAME_LIST}"										\
	"${SUCCESS_TAG}"				    						\

test -e ${SUCCESS_TAG}
if [ $? -eq 0 ];then
	echo "predict success"
#	${CURL} "http://10.15.17.31:10850/dumpData?featureName=item_gene,userGenePrefer,userGenePreferOrder&method=localList"
#
#	${CURL} "http://10.15.18.40:10850/dumpData?featureName=item_gene&method=local" &
#	${CURL} "http://10.15.18.40:10850/dumpData?featureName=userGenePrefer&method=local" &
#	${CURL} "http://10.15.18.40:10850/dumpData?featureName=userGenePreferOrder&method=local" &

    # put record
#	RECORD_PATH="${USER_GENE_PREFER_HDFS_DIR}Record"
#	put_record ${USER_GENE_PREFER_HDFS_DIR} ${RECORD_PATH} ${CUR_DATE}
#    echo "record path: ${RECORD_PATH}"

    # remove record
#    remove_hdfs_file ${RECORD_PATH} ${DAY_SUB20}
#    echo "record sub path: ${RECORD_PATH}/${DAY_SUB20}"

    # put record
#    RECORD_PATH="${USER_GENE_PREFER_ORDER_HDFS_DIR}Record"
#    put_record ${USER_GENE_PREFER_ORDER_HDFS_DIR} ${RECORD_PATH} ${CUR_DATE}
#    echo "record path: ${RECORD_PATH}"

    # remove record
#    remove_hdfs_file ${RECORD_PATH} ${DAY_SUB20}
#    echo "record sub path: ${RECORD_PATH}/${DAY_SUB20}"

    # put record
#    RECORD_PATH="/user/digu/itemGeneRecord"
#    put_record ${GENE_DIR_SUB} ${RECORD_PATH} ${CUR_DATE}

    # remove record
#    remove_hdfs_file ${RECORD_PATH} ${DAY_SUB20}
#    echo "record sub path: ${RECORD_PATH}/${DAY_SUB20}"
fi

echo "remove success tag"
test -e ${SUCCESS_TAG}
if [ $? -eq 0 ];then
	rm ${SUCCESS_TAG}
fi
