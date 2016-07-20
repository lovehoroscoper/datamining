#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`

DATA_DIR="${RESULT_PATH_PREFIX}/user/digu/itemBigraphSim/resultUnionGroupGlobalNormalize"
ITEM_BIGRAPH_SIM_UNION_PATH=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "item bigraph sim union path: ${ITEM_BIGRAPH_SIM_UNION_PATH}"

DATA_DIR="${RESULT_PATH_PREFIX}/user/digu/wordSim"
WORD_SIM=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "word sim path: ${WORD_SIM}"

DATA_DIR="${RESULT_PATH_PREFIX}/user/digu/queryIdf"
QUERY_IDF=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "query idf path: ${QUERY_IDF}"

DATA_DIR="${RESULT_PATH_PREFIX}/user/digu/wordTag"
WORD_TAG=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "word tag path: ${WORD_TAG}"

DATA_DIR="${RESULT_PATH_PREFIX}/user/digu/itemSimGlobalNormalize"
ITEM_SIM_PATH=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "item sim path: ${ITEM_SIM_PATH}"

DICT_PATH="/home/digu/workspace/data/dict"
if [ -f "${DICT_PATH}" ]; then
    # rm ${DATA_PATH}
    echo "${DICT_PATH} exits"
else
    ${HDFS} -get ${RESULT_PATH_PREFIX}/user/digu/dict ${DICT_PATH}
    for line in `cat ${DICT_PATH}`
    do
        echo -e ${line}'\t1'
    done > ${DICT_PATH}_temp

    cat ${DICT_PATH}_temp > ${DICT_PATH}
    rm ${DICT_PATH}_temp
fi

head ${DICT_PATH}
echo "dict path: ${DICT_PATH}"

ITEM_SIM_MERGE_RESULT="${RESULT_PATH_PREFIX}/user/digu/itemSimContentMerge"
echo "item sim merge result: ${ITEM_SIM_MERGE_RESULT}"

ITEM_BIGRAPH_SIM_CONTENT_MERGE_PATH="${ITEM_SIM_MERGE_RESULT}/partA"
echo "item bigraph sim content merge path: ${ITEM_BIGRAPH_SIM_CONTENT_MERGE_PATH}"
remove_hdfs_file ${ITEM_BIGRAPH_SIM_CONTENT_MERGE_PATH}

ITEM_SIM_CONTENT_MERGE_PATH="${ITEM_SIM_MERGE_RESULT}/partB"
echo "item sim content merge path: ${ITEM_SIM_CONTENT_MERGE_PATH}"
remove_hdfs_file ${ITEM_SIM_CONTENT_MERGE_PATH}

W1="1e6"
echo "w1:${W1}"

W2="200"
echo "w2:${W2}"

W3="10"
echo "w3:${W3}"

T1="0"
echo "t1:${T1}"

T2="0.005"
echo "t2:${T2}"

T3="0"
echo "t3:${T3}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm           							\
	--driver-memory	32g											\
	--num-executors	64											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.cf.content.ItemSimContentMerge  	        \
	"${JAR_PATH}"												\
	"${ITEM_BIGRAPH_SIM_UNION_PATH}"					        \
	"${ITEM_SIM_PATH}"							                \
	"${WORD_SIM}"											    \
	"${QUERY_IDF}"						    	                \
	"${DICT_PATH}"						    	                \
	"${WORD_TAG}"						    	                \
	"${ITEM_SIM_MERGE_RESULT}"		                		    \
	"${W1}"					                                    \
	"${W2}"					                                    \
	"${W3}"					                                    \
	"${T1}"					                                    \
	"${T2}"					                                    \
	"${T3}"					                                    \