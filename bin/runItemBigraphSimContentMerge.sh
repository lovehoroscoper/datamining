#!/bin/bash

#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

DATA_DIR="/user/digu/itemBigraphSim/resultUnionGroupGlobalNormalize"
ITEM_BIGRAPH_SIM_UNION_PATH=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "item bigraph sim union path: ${ITEM_BIGRAPH_SIM_UNION_PATH}"

DATA_DIR="/user/digu/wordSim"
WORD_SIM=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "word sim path: ${WORD_SIM}"

DATA_DIR="/user/digu/queryIdf"
QUERY_IDF=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "query idf path: ${QUERY_IDF}"

DATA_DIR="/user/digu/wordTag"
WORD_TAG=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "word tag path: ${WORD_TAG}"

DATA_DIR="/user/digu/itemSimGlobalNormalize"
ITEM_SIM_PATH=`find_latest_file ${DATA_DIR} ${CUR_DATE} 10`
echo "item sim path: ${ITEM_SIM_PATH}"

DICT_PATH="/home/digu/workspace/data/dict"
if [ -f "${DICT_PATH}" ]; then
    # rm ${DATA_PATH}
    echo "${DICT_PATH} exits"
else
    ${HDFS} -get /user/digu/dict ${DICT_PATH}
    for line in `cat ${DICT_PATH}`
    do
        echo -e ${line}'\t1'
    done > ${DICT_PATH}_temp

    cat ${DICT_PATH}_temp > ${DICT_PATH}
    rm ${DICT_PATH}_temp
fi

head ${DICT_PATH}
echo "dict path: ${DICT_PATH}"

ITEM_SIM_MERGE_RESULT="/user/digu/itemSimContentMerge"
echo "item sim merge result: ${ITEM_SIM_MERGE_RESULT}"

ITEM_SIM_MERGE_ORIGIN_RESULT="/user/digu/itemSimContentMergeOrigin"
echo "item sim merge origin result: ${ITEM_SIM_MERGE_ORIGIN_RESULT}"

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
	"${ITEM_SIM_MERGE_ORIGIN_RESULT}"					        \

# dump to rec sys
#TODAY=`date  +%Y%m%d`
#REC_HDFS_PATH="/user/fst/algo/recommend_sys/rec_data/${TODAY}/graph_digu_rec_v2"
#echo "rec hdfs path: ${REC_HDFS_PATH}"
#DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
#echo "day sub:${DAY_SUB1}"
#TEMP_LOCAL_FILE="temp_local_file"
#${HDFS} -getmerge ${ITEM_SIM_MERGE_RESULT}/${DAY_SUB1} ${TEMP_LOCAL_FILE}
#${HDFS} -put ${TEMP_LOCAL_FILE} ${REC_HDFS_PATH}
#rm ${TEMP_LOCAL_FILE}
#curl "10.11.6.179:10849/sendAlgo?type=algoWithOutMap&date=${TODAY}&algoName=graph_digu_rec_v2"
#echo "success"

# dump to rank sys
FILE_PATH=`find_latest_file ${ITEM_SIM_MERGE_RESULT} ${CUR_DATE} 10`
echo "${FILE_PATH} exists"

RESULT_DIR_CURRENT_USED="/user/digu/itemSimCurrentUsedV2"
remove_hdfs_file ${RESULT_DIR_CURRENT_USED}
echo "result dir current used: ${RESULT_DIR_CURRENT_USED}"
${HDFS} -cp ${FILE_PATH} ${RESULT_DIR_CURRENT_USED}

curl "http://10.15.17.31:10850/dumpData?featureName=itemSimV2&method=local"
#curl "http://10.19.22.49:10850/dumpData?featureName=itemSimV2&method=local"
#curl "http://10.15.19.20:10850/dumpData?featureName=itemSimV2&method=local"
#curl "http://10.19.16.30:10850/dumpData?featureName=itemSimV2&method=local"
curl "http://10.15.18.40:10850/dumpData?featureName=itemSimV2&method=local" &

remove_hdfs_file ${ITEM_SIM_MERGE_RESULT} ${DAY_SUB20}
remove_hdfs_file ${ITEM_SIM_MERGE_ORIGIN_RESULT} ${DAY_SUB20}