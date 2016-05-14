#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`

DATA_DIR="/user/digu/itemBigraphSim/resultUnionGroupGlobalNormalize"
for k in $( seq 1 10 )
do
	DAY_SUB=`date -d "${CUR_DATE} -${k} day" +"%Y-%m-%d"`
	FILE_PATH=${DATA_DIR}/${DAY_SUB}
	hdfs dfs -test -e ${FILE_PATH}/"_SUCCESS"
	if [ $? -eq 0 ] ;then
    	echo "${FILE_PATH} exists"
    	break
	fi
done

ITEM_BIGRAPH_SIM_UNION_PATH=${FILE_PATH}
echo "item bigraph sim union path: ${ITEM_BIGRAPH_SIM_UNION_PATH}"

DATA_DIR="/user/digu/wordSim"
for k in $( seq 1 10 )
do
	DAY_SUB=`date -d "${CUR_DATE} -${k} day" +"%Y-%m-%d"`
	FILE_PATH=${DATA_DIR}/${DAY_SUB}
	hdfs dfs -test -e ${FILE_PATH}/"_SUCCESS"
	if [ $? -eq 0 ] ;then
    	echo "${FILE_PATH} exists"
    	break
	fi
done

WORD_SIM=${FILE_PATH}
echo "word sim path: ${WORD_SIM}"

DATA_DIR="/user/digu/queryIdf"
for k in $( seq 1 10 )
do
	DAY_SUB=`date -d "${CUR_DATE} -${k} day" +"%Y-%m-%d"`
	FILE_PATH=${DATA_DIR}/${DAY_SUB}
	hdfs dfs -test -e ${FILE_PATH}/"_SUCCESS"
	if [ $? -eq 0 ] ;then
    	echo "${FILE_PATH} exists"
    	break
	fi
done

QUERY_IDF=${FILE_PATH}
echo "query idf path: ${QUERY_IDF}"

DATA_DIR="/user/digu/wordTag"
for k in $( seq 1 10 )
do
	DAY_SUB=`date -d "${CUR_DATE} -${k} day" +"%Y-%m-%d"`
	FILE_PATH=${DATA_DIR}/${DAY_SUB}
	hdfs dfs -test -e ${FILE_PATH}/"_SUCCESS"
	if [ $? -eq 0 ] ;then
    	echo "${FILE_PATH} exists"
    	break
	fi
done

WORD_TAG=${FILE_PATH}
echo "word tag path: ${WORD_TAG}"

DICT_PATH="/home/digu/workspace/data/dict"
if [ -f "${DICT_PATH}" ]; then
    # rm ${DATA_PATH}
    echo "${DICT_PATH} exits"
else
    hdfs dfs -get /user/digu/dict ${DICT_PATH}
    for line in `cat ${DICT_PATH}`
    do
        echo -e ${line}'\t1'
    done > ${DICT_PATH}_temp

    cat ${DICT_PATH}_temp > ${DICT_PATH}
    rm ${DICT_PATH}_temp
fi

head ${DICT_PATH}
echo "dict path: ${DICT_PATH}"

DATA_DIR="/user/digu/itemSimGlobalNormalize"
for k in $( seq 1 10 )
do
	DAY_SUB=`date -d "${CUR_DATE} -${k} day" +"%Y-%m-%d"`
	FILE_PATH=${DATA_DIR}/${DAY_SUB}
	hdfs dfs -test -e ${FILE_PATH}/"_SUCCESS"
	if [ $? -eq 0 ] ;then
    	echo "${FILE_PATH} exists"
    	break
	fi
done

ITEM_SIM_PATH=${FILE_PATH}
echo "item sim path: ${ITEM_SIM_PATH}"

ITEM_SIM_MERGE_RESULT="/user/digu/itemSimContentMerge"
echo "item sim merge result: ${ITEM_SIM_MERGE_RESULT}"

ITEM_BIGRAPH_SIM_CONTENT_MERGE_PATH="${ITEM_SIM_MERGE_RESULT}/partA"
echo "item bigraph sim content merge path: ${ITEM_BIGRAPH_SIM_CONTENT_MERGE_PATH}"
hdfs dfs -test -e ${ITEM_BIGRAPH_SIM_CONTENT_MERGE_PATH}
if [ $? -eq 0 ] ;then
    echo "${ITEM_BIGRAPH_SIM_CONTENT_MERGE_PATH} exists"
    hdfs dfs -rm -r ${ITEM_BIGRAPH_SIM_CONTENT_MERGE_PATH}
fi

ITEM_SIM_CONTENT_MERGE_PATH="${ITEM_SIM_MERGE_RESULT}/partB"
echo "item sim content merge path: ${ITEM_SIM_CONTENT_MERGE_PATH}"
hdfs dfs -test -e ${ITEM_SIM_CONTENT_MERGE_PATH}
if [ $? -eq 0 ] ;then
    echo "${ITEM_SIM_CONTENT_MERGE_PATH} exists"
    hdfs dfs -rm -r ${ITEM_SIM_CONTENT_MERGE_PATH}
fi

W1="10000"
W2="1"
SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

#${SUBMIT}														\
#	--master yarn												\
#	--queue root.algorithm           							\
#	--driver-memory	32g											\
#	--num-executors	64											\
#	--executor-cores 1											\
#	--executor-memory 7373m										\
#	--class com.mgj.cf.content.ItemSimContentMerge      	    \
#	"${JAR_PATH}"												\
#	"${ITEM_BIGRAPH_SIM_UNION_PATH}"							\
#	"${WORD_SIM}"											    \
#	"${QUERY_IDF}"						    	                \
#	"${DICT_PATH}"						    	                \
#	"${WORD_TAG}"						    	                \
#	"${ITEM_BIGRAPH_SIM_CONTENT_MERGE_PATH}"					\
#	"${W1}"					                                    \
#	"${W2}"					                                    \

W1="1e6"
echo "w1:${W1}"

W2="500"
echo "w2:${W2}"

W3="10"
echo "w3:${W3}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm           							\
	--driver-memory	32g											\
	--num-executors	64											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.cf.content.ItemAllSimMerge  	            \
	"${JAR_PATH}"												\
	"${ITEM_BIGRAPH_SIM_UNION_PATH}"					        \
	"${ITEM_SIM_PATH}"							                \
	"${WORD_SIM}"											    \
	"${QUERY_IDF}"						    	                \
	"${DICT_PATH}"						    	                \
	"${WORD_TAG}"						    	                \
	"${ITEM_BIGRAPH_SIM_CONTENT_MERGE_PATH}"				    \
	"${W1}"					                                    \
	"${W2}"					                                    \
	"${W3}"					                                    \