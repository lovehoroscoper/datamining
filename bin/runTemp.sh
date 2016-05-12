#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB2=`date -d "${CUR_DATE} -2 day" +"%Y-%m-%d"`
DAY_SUB3=`date -d "${CUR_DATE} -3 day" +"%Y-%m-%d"`
DAY_SUB7=`date -d "${CUR_DATE} -7 day" +"%Y-%m-%d"`
DAY_SUB10=`date -d "${CUR_DATE} -10 day" +"%Y-%m-%d"`
DAY_SUB15=`date -d "${CUR_DATE} -15 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
START=${DAY_SUB2}
END=${DAY_SUB2}

echo "start_date:${START}"
echo "end_date:${END}"

ITEM_SIM_RESULT_HDFS_DIR="/user/digu/itemBigraphSim"
echo "item sim result: ${ITEM_SIM_RESULT_HDFS_DIR}"

ITEM_SIM_RESULT_HDFS_DIR_UNION="/user/digu/itemBigraphSim/resultUnion"
echo "item sim result union: ${ITEM_SIM_RESULT_HDFS_DIR_UNION}"

ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP="/user/digu/itemBigraphSim/resultUnionGroup"
echo "item sim result union group: ${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP}"

ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP_GLOBAL_NORMALIZE="/user/digu/itemBigraphSim/resultUnionGroupGlobalNormalize"
echo "item sim result union group global normalize: ${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP_GLOBAL_NORMALIZE}"

N="7"
echo "number of days: ${N}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm           							\
	--driver-memory	16g											\
	--num-executors	64											\
	--executor-cores 1											\
	--executor-memory 7373m										\
	--class com.mgj.cf.ItemBigraphSimUnion						\
	"${JAR_PATH}"												\
	"${ITEM_SIM_RESULT_HDFS_DIR}"	    						\
	"${ITEM_SIM_RESULT_HDFS_DIR_UNION}"							\
	"${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP}"					\
	"${N}"			                                    		\
	"${ITEM_SIM_RESULT_HDFS_DIR_UNION_GROUP_GLOBAL_NORMALIZE}"  \