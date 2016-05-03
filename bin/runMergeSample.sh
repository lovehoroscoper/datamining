#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y-%m-%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d"`
DAY_SUB30=`date -d "${CUR_DATE} -30 day" +"%Y-%m-%d"`
BIZDATE=${DAY_SUB1}
BIZDATE_SUB30=${DAY_SUB30}

#FEATURES="user_item_prefer,user_real_item_prefer,item_ctr,user_shop_prefer,user_shop_prefer_order,user_category_prefer,user_category_prefer_order,user_gene_prefer,user_gene_prefer_order"
#FEATURES="item_ctr,user_gene_prefer_order,user_gene_prefer"
FEATURES="${1}"
#SAMPLE="s_dg_click_sample,s_dg_order_sample,s_dg_all_sample"
SAMPLE="${2}"

echo "features: ${FEATURES}"
echo "sample: ${SAMPLE}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm     									\
	--driver-memory	16g											\
	--num-executors	32											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.ml.lr.MergeSample							\
	"${JAR_PATH}"												\
	"${DAY_SUB1}"												\
	"${FEATURES}"												\
	"${SAMPLE}"													\