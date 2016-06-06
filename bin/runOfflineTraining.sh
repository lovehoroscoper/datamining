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
YESTERDAY=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d %H:%M:%S"`

BIZDATE=${DAY_SUB1}
echo "bizdate: ${BIZDATE}"

CODE="app_clothing_pop"
echo "code:${CODE}"

# user_category_prefer,user_category_prefer_order,user_item_prefer,user_real_item_prefer,item_ctr,user_shop_prefer,user_shop_prefer_order,user_gene_prefer,user_gene_prefer_order,pos
#FEATURES="user_category_prefer,user_category_prefer_order,user_item_prefer,user_real_item_prefer,item_ctr,user_shop_prefer,user_shop_prefer_order,user_gene_prefer,user_gene_prefer_order,pos"
#FEATURES="user_category_prefer,user_category_prefer_order,user_item_prefer,user_real_item_prefer,item_search_ctr,user_shop_prefer,user_shop_prefer_order,user_gene_prefer,user_gene_prefer_order,pos"
FEATURES="user_category_prefer,user_category_prefer_order,user_item_prefer,user_real_item_prefer,item_ctr,user_shop_prefer,user_shop_prefer_order,user_gene_prefer,user_gene_prefer_order,pos"
echo "features:${FEATURES}"

SAMPLE_TABLE="s_dg_sample_${CODE}"
echo "sample table:${SAMPLE_TABLE}"

FEATURE_TABLE="s_dg_sample_${CODE}"
echo "feature table:${FEATURE_TABLE}"

# build_sample,adapt_features,train
STAGE="build_sample,adapt_features,train"
echo "stage:${STAGE}"

N="20"
echo "N:${N}"

MODEL_NAME="CLOTHING_MODEL"
echo "model name:${MODEL_NAME}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"
echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 4											\
	--executor-memory 7373m										\
	--class com.mgj.ml.rank.OfflineTraining 		    		\
	"${JAR_PATH}"												\
	"${CODE}"									        		\
	"${SAMPLE_TABLE}"									    	\
	"${FEATURE_TABLE}"									    	\
	"${BIZDATE}"									    		\
	"${FEATURES}"									    		\
	"${STAGE}"			    						    		\
	"${N}"			    						         		\
	"${MODEL_NAME}"			    						        \
