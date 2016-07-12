#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

YESTERDAY=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d %H:%M:%S"`

BIZDATE="2016-07-10"
echo "bizdate: ${BIZDATE}"

CODE="app_sport_pop"
echo "code:${CODE}"

# user_category_prefer,user_category_prefer_order,user_item_prefer,user_real_item_prefer,item_ctr,user_shop_prefer,user_shop_prefer_order,user_gene_prefer,user_gene_prefer_order,pos
#FEATURES="user_category_prefer,user_category_prefer_order,user_item_prefer,user_real_item_prefer,item_ctr,user_shop_prefer,user_shop_prefer_order,user_gene_prefer,user_gene_prefer_order,pos"
#FEATURES="user_category_prefer,user_category_prefer_order,user_item_prefer,user_real_item_prefer,item_search_ctr,user_shop_prefer,user_shop_prefer_order,user_gene_prefer,user_gene_prefer_order,pos"
FEATURES="user_category_prefer,user_category_prefer_order,user_item_prefer,user_real_item_prefer,item_search_ctr,user_shop_prefer,user_shop_prefer_order,user_gene_prefer,user_gene_prefer_order,pos"
echo "features:${FEATURES}"

SAMPLE_TABLE="s_dg_sample_${CODE}"
echo "sample table:${SAMPLE_TABLE}"

FEATURE_TABLE="s_dg_sample_${CODE}"
echo "feature table:${FEATURE_TABLE}"

# build_sample,adapt_features,train
STAGE="adapt_features"
echo "stage:${STAGE}"

N="20"
echo "N:${N}"

MODEL_NAME="SPORT_MODEL"
echo "model name:${MODEL_NAME}"

${SUBMIT}														\
	--master yarn												\
	--queue root.pool   										\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 4											\
	--executor-memory 7373m										\
	--class com.mgj.ml.rank.OfflineTrainingV2 		    		\
	"${JAR_PATH}"												\
	"${CODE}"									        		\
	"${SAMPLE_TABLE}"									    	\
	"${FEATURE_TABLE}"									    	\
	"${BIZDATE}"									    		\
	"${FEATURES}"									    		\
	"${STAGE}"			    						    		\
	"${N}"			    						         		\
	"${MODEL_NAME}"			    						        \
