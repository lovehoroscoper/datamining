#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh
BIZDATE=${DAY_SUB2}

#FEATURES="user_item_prefer,user_real_item_prefer,item_ctr,user_shop_prefer,user_shop_prefer_order,user_category_prefer,user_category_prefer_order,user_gene_prefer,user_gene_prefer_order"
#FEATURES="item_ctr,user_gene_prefer_order,user_gene_prefer"
FEATURES="${1}"
#SAMPLE="s_dg_click_sample,s_dg_order_sample,s_dg_all_sample"
SAMPLE="${2}"

echo "bizdate: ${BIZDATE}"
echo "features: ${FEATURES}"
echo "sample: ${SAMPLE}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm     									\
	--driver-memory	16g											\
	--num-executors	32											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.ml.rank.MergeSample							\
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${FEATURES}"												\
	"${SAMPLE}"													\