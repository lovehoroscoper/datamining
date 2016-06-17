#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

BIZDATE=${DAY_SUB2}
echo "bizdate: ${BIZDATE}"

# SEARCH: 129,139,130,134,307
# TUAN: 295,291,280
# APP_IDS="295,291,280"
APP_IDS="1389,1661,1662"
echo "app_ids:${APP_IDS}"

TABLE="s_dg_click_sample"
echo "table:${TABLE}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.ml.rank.BuildResourceSample		    		\
	"${JAR_PATH}"												\
	"${APP_IDS}"									    		\
	"${TABLE}"									    	    	\
	"${BIZDATE}"									    	    \