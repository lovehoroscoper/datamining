#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

YESTERDAY=`date -d "${CUR_DATE} -1 day" +"%Y-%m-%d %H:%M:%S"`
START=${DAY_SUB10}
END=${DAY_SUB1}

echo "start_date:${START}"
echo "end_date:${END}"
echo "${YESTERDAY}"

# input table.
TABLE_NAME="s_dg_cf_user_click_log_7d"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	7g											\
	--num-executors	10											\
	--executor-cores 4											\
	--executor-memory 7g										\
	--class com.mgj.useritemprefer.UserItemPrefer				\
	"${JAR_PATH}"												\
	"${YESTERDAY}"												\
	"${START}"													\
	"${END}"													\

