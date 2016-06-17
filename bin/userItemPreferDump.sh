#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

# input table.
TABLE_NAME="s_dg_cf_user_click_log_7d"

# dump to redis
${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	7g											\
	--num-executors	10											\
	--executor-cores 4											\
	--executor-memory 7g										\
	--class com.mgj.useritemprefer.dump.UserItemPreferDump		\
	"${JAR_PATH}"												\

