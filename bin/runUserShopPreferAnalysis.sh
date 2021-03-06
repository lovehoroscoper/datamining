#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

# date.
BIZDATE=${DAY_SUB1}
BIZDATE_SUB1=${DAY_SUB2}
BIZDATE_SUB30=${DAY_SUB30}

echo "bizdate:${BIZDATE}"
echo "bizdate_sub1:${BIZDATE_SUB1}"
echo "bizdate_sub30:${BIZDATE_SUB30}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	10											\
	--executor-cores 4											\
	--executor-memory 7g										\
	--class com.mgj.usershopprefer.ShopPreferAnalysis			\
	"${JAR_PATH}"												\
	"${BIZDATE}"												\
	"${BIZDATE_SUB1}"											\
	"${BIZDATE_SUB30}"											\

