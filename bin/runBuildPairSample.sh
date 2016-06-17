#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	8g											\
	--num-executors	16											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.ml.sample.BuildPairSample					\
	"${JAR_PATH}"												\
	"${DAY_SUB1}"												\
	"${DAY_SUB1}"												\
	"${DAY_SUB2}"												\
	"${CUR_DATE}"												\