#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	32g											\
	--num-executors	32											\
	--executor-cores 8											\
	--executor-memory 7373m										\
	--class com.mgj.usershopprefer.UserRealShopPrefer			\
	"${JAR_PATH}"												\