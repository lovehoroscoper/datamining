#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile
source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

FILE_PATH="${RESULT_PATH_PREFIX}/user/digu/shopPL"
echo "file path: ${FILE_PATH}"

SHOP_SIM_RANK="${RESULT_PATH_PREFIX}/user/digu/shopSimRank"
echo "shop sim rank: ${SHOP_SIM_RANK}"

WEIGHT="100"
echo "weight: ${WEIGHT}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	16g											\
	--num-executors	32											\
	--executor-cores 2											\
	--executor-memory 7373m										\
	--class com.mgj.usershopprefer.ShopSelect					\
	"${JAR_PATH}"												\
	"${DAY_SUB7}"												\
	"${DAY_SUB15}"												\
	"${FILE_PATH}"												\
	"${SHOP_SIM_RANK}"											\
	"${WEIGHT}"													\
