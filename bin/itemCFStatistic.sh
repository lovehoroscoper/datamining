#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

source ./bin/utils/conf.sh
source ./bin/utils/constant.sh
source ./bin/utils/functions.sh

START=${DAY_SUB1}
END=${DAY_SUB1}

echo "start date:${START}"
echo "end date:${END}"

N="10"
echo "N:${N}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	7g											\
	--num-executors	10											\
	--executor-cores 4											\
	--executor-memory 7g										\
	--class com.mgj.cf.evaluate.Statistic						\
	"${JAR_PATH}"												\
	"$1"														\
	"${START}"													\
	"${END}"													\
	"${N}"		    											\

