#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# input table.
OUTPUT_PATH="/user/digu/did2uid"
hdfs dfs -test -e ${OUTPUT_PATH}
if [ $? -eq 0 ] ;then
    echo "${OUTPUT_PATH} exists"
    hdfs dfs -rm -r ${OUTPUT_PATH}
fi
echo "output path: ${OUTPUT_PATH}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "

JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.data   										\
	--driver-memory	4g											\
	--num-executors	8											\
	--executor-cores 1											\
	--executor-memory 4g										\
	--class com.mgj.bizdata.Did2UidDump         				\
	"${JAR_PATH}"												\
	"${OUTPUT_PATH}"											\

curl "http://10.15.17.31:10850/dumpData?featureName=did2uid&method=local"
curl "http://10.19.22.49:10850/dumpData?featureName=did2uid&method=local"
curl "http://10.15.19.20:10850/dumpData?featureName=did2uid&method=local"
curl "http://10.19.16.30:10850/dumpData?featureName=did2uid&method=local"
curl "http://10.15.18.40:10850/dumpData?featureName=did2uid&method=local" &