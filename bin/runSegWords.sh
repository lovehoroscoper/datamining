#!/bin/bash

# enviroment parameter.
source /home/digu/.bash_profile

# date.
DATA_PATH="/home/digu/workspace/data/dict"
if [ -f "${DATA_PATH}" ]; then
    rm ${DATA_PATH}
fi
hdfs dfs -get /user/bizdata/dict ${DATA_PATH}

for line in `cat ${DATA_PATH}`
do
echo -e $line'\t1'
done > ${DATA_PATH}_temp

cat ${DATA_PATH}_temp > ${DATA_PATH}
rm ${DATA_PATH}_temp
head ${DATA_PATH}

echo "data path: ${DATA_PATH}"
GENE_PATH="/user/digu/itemGroupWithTitle/data"

RESULT_DIR="/user/digu/geneWordsWithTitle"
hdfs dfs -test -e ${RESULT_DIR}
if [ $? -eq 0 ] ;then
    echo "${RESULT_DIR} exists"
    hdfs dfs -rm -r ${RESULT_DIR}
fi
echo "result dir: ${RESULT_DIR}"
echo "gene dir: ${GENE_PATH}"

SUBMIT="/home/spark/spark-1.6.0-bin-hadoop2.3/bin/spark-submit "
JAR_PATH="`pwd`/target/data-mining-1.0-SNAPSHOT-jar-with-dependencies.jar"

echo "${JAR_PATH}"

${SUBMIT}														\
	--master yarn												\
	--queue root.algorithm										\
	--driver-memory	16g											\
	--num-executors	16											\
	--executor-cores 4											\
	--executor-memory 7373m										\
	--class com.mgj.ml.nlp.SegWords								\
	"${JAR_PATH}"												\
	"${DATA_PATH}"												\
	"${GENE_PATH}"												\
	"${RESULT_DIR}"												\