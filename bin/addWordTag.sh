#!/usr/bin/env bash
WORD_TAG_PATH="/user/digu/wordTagTrain"
echo "word tag path:${WORD_TAG_PATH}"

WORD_TAG_LOCAL="wordTagTrain"
echo "word tag local:${WORD_TAG_LOCAL}"

NEW_WORD=${1}
echo "new word:${NEW_WORD}"

${HDFS} -test -e ${WORD_TAG_PATH}
if [ $? -eq 0 ] ;then
    ${HDFS} -get ${WORD_TAG_PATH} ${WORD_TAG_LOCAL}
fi

echo ${NEW_WORD} >> ${WORD_TAG_LOCAL}

${HDFS} -test -e ${WORD_TAG_PATH}
if [ $? -eq 0 ] ;then
    ${HDFS} -rm -r ${WORD_TAG_PATH}
    ${HDFS} -put ${WORD_TAG_LOCAL} ${WORD_TAG_PATH}
else
    ${HDFS} -put ${WORD_TAG_LOCAL} ${WORD_TAG_PATH}
fi

echo "rm ${WORD_TAG_LOCAL}"
rm -rf ${WORD_TAG_LOCAL}

echo "success!"