#!/usr/bin/env bash

CUR_TIME=`date +%s`
CUR_DATE=`date  +%Y%m%d`
DAY_SUB1=`date -d "${CUR_DATE} -1 day" +"%Y%m%d"`
DAY_SUB2=`date -d "${CUR_DATE} -2 day" +"%Y%m%d"`

HDFS_FILE_PATH="${RESULT_PATH_PREFIX}/user/wujia/ltr/bookorder/cvr/"${CUR_DATE}/${CUR_DATE}".train.app"
echo "hdfs file path:${HDFS_FILE_PATH}"

FEATURE_PATH="${RESULT_PATH_PREFIX}/user/digu/LTR_FEATURE/feature_predict"
echo "feature path:${FEATURE_PATH}"

PREDICTOR_PATH="/home/digu/workspace/datamining"
echo "predictor path:${PREDICTOR_PATH}"

RESUILT_HDFS_PATH="${RESULT_PATH_PREFIX}/user/digu/LTR_FEATURE/pair_predict"
echo "result hdfs path:${RESUILT_HDFS_PATH}"

RESUILT_PATH="/home/digu/pair_predict"
echo "result path:${RESUILT_PATH}"

LOG_FILE="/home/digu/runBuildPairSample.log"
echo "log file:${LOG_FILE}"

LOCK_FILE="/home/digu/predictLock"
echo "lock file:${LOCK_FILE}"

test -e ${LOCK_FILE}
if [ $? -eq 0 ];then
    echo "predict has been locked"
else
    touch ${LOCK_FILE}
    ${HDFS} -test -e ${HDFS_FILE_PATH}
    if [ $? -eq 0 ];then
        ${HDFS} -rm -r ${FEATURE_PATH}
        echo "cp to local"
        ${HDFS} -cp ${HDFS_FILE_PATH} ${FEATURE_PATH}
        cd ${PREDICTOR_PATH}
        echo "rm predict result"
        ${HDFS} -rm -r ${RESUILT_HDFS_PATH}
        echo "run predict"
        sh bin/runBuildPairSample.sh 2>&1 | tee ${LOG_FILE}
        echo "rm local result"
        rm -rf ${RESUILT_PATH}
        echo "cp result to local"
        ${HDFS} -getmerge ${RESUILT_HDFS_PATH} ${RESUILT_PATH}
        echo "scp to 105"
        scp -P 10022 ${RESUILT_PATH} 10.15.2.105:/var/data/ltr_2_rank/${CUR_DATE}
        echo "DONE"
    fi
    rm ${LOCK_FILE}
    echo "THE END"
fi

