#!/usr/bin/env bash

function remove_hdfs_file_base(){
    if [ $# -ne 1 ];then
        echo "input param size should be 1"
        return 1
    fi

    local FILE_NAME="${1}"

    hdfs dfs -test -e ${FILE_NAME}
    if [ $? -eq 0 ] ;then
        echo "${FILE_NAME} exists"
        hdfs dfs -rm -r ${FILE_NAME}
    fi

    return $?
}

function remove_hdfs_file(){
    if [ $# -gt 2 ];then
        echo "input param size should be less than 2"
        return 1
    fi

    local FILE_NAME="${1}"

    if [ x"${FILE_NAME}" == x"" ];then
        echo "file name should not be empty"
        return 1
    fi

    if [ $# -eq 2 ];then
        local DATE="${2}"
        if [ x"${DATE}" == x"" ];then
        echo "date should not be empty"
        return 1
        fi
        FILE_NAME=${FILE_NAME}/${DATE}
    fi

    remove_hdfs_file_base ${FILE_NAME}
    return $?
}

function find_latest_file(){
    if [ $# -ne 3 ];then
        echo "input param should include base data dir, current date and N"
        return 1
    fi

    local DATA_DIR=${1}
    local CUR_DATE=${2}
    local N=${3}

    if [ x"${DATA_DIR}" == x"" ];then
        echo "base data dir should not be empty"
        return 1
    fi

    for k in $( seq 1 ${N} )
    do
        DAY_SUB=`date -d "${CUR_DATE} -${k} day" +"%Y-%m-%d"`
        FILE_PATH=${DATA_DIR}/${DAY_SUB}
        hdfs dfs -test -e ${FILE_PATH}/"_SUCCESS"
        if [ $? -eq 0 ] ;then
            echo ${FILE_PATH}
            return 0
        fi
    done

    return 1
}

function put_record(){
    if [ $# -ne 3 ];then
        echo "input param should include current dir, target dir and date"
        return 1
    fi

    local CURRENT_DIR=${1}
    local TARGET_DIR=${2}
    local CUR_DATE=${3}

    echo "current dir path: ${CURRENT_DIR}"
    echo "target dir path: ${TARGET_DIR}"
    hdfs dfs -test -e ${TARGET_DIR}
    if [ $? -eq 0 ] ;then
        echo "${TARGET_DIR} exists"
    else
        hdfs dfs -mkdir ${TARGET_DIR}
    fi
    hdfs dfs -cp ${CURRENT_DIR} ${TARGET_DIR}/${CUR_DATE} &
    return $?
}