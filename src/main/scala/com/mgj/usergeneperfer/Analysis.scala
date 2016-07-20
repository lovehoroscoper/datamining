package com.mgj.usergeneperfer

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by xiaonuo on 1/19/16.
 */
object Analysis {
  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("calculate user gene prefer")
      .set("spark.cores.max", "28")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    val userGenePrefer = sc.textFile("hdfs://mgjcluster/user/digu/userGenePreferSub")
    userGenePrefer.take(10)

    val clickUser = sqlContext.sql("select user_id from s_dg_user_base_log where pt = '2016-01-18' and action_type='click' and platform_type='app'")
    val uv = clickUser.rdd.filter(x => x.anyNull == false).map(x => x(0).toString).distinct()
    uv.map(x => (x, 1)).join(userGenePrefer.map(x => (x.split(" ")(0), 1))).map(x => (x._1, 1)).count()
  }
}
