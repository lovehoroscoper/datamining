package com.mgj.usershopprefer

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by xiaonuo on 11/25/15.
 */
object ShopPreferAnalysis {
  def main(args: Array[String]): Unit = {
    // args[0]: User base log.
    val conf = new SparkConf().
      setAppName("calculate shop rank").
      set("spark.sql.parquet.binaryAsString", "true")
    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val bizdate = args(0)
    val bizdateSub1 = args(1)
    val bizdateSub30 = args(2)

    val sampleSql = "select device_id, user_id, shop_id, time from s_dg_user_base_log where pt = '" + bizdate + "' and action_type = 'click' and platform_type = 'app'"
    val featureSql = "select user_id, shop_id, time from s_dg_user_base_log where pt >= '" + bizdateSub30 + "' and pt <= '" + bizdateSub1 + "' and (action_type = 'click' or action_type = 'order') and platform_type = 'app'"

    val allUserNumber = sqlContext.sql(sampleSql).select("device_id").rdd.filter(x => x.anyNull == false).map(x => x(0).toString).distinct().count()
    println("all user number:" + allUserNumber)
    val sampleUser = sqlContext.sql(sampleSql).rdd.filter(x => x.anyNull == false).map(x => x(1).toString).distinct()
    val featureUser = sqlContext.sql(featureSql).rdd.filter(x => x.anyNull == false).map(x => x(0).toString).distinct()
    val count = sampleUser.map(x => (x(0).toString, 1)).leftOuterJoin(featureUser.map(x => (x(0).toString, 1))).repartition(1000).map(x => {
      if (x._2._2 != None && x._2._2 != null) 1 else 0
    }).reduce((a, b) => (a + b))

    println("position count: " + count)
    println("total user: " + sampleUser.repartition(1000).count())

  }
}
