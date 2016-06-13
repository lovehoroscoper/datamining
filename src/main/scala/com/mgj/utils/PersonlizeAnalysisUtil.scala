package com.mgj.utils

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xiaonuo on 6/13/16.
  */
object PersonlizeAnalysisUtil {
  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("calculate user shop prefer")
      .set("spark.cores.max", "28")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val bizdateSub = args(0)
    val bizdate = args(1)
    val clickLogSql = s"select user_id, item_id, time, category_id, pt from s_dg_user_base_log where pt >= '${bizdateSub}' and pt <= '${bizdate}' and action_type = 'click' and platform_type = 'app'"
    val clickLog = sqlContext.sql(clickLogSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(4).toString))

    val userSet = clickLog.groupBy(x => x._1).map(x => (x._1, x._2.map(x => x._3).toList.distinct.size)).filter(x => x._2 > 1).map(x => x._1).collect().toSet

    val N = 10
    println(s"userSet size:${userSet.size}")

    val userShopPreferSub = sc.textFile(s"/user/digu/userShopPreferRecord/${bizdateSub}")
      .map(x => (x.split(" ")(0), x.split(" ")(1).split(",").toList.map(x => x.split(":")(0)).take(N)))
      .filter(x => userSet.contains(x._1))

    val userShopPrefer = sc.textFile(s"/user/digu/userShopPreferRecord/${bizdate}")
      .map(x => (x.split(" ")(0), x.split(" ")(1).split(",").toList.map(x => x.split(":")(0)).take(N)))
      .filter(x => userSet.contains(x._1))

    println(s"userShopPreferSub count:${userShopPreferSub.count}")
    println(s"userShopPrefer count:${userShopPrefer.count}")

    val commonShopPrefer = userShopPrefer.join(userShopPreferSub).map(x => {
      var sum = 0
      for (i <- 0 to Math.min(x._2._1.size, x._2._2.size) - 1) {
        val a = x._2._1.apply(i)
        val b = x._2._2.apply(i)
        if (a.equals(b)) {
          sum += 1
        }
      }
      (x._1, (x._2._1.take(3).toSet & x._2._2.take(3).toSet).size, sum)
    })

    val commonShopPreferSize = commonShopPrefer.count()
    val commonShopPreferTop3 = commonShopPrefer.map(x => (x._2.toDouble)).sum()
    val commonShopPreferTop10 = commonShopPrefer.map(x => (x._3.toDouble)).sum()

    println(s"avg shop top3 contain:${commonShopPreferTop3 / commonShopPreferSize}")
    println(s"avg shop top10 same:${commonShopPreferTop10 / commonShopPreferSize}")

    val userItemPreferSub = sc.textFile(s"/user/digu/userItemPreferRecord/${bizdateSub}")
      .map(x => (x.split(" ")(0), x.split(" ")(1).split(",").toList.map(x => x.split(":")(0)).take(N)))
      .filter(x => userSet.contains(x._1))

    val userItemPrefer = sc.textFile(s"/user/digu/userItemPreferRecord/${bizdate}")
      .map(x => (x.split(" ")(0), x.split(" ")(1).split(",").toList.map(x => x.split(":")(0)).take(N)))
      .filter(x => userSet.contains(x._1))

    println(s"userItemPreferSub count:${userItemPreferSub.count}")
    println(s"userItemPrefer count:${userItemPrefer.count}")

    val commonItemPrefer = userItemPrefer.join(userItemPreferSub).map(x => {
      var sum = 0
      for (i <- 0 to Math.min(x._2._1.size, x._2._2.size) - 1) {
        val a = x._2._1.apply(i)
        val b = x._2._2.apply(i)
        if (a.equals(b)) {
          sum += 1
        }
      }
      (x._1, (x._2._1.take(3).toSet & x._2._2.take(3).toSet).size, sum)
    })

    val commonItemPreferSize = commonItemPrefer.count()
    val commonItemPreferTop3 = commonItemPrefer.map(x => (x._2.toDouble)).sum()
    val commonItemPreferTop10 = commonItemPrefer.map(x => (x._3.toDouble)).sum()

    println(s"avg item top3 contain:${commonItemPreferTop3 / commonItemPreferSize}")
    println(s"avg item top10 same:${commonItemPreferTop10 / commonItemPreferSize}")

  }
}
