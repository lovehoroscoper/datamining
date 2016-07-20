package com.mgj.utils

import java.util

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 6/13/16.
  */
object PersonlizeAnalysisUtil {
  val N = 10

  def getUserSet(sqlContext: HiveContext, bizdateSub: String, bizdate: String): Set[String] = {
    val clickLogSql = s"select user_id, item_id, time, category_id, pt from s_dg_user_base_log where pt >= '${bizdateSub}' and pt <= '${bizdate}' and action_type = 'expose' and user_id is not null and user_id not in ('19800')"
    val clickLog = sqlContext.sql(clickLogSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(4).toString))
    val userSet = clickLog.groupBy(x => x._1).map(x => (x._1, x._2.map(x => x._3).toList.distinct.size)).filter(x => x._2 > 1).map(x => x._1).collect().toSet
    println(s"userSet size:${userSet.size}")
    return userSet
  }

  def getRepeatAnalysis(sc: SparkContext, pathA: String, pathB: String, userSet: Set[String]): Unit = {
    val userPreferA = sc.textFile(pathA)
      .map(x => (x.split(" ")(0), x.split(" ")(1).split(",").toList.map(x => x.split(":")(0)).take(N)))
      .filter(x => userSet.contains(x._1)).cache()

    val userPreferB = sc.textFile(pathB)
      .map(x => (x.split(" ")(0), x.split(" ")(1).split(",").toList.map(x => x.split(":")(0)).take(N)))
      .filter(x => userSet.contains(x._1)).cache()

    println(s"${pathA} count:${userPreferA.count}")
    println(s"${pathB} count:${userPreferB.count}")

    val commonPrefer = userPreferB.fullOuterJoin(userPreferA).map(x => {
      if (x._2._1 == None || x._2._2 == None) {
        (x._1, 0, 0)
      } else {
        var sum = 0
        for (i <- 0 to Math.min(x._2._1.get.size, x._2._2.get.size) - 1) {
          val a = x._2._1.get.apply(i)
          val b = x._2._2.get.apply(i)
          if (a.equals(b)) {
            sum += 1
          }
        }
        (x._1, (x._2._1.get.take(3).toSet & x._2._2.get.take(3).toSet).size, sum)
      }
    }).cache()
    userPreferA.unpersist(blocking = false)
    userPreferB.unpersist(blocking = false)

    val commonPreferSize = commonPrefer.count()
    val commonPreferTop3 = commonPrefer.map(x => (x._2.toDouble)).sum()
    val commonPreferTop10 = commonPrefer.map(x => (x._3.toDouble)).sum()
    commonPrefer.unpersist(blocking = false)

    println(s"avg top3 contain same:${commonPreferTop3 / commonPreferSize}")
    println(s"avg top10 rank same:${commonPreferTop10 / commonPreferSize}")
  }

  def getSceneAnalysis(sqlContext: HiveContext, bizdate: String, bizdateSub: String, code: String, userSet: Set[String]): Unit = {
    val clickSample = SampleV2Util.getClickSample(sqlContext, bizdate, false, code)
      .select("user_id", "item_id", "pos", "label")
      .map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString))
      .filter(x => Math.exp(x._3.toDouble) - 1 < 11d && userSet.contains(x._1)).groupBy(x => x._1).cache()
    println(s"click ${bizdate} count:${clickSample.count()}")

    val clickSampleSub = SampleV2Util.getClickSample(sqlContext, bizdateSub, false, code)
      .select("user_id", "item_id", "pos", "label")
      .map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString))
      .filter(x => Math.exp(x._3.toDouble) - 1 < 11d && userSet.contains(x._1)).groupBy(x => x._1).cache()
    println(s"click ${bizdateSub} count:${clickSampleSub.count()}")

    val joinResult = clickSample.join(clickSampleSub).map(x => {
      val setA = x._2._1.map(x => x._2).toSet
      val setB = x._2._2.map(x => x._2).toSet
      val list = new util.ArrayList[(String, String, String)]()
      val commonSet = setA & setB
      for (e <- x._2._2) {
        val itemId = e._2
        val label = e._4
        val flag = if (commonSet.contains(itemId)) "Y" else "N"
        list.add((itemId, flag, label))
      }
      (x._1, commonSet.size, setA.size, list)
    })

    val repeatStatic = joinResult.map(x => (x._2, x._3)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    println(s"${code} repeatRatio: ${1.0 * repeatStatic._1 / repeatStatic._2}")
    println(s"${code} repeatStatic: ${repeatStatic}")
    println(s"user view repeat:${joinResult.filter(x => x._2 > 0).count()}")
    println(s"all user:${joinResult.count()}")

    val ctr = joinResult.map(x => x._4).flatMap(x => x).groupBy(x => (x._1, x._2)).map(x => {
      val label = x._2.toList.map(x => x._3.toDouble)
      (x._1, label.sum, label.size)
    })
    ctr.take(10).foreach(println)

    val ctrResult = ctr.map(x => (x._1._2, (x._2, x._3))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).collect().toMap
    val avgCtr = ctr.map(x => (x._2, x._3)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    println(s"avg ctr:${avgCtr._1 / avgCtr._2}")
    println(avgCtr)

    println(s"Y avg ctr:${ctrResult.get("Y").get._1 / ctrResult.get("Y").get._2}")
    println(s"N avg ctr:${ctrResult.get("N").get._1 / ctrResult.get("N").get._2}")
    println(ctrResult)
  }

  def main(args: Array[String]): Unit = {
    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("data analysis")
      .set("spark.cores.max", "28")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    val bizdateSub = args(0)
    val bizdate = args(1)
    val userSet = getUserSet(sqlContext, bizdateSub, bizdate)

    //    getRepeatAnalysis(sc, s"/user/digu/userShopPreferRecord/${bizdateSub}", s"/user/digu/userShopPreferRecord/${bizdate}", userSet)
    //    getRepeatAnalysis(sc, s"/user/digu/userItemPreferRecord/${bizdateSub}", s"/user/digu/userItemPreferRecord/${bizdate}", userSet)

    //    getSceneAnalysis(sqlContext, bizdate, bizdateSub, "app_shoes_pop", userSet)
    //    getSceneAnalysis(sqlContext, bizdate, bizdateSub, "app_clothing_pop", userSet)
    //    getSceneAnalysis(sqlContext, bizdate, bizdateSub, "app_bags_pop", userSet)
    getSceneAnalysis(sqlContext, bizdate, bizdateSub, "app_xitemsearch_pop", userSet)
  }
}
