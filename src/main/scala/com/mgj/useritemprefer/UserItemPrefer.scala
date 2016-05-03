package com.mgj.useritemprefer

/**
  * Created by xiaonuo on 8/27/15.
  */

import java.text.SimpleDateFormat
import java.util.{Date, ArrayList, HashSet}
import com.mgj.utils.PartitionUtil
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

object UserItemPrefer {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("calculate user item prefer").
      set("spark.sql.parquet.binaryAsString", "true")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val bizdate = args(0)
    val bizdateSub = args(1)
    val bizdateSubSub = args(2)
    val itemSimPath = args(3)
    val itemCtrPath = args(4)

    //    val bizdate = "2016-01-18"
    //    val bizdateSub = "2016-01-17"
    //    val bizdateSubSub = "2016-01-15"
    //    val itemSimPath = "/user/digu/itemSim"

    PartitionUtil.checkAppLog(sqlContext, bizdate, "click")
    val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val itemViewSql = "select user_id, item_id, category_id, time, pt from s_dg_user_base_log where pt >= '" + bizdateSubSub + "' and pt <= '" + bizdateSub + "' and action_type='click' and platform_type='app'"
    val clickSql = "select user_id, item_id, category_id, time, pt from s_dg_user_base_log where pt = '" + bizdate + "' and action_type='click' and platform_type='app'"

    val userItemView = sqlContext.sql(itemViewSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString))

    val clickLog = sqlContext.sql(clickSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString))

    val N = 20
    val simItemNum = 5
    // user_id,item_id,category_id,time,pt
    val userItemPrefer = userItemView.groupBy(x => x._1).map(x => {

      // get category variance.
      def getDateVar(timeList: List[String]): Double = {
        if (timeList.size == 1) {
          return 0d
        }
        val sdfDay = new SimpleDateFormat("yyyy-MM-dd")
        val list = timeList.map(x => sdfDay.parse(x).getTime / 1000)
        val avg = 1.0 * list.sum / list.size
        val variance = 1.0 * list.map(x => (x - avg) * (x - avg)).sum / (list.size - 1)
        return Math.sqrt(variance) / 24 / 60 / 60
      }

      // get day diff
      def getDateDiff(dateA: String, dateB: String, pattern: String): Double = {
        val sdfDay = new SimpleDateFormat(pattern)
        return 1.0 * Math.abs(sdfDay.parse(dateA).getTime - sdfDay.parse(dateB).getTime) / 1000 / 60 / 60 / 24
      }

      // item_id, category_id, time, pt in one user
      val userTrack = x._2.map(x => (x._2, x._3, x._4, x._5)).toList

      // item_id, category_id, pt in one user
      // cntSum = sum(1/(1+timeDiff))
      // (category_id, (1+variance)*cntSum, cntSum, variance)
      val userCategoryScore = userTrack.groupBy(x => x._2).map(x => {
        val variance = getDateVar(x._2.map(x => x._4))
        val cntSum = x._2.map(x => 1.0 / (1 + Math.exp(0.5 * getDateDiff(bizdateSub, x._4, "yyyy-MM-dd")))).sum
        val minTime = x._2.map(x => getDateDiff(bizdateSub, x._4, "yyyy-MM-dd")).min
        (x._1, Math.log(cntSum + 1) / Math.log(1.5d) * (1 + variance), Math.log(cntSum + 1) / Math.log(1.5d), variance, minTime)
      }).toList.sortWith((a, b) => a._2 > b._2)

      val sumScore = userCategoryScore.map(x => x._2).sum
      val result = new ArrayList[(String, String, Double, Double, Double, Double, Double)]()
      var n = 0
      var i = 0
      val itemInCategory = x._2.groupBy(x => x._3).map(x => (x._1, x._2.groupBy(x => x._2).map(x => x._2.head)))
      while (n <= N && i < userCategoryScore.size) {
        val itemList = itemInCategory.get(userCategoryScore.apply(i)._1).get.toList.sortWith((a, b) => a._4 > b._4)
        val size = Math.min(itemList.size, Math.round(1.0 * N * userCategoryScore.apply(i)._2 / sumScore)).toInt
        result.addAll(itemList.slice(0, size).map(x => (x._1, x._2, getDateDiff(bizdate, x._5, "yyyy-MM-dd"), userCategoryScore.apply(i)._2, userCategoryScore.apply(i)._3, userCategoryScore.apply(i)._4, userCategoryScore.apply(i)._5)))
        n += size
        i += 1
      }
      result.toList.take(N)
    })
    userItemPrefer.take(20).foreach(println)

    val itemSim = sc.textFile(itemSimPath).map(x => (x.split(" ")(0), x.split(" ")(1).split(",").map(x => (x.split(":")(0), x.split(":")(1).toDouble / 100000d)).toList.take(simItemNum))).collect().toMap
    val itemCtr = sc.textFile(itemCtrPath).map(x => (x.split(" ")(0), x.split(" ")(1).toDouble / 1000000d)).collect().toMap

    val userItemSimFeature = userItemPrefer.flatMap(x => x).map(x => {
      val result = new ArrayList[(String, String, Double, Double, Double, Double, Double, Double, Double, Double)]()
      val itemSet = new HashSet[String]()
      if (itemSim.contains(x._2)) {
        val list = itemSim.get(x._2).get
        for (i <- 1 to list.size) {
          val itemId = list.apply(i - 1)._1
          val score = list.apply(i - 1)._2
          val ctrScore = itemCtr.getOrElse(itemId, 0d)
          if (!itemSet.contains(itemId)) {
            result.add((x._1, itemId, x._3, x._4, x._5, x._6, x._7, score, i.toDouble, ctrScore))
            itemSet.add(itemId)
          }
        }
      } else {
        if (!itemSet.contains(x._2)) {
          val ctrScore = itemCtr.getOrElse(x._2, 0d)
          result.add((x._1, x._2, x._3, x._4, x._5, x._6, x._7, 1d, 0d, ctrScore))
          itemSet.add(x._2)
        }
      }
      result.toList
    }).flatMap(x => x).map(x => (x._1, x._2, Vectors.dense(Array(x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10))))
    userItemSimFeature.take(20).foreach(println)
    userItemPrefer.unpersist(blocking = false)
    //    println("userItemSimFeature: " + userItemSimFeature.count())

    def buildSample(sampleLog: RDD[(String, String, String)], feature: RDD[(String, String, Vector)]): RDD[(Vector, Double)] = {
      val sample = feature.map(x => (x._1 + x._2, x._3)).leftOuterJoin(sampleLog.map(x => (x._1 + x._2, 1d))).map(x => {
        if (x._2._2 == None) {
          (x._2._1, 0d)
        } else {
          (x._2._1, 1d)
        }
      })
      return sample
    }

    val sample = buildSample(clickLog.map(x => (x._1, x._2, x._3)), userItemSimFeature)

    val ratioCount = sample.map(x => (x._2.toDouble, 1d)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    val ratio = ratioCount._1 / ratioCount._2

    println("total sample count: " + ratioCount._2)
    println("total positive sample count: " + ratioCount._1)
    println("positive negtive sample ratio: " + ratio)

    val posSample = sample.filter(x => x._2 > 0.5)
    val negSample = sample.filter(x => x._2 < 0.5).sample(false, ratio)

    val sampleFinal = posSample.union(negSample).map(x => Row(x._1.toString, x._2))
    println("sample count after sampling: " + sampleFinal.count)

    val schema =
      StructType(
        StructField("feature", StringType, true)
          :: StructField("label", DoubleType, true)
          :: Nil)

    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    val sampleDF: DataFrame = sqlContext.createDataFrame(sampleFinal, schema)
    sampleFinal.unpersist(blocking = false)
    sampleDF.registerTempTable("s_dg_user_item_prefer_sample_temp")
    sqlContext.sql("drop table if exists s_dg_user_item_prefer_sample")
    sqlContext.sql("create table s_dg_user_item_prefer_sample as select * from s_dg_user_item_prefer_sample_temp")
  }
}
