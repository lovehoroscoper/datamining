package com.mgj.utils

import java.text.SimpleDateFormat
import java.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 4/12/16.
  */
object SampleV2Util {
  def takeSample(sample: DataFrame): DataFrame = {
    val ratioCount = sample.select("label").rdd.map(x => (x(0).toString.toDouble, 1d)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    val ratio = ratioCount._1 / ratioCount._2
    println("total sample count: " + ratioCount._2)
    println("total positive sample count: " + ratioCount._1)
    println("positive negtive sample ratio: " + ratio)

    val posSample = sample.where("cast(label as double) > 0.5")
    val negSample = sample.where("cast(label as double) < 0.5 ").sample(false, ratio)

    val sampleFinal = posSample.unionAll(negSample)
    return sampleFinal
  }

  def getClickSample(sqlContext: HiveContext, appIds: String*): DataFrame = {
    def getTimeDiff(visitTimex: String, visitTimey: String): Double = {
      val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val datex = df.parse(visitTimex)
      val datey = df.parse(visitTimey)
      return Math.abs(1.0 * (datex.getTime() - datey.getTime()) / 1000 / 60)
    }

    val clickSampleLog = getClickSampleLog(sqlContext, appIds: _*).select("user_id", "item_id", "time", "pos", "label")
    println("click sample log")
    clickSampleLog.show()

    val clickSampleRDD = clickSampleLog.rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString))

    // user_id, item_id, time, pos, label.
    // skip above.
    val sampleFinal = clickSampleRDD
      .groupBy(x => x._1)
      .map(x => {
        // sort with visit time.
        val sampleList = x._2.toList.sortWith((a, b) => a._3.toLong > b._3.toLong)

        // time stamp id.
        var timeId = 0

        // last visit time.
        var lastTime = sampleList.head._3.toLong

        // make time stamp id.
        val sampleWithTimeId = new util.ArrayList[((String, String, String, String, String), Int)]()
        for (e <- sampleList) {
          val currentTime = e._3.toLong
          if (Math.abs(currentTime - lastTime) / 60 > 30) {
            timeId += 1
          }
          lastTime = currentTime
          sampleWithTimeId.add((e, timeId))
        }

        // skip above.
        val sampleFilter = sampleWithTimeId.groupBy(x => x._2).map(x => {
          val list = x._2.toList.map(x => x._1)
          if (list.filter(x => x._5.equals("1")).size > 0) {
            val maxClickPos = list.filter(x => x._5.equals("1")).map(x => x._4.toInt).max
            (list.filter(x => x._4.toInt <= maxClickPos), 1)
          } else {
            (list, 0)
          }
        }).filter(x => x._2 != 0).map(x => x._1).flatMap(x => x)
        sampleFilter
      }).flatMap(x => x)

    val schema =
      StructType(
        StructField("user_id", StringType, true) ::
          StructField("item_id", StringType, true) ::
          StructField("time", StringType, true) ::
          StructField("pos", StringType, true) ::
          StructField("label", StringType, true) :: Nil)

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val sampleFinalDF = sqlContext.createDataFrame(sampleFinal.map(x => Row(x._1, x._2, sdf.format(x._3.toLong * 1000), Math.log(1 + x._4.toDouble).toString, x._5)), schema)
    return this.takeSample(sampleFinalDF)
  }

  def getOrderSampleLog(sqlContext: HiveContext, appIds: String*): DataFrame = {
    val orderSample = getSampleLog(sqlContext, "/sql/get_order_sample.sql", "app_id", appIds: _*)
    return orderSample
  }

  def getClickSampleLog(sqlContext: HiveContext, appIds: String*): DataFrame = {
    val clickSample = getSampleLog(sqlContext, "/sql/get_click_sample.sql", "app_id", appIds: _*)
    return clickSample
  }

  def getSampleLog(sqlContext: HiveContext, path: String, appId: String, appIds: String*): DataFrame = {
    val appIdSet = appIds.toSet
    val isContain = udf { (appId: String) => if (appIdSet.contains(appId)) true else false }
    val sql = SqlUtil.getSql(path)
    val sampleLog = sqlContext.sql(sql)
    val sampleLogFilter = sampleLog.filter(isContain(sampleLog(appId)))
    return sampleLogFilter
  }
}