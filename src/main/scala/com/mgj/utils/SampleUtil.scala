package com.mgj.utils

import java.math.BigInteger
import java.text.SimpleDateFormat
import java.util

import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 4/12/16.
  */
object SampleUtil {
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

    val exposeLog = this.getExposeLog(sqlContext, appIds: _*)
    val clickLog = this.getClickLog(sqlContext, appIds: _*)
    val exposeFilterCondition = exposeLog.columns.map(x => x + " is not null").mkString(" and ")
    val clickFilterCondition = clickLog.columns.map(x => x + " is not null").mkString(" and ")
    val exposeLogFilter = exposeLog.where(exposeFilterCondition)
    val clickLogFilter = clickLog.where(clickFilterCondition)
    println("click log")
    clickLogFilter.show()
    println("expose log")
    exposeLogFilter.show()
    val sample = exposeLogFilter.join(clickLogFilter, exposeLogFilter("request_id") === clickLogFilter("request_id_click") && exposeLogFilter("entity_id") === clickLogFilter("entity_id_click"), "left_outer")
    val sampleDroped = sample
      .drop("device_id_click")
      .drop("user_id_click")
      .drop("entity_id_click")
      .drop("app_id_click")
      .drop("request_id_click")
    val sampleWithLabel = sampleDroped.select("user_id", "entity_id", "expose_time", "click_time", "pos").rdd.map(x => {
      val label = if (x(3) == null || x(3) == None) "0" else "1"
      val clickTime = if (x(3) == null || x(3) == None) x(2).toString else x(3).toString
      (x(0).toString, x(1).toString, x(2).toString, clickTime, x(4).toString, label)
    })

    // user_id, entity_id, expose_time, click_time, pos, label.
    // skip above.

    val sampleFinal = sampleWithLabel
      .groupBy(x => x._1)
      .map(x => {
        // sort with visit time.
        val sampleList = x._2.toList.sortWith((a, b) => a._4.toLong > b._4.toLong)

        // time stamp id.
        var timeId = 0

        // last visit time.
        var lastTime = sampleList.head._4.toLong

        // make time stamp id.
        val sampleWithTimeId = new util.ArrayList[((String, String, String, String, String, String), Int)]()
        for (e <- sampleList) {
          val currentTime = e._4.toLong
          if (Math.abs(currentTime - lastTime) / 60 > 30) {
            timeId += 1
          }
          lastTime = currentTime
          sampleWithTimeId.add((e, timeId))
        }

        // skip above.
        val sampleFilter = sampleWithTimeId.groupBy(x => x._2).map(x => {
          val list = x._2.toList.map(x => x._1)
          if (list.filter(x => x._6.equals("1")).size > 0) {
            val maxClickPos = list.filter(x => x._6.equals("1")).map(x => x._5.toInt).max
            (list.filter(x => x._5.toInt <= maxClickPos), 1)
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
    val sampleFinalDF = sqlContext.createDataFrame(sampleFinal.map(x => Row(x._1, x._2, sdf.format(x._4.toLong * 1000), Math.log(1 + x._5.toDouble).toString, x._6)), schema)

    return this.takeSample(sampleFinalDF)
  }

  def getExposeLog(sqlContext: HiveContext, appIds: String*): DataFrame = {
    val exposeLog = getLog(sqlContext, "/sql/get_expose_entity.sql", "app_id", appIds: _*)
    return exposeLog
  }

  def getClickLog(sqlContext: HiveContext, appIds: String*): DataFrame = {
    val clickLog = getLog(sqlContext, "/sql/get_click_entity.sql", "app_id_click", appIds: _*)
    return clickLog
  }

  def decode(text: String): String = {
    if (text == null || text.isEmpty() || text.length() <= 1 || text.equals("null")) {
      return null
    }
    val idStr = text.substring(1)
    if (WordUtil.isDigitalOrLetter(idStr)) {
      val id = (NumberUtils.toLong(new BigInteger(idStr, 36).toString(10)) - 56) / 2
      return id.toString
    } else {
      return null
    }
  }

  def encode(text: String): String = {
    return 1 + Integer.toString(text.toInt * 2 + 56, 36)
  }

  def getAppId(text: String): String = {
    if (text == null || text == "") {
      return null
    }
    val pattern = """\d+\.[-\w]+\.[-\w]+\.[-\w]+\.[-\w]+\.[-\w]+\.([-\w]+)""".r
    val acm = pattern.findFirstIn(text)
    if (acm != None) {
      val pattern(appId) = acm.get
      val idPattern = """(\d+)[-\w]*""".r
      val flag = idPattern.findFirstIn(appId)
      if (flag != None) {
        val idPattern(id) = flag.get
        return id
      } else {
        return null
      }
    } else {
      return null
    }
  }

  def getRequestId(text: String): String = {
    if (text == null || text == "") {
      return null
    }
    val pattern = """\d+\.[-\w]+\.[-\w]+\.[-\w]+\.[-\w]+\.([-\w]+)\.[-\w]+""".r
    val acm = pattern.findFirstIn(text)
    if (acm != None) {
      val pattern(requestId) = acm.get
      return requestId
    } else {
      return null
    }
  }

  private def getLog(sqlContext: HiveContext, path: String, appId: String, appIds: String*): DataFrame = {
    sqlContext.udf.register("decode", (text: String) => this.decode(text))
    sqlContext.udf.register("get_app_id", (text: String) => this.getAppId(text))
    sqlContext.udf.register("get_request_id", (text: String) => this.getRequestId(text))
    val appIdSet = appIds.toSet
    val isContain = udf { (appId: String) => if (appIdSet.contains(appId)) true else false }
    val sql = SqlUtil.getSampleSql(path)
    val log = sqlContext.sql(sql)
    val logFilter = log.filter(isContain(log(appId)))
    return logFilter
  }
}
