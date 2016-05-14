package com.mgj.cf.content

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mgj.utils.{NormalizeUtil, WordSegUtil}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by xiaonuo on 3/24/16.
  */
object ItemAllSimMerge {

  val const = 1e4
  val N = 50

  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("item sim merge")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("spark.driver.maxResultSize", "4g")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val itemBigraphSimPath = args(0)
    println(s"itemBigraphSimPath:${itemBigraphSimPath}")

    val itemSimPath = args(1)
    println(s"itemSimPath:${itemSimPath}")

    val wordSimPath = args(2)
    println(s"wordSimPath:${wordSimPath}")

    val idfPath = args(3)
    println(s"idfPath:${idfPath}")

    val dictPath = args(4)
    println(s"dictPath:${dictPath}")

    val wordTagPath = args(5)
    println(s"wordTagPath:${wordTagPath}")

    val outputPath = args(6)
    println(s"outputPath:${outputPath}")

    val w1 = args(7).toDouble
    println(s"w1:${w1}")

    val w2 = args(8).toDouble
    println(s"w2:${w2}")

    val w3 = args(9).toDouble
    println(s"w3:${w3}")

    val itemSim = sc.textFile(itemSimPath).map(x => {
      val itemx = x.split(" ")(0).toInt
      val list = x.split(" ")(1).split(",").map(x => (itemx, x.split(":")(0).toInt, x.split(":")(1).toDouble))
      list
    }).flatMap(x => x)

    val itemBigraphSim = sc.textFile(itemBigraphSimPath).map(x => {
      val itemx = x.split(" ")(0).toInt
      val list = x.split(" ")(1).split(",").map(x => (itemx, x.split(":")(0).toInt, x.split(":")(1).toDouble))
      list
    }).flatMap(x => x)

    val itemSimAvg = itemSim.map(x => (x._3, 1d)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val itemBigraphSimAvg = itemBigraphSim.map(x => (x._3, 1d)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    println(s"itemSimAvg:${itemSimAvg._1 / itemSimAvg._2}")
    println(s"itemBigraphSimAvg:${itemBigraphSimAvg._1 / itemBigraphSimAvg._2}")

    val itemSimMerge = itemBigraphSim.map(x => ((x._1, x._2), x._3)).fullOuterJoin(itemSim.map(x => ((x._1, x._2), x._3)))
      .map(x => (x._1._1, x._1._2, x._2._1.getOrElse(0d), x._2._2.getOrElse(0d)))

    val itemSet = itemSimMerge.map(x => Set(x._1, x._2)).flatMap(x => x).distinct()
    println(s"item count:${itemSet.count}")

    val itemTitleSeg = sqlContext.sql("select tradeitemid, title from v_dw_trd_tradeitem").map(x => (x(0).toString.toInt, x(1).toString)).join(itemSet.map(x => (x, 1))).map(x => (x._1, x._2._1)).collect()
      .map(x => (x._1, WordSegUtil.process(x._2).take(50))).toMap
    itemSet.unpersist(blocking = false)

    WordSegUtil.loadDict(dictPath)
    val wordTag = sc.textFile(wordTagPath).map(x => (x.split("@")(0), x.split("@")(1))).collect().toMap
    val wordSim = sc.textFile(wordSimPath).map(x => ((x.split(" ")(0), x.split(" ")(1)), x.split(" ")(2).toDouble)).groupBy(x => x._1._1).map(x => x._2.toList.sortWith((a, b) => a._2 > b._2).take(50)).flatMap(x => x).collect().toMap
    val wordIdf = sc.textFile(idfPath).map(x => (x.split(" ")(0), x.split(" ")(1).toDouble)).collect().toMap

    val itemSimWithContentTemp = itemSimMerge.map(x => {
      val itemx = x._1
      val itemy = x._2
      val score = GetSimUtil.getSimScore(wordSim, wordTag, wordIdf, itemTitleSeg.get(itemx).get, itemTitleSeg.get(itemy).get)
      (itemx, itemy, x._3, x._4, score)
    }).cache()
    itemSim.unpersist(blocking = false)

    val max = itemSimWithContentTemp.map(x => x._4).max
    val min = itemSimWithContentTemp.map(x => x._4).min

    val itemSimWithContent = itemSimWithContentTemp.map(x => {
      val score = (w1 * x._3 + w2 * x._4 + w3 * NormalizeUtil.minMaxScaler(min, max, x._5, 0d)) / (w1 + w2 + w3)
      (x._1, x._2, score)
    })
    itemSimWithContentTemp.unpersist(blocking = false)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)

    itemSimWithContent.groupBy(_._1).map(x => x._1 + " " + x._2.toList.sortWith((a, b) => a._3 > b._3).map(x => x._2 + ":" + x._3).mkString(",")).saveAsTextFile(outputPath)
    //    itemSimMerge.map(x => (x._1, x._2, (w1 * x._3 + w2 * x._4) / (w1 + w2))).groupBy(_._1).map(x => x._1 + " " + x._2.toList.sortWith((a, b) => a._3 > b._3).map(x => x._2 + ":" + x._3).mkString(",")).saveAsTextFile(outputPath)
    //    itemSimMerge.groupBy(_._1).map(x => x._1 + " " + x._2.toList.sortWith((a, b) => a._3 > b._3).map(x => x._2 + ":" + x._3).mkString(" ")).saveAsTextFile(outputPath + "/" + sdf.format(calendar.getTime))
  }
}