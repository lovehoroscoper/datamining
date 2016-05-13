package com.mgj.cf.content

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mgj.utils.{NormalizeUtil, KMUtil, WordSegUtil}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by xiaonuo on 3/24/16.
  */
object ItemSimContentMerge {

  val const = 1e5
  val N = 100

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

    val itemSimPath = args(0)
    val wordSimPath = args(1)
    val idfPath = args(2)
    val dictPath = args(3)
    val wordTagPath = args(4)
    val outputPath = args(5)
    val w1 = args(6).toDouble
    val w2 = args(7).toDouble

    println(s"itemSimPath:${itemSimPath}")
    println(s"wordSimPath:${wordSimPath}")
    println(s"idfPath:${idfPath}")
    println(s"wordTagPath:${wordTagPath}")
    println(s"outputPath:${outputPath}")
    println(s"w1:${w1}")
    println(s"w2:${w2}")

    val itemSim = sc.textFile(itemSimPath).map(x => (x.split(" ")(0).toInt, x.split(" ")(1).split(",").map(x => (x.split(":")(0).toInt, x.split(":")(1).toDouble)))).repartition(2000)

    val itemSet = itemSim.map(x => (Set(x._1) | x._2.map(x => x._1).toSet)).flatMap(x => x).distinct()
    println(s"item count:${itemSet.count}")

    val itemTitleSeg = sqlContext.sql("select tradeitemid, title from v_dw_trd_tradeitem").map(x => (x(0).toString.toInt, x(1).toString)).join(itemSet.map(x => (x, 1))).map(x => (x._1, x._2._1)).collect()
      .map(x => (x._1, WordSegUtil.process(x._2).take(50))).toMap
    itemSet.unpersist(blocking = false)

    WordSegUtil.loadDict(dictPath)
    val wordTag = sc.textFile(wordTagPath).map(x => (x.split("@")(0), x.split("@")(1))).collect().toMap
    val wordSim = sc.textFile(wordSimPath).map(x => ((x.split(" ")(0), x.split(" ")(1)), x.split(" ")(2).toDouble)).groupBy(x => x._1._1).map(x => x._2.toList.sortWith((a, b) => a._2 > b._2).take(50)).flatMap(x => x).collect().toMap
    val wordIdf = sc.textFile(idfPath).map(x => (x.split(" ")(0), x.split(" ")(1).toDouble)).collect().toMap

    var itemSimWithContent = itemSim.map(x => x._2.map(t => (x._1, t._1, t._2))).flatMap(x => x).map(x => {
      val itemx = x._1
      val itemy = x._2
      val score = GetSimUtil.getSimScore(wordSim, wordTag, wordIdf, itemTitleSeg.get(itemx).get, itemTitleSeg.get(itemy).get)
      (itemx, itemy, x._3, score)
    }).cache()
    itemSim.unpersist(blocking = false)

    val max = itemSimWithContent.map(x => x._4).max
    val min = itemSimWithContent.map(x => x._4).min

    itemSimWithContent = itemSimWithContent.map(x => (x._1, x._2, x._3, NormalizeUtil.minMaxScaler(min, max, x._4.toDouble, 0d)))
    println("item sim with content")
    itemSimWithContent.take(10).foreach(println)

    println(s"content sim max value:${max}")
    println(s"content sim min value:${min}")

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)

    itemSimWithContent.map(x => (x._1, x._2, (w1 * x._3 + w2 * x._4) / (w1 + w2))).groupBy(_._1).map(x => x._1 + " " + x._2.map(x => x._2 + ":" + x._3).mkString(",")).saveAsTextFile(outputPath)
  }
}