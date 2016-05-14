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
    val itemSimPath = args(1)
    val outputPath = args(2)
    var w1 = args(3).toDouble
    var w2 = args(4).toDouble

    println(s"itemBigraphSimPath:${itemBigraphSimPath}")
    println(s"itemSimPath:${itemSimPath}")
    println(s"outputPath:${outputPath}")
    println(s"w1:${w1}")
    println(s"w2:${w2}")

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

    w1 /= itemBigraphSimAvg._1 / itemBigraphSimAvg._2
    w2 /= itemSimAvg._1 / itemSimAvg._2
    val itemSimMerge = itemBigraphSim.map(x => ((x._1, x._2), x._3)).fullOuterJoin(itemSim.map(x => ((x._1, x._2), x._3)))
      .map(x => {
        val scoreA = x._2._1.getOrElse(0d)
        val scoreB = x._2._2.getOrElse(0d)
        (x._1._1, x._1._2, (w1 * scoreA + w2 * scoreB) / (w1 + w2))
      })

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)

    itemSimMerge.groupBy(_._1).map(x => x._1 + " " + x._2.toList.sortWith((a, b) => a._3 > b._3).map(x => x._2 + ":" + x._3).mkString(" ")).saveAsTextFile(outputPath)
    //    itemSimMerge.groupBy(_._1).map(x => x._1 + " " + x._2.toList.sortWith((a, b) => a._3 > b._3).map(x => x._2 + ":" + x._3).mkString(" ")).saveAsTextFile(outputPath + "/" + sdf.format(calendar.getTime))
  }
}