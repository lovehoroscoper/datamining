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

    println(s"itemBigraphSimPath:${itemBigraphSimPath}")
    println(s"itemSimPath:${itemSimPath}")
    println(s"outputPath:${outputPath}")

    val itemSim = sc.textFile(itemSimPath).map(x => (x.split(" ")(0).toInt, x.split(" ")(1).split(",").map(x => (x.split(":")(0).toInt, x.split(":")(1).toDouble)))).coalesce(2000)
    val itemBigraphSim = sc.textFile(itemBigraphSimPath).map(x => (x.split(" ")(0).toInt, x.split(" ")(1).split(",").map(x => (x.split(":")(0).toInt, x.split(":")(1).toDouble)))).coalesce(2000)

    val itemSimMerge = itemSim.fullOuterJoin(itemBigraphSim).map(x => {
      val itemx = x._1
      val listA = x._2._1
      val listB = x._2._2
      if (listA != None && listB != None) {
        val set = (listA.get ++ listB.get).map(x => x._1).toSet
        val mapA = listA.get.toMap
        val mapB = listB.get.toMap
        val list = set.map(key => {
          val scoreA = mapA.getOrElse(key, 0d)
          val scoreB = mapB.getOrElse(key, 0d)
          val w1 = 1d
          val w2 = 10d
          val value = (w1 * scoreA + w2 * scoreB) / (w1 + w2)
          (key, value)
        }).toList.sortWith((a, b) => a._2 > b._2)

        // normalize
        val max = list.map(x => x._2).max
        val min = list.map(x => x._2).min

        (itemx, list.map(x => {
          val score = NormalizeUtil.minMaxScaler(min, max, x._2, 1d / const)
          (x._1, Math.round(score * const))
        }).take(N))
      } else if (listA == None) {
        (itemx, listB.get.toList.map(x => (x._1, Math.round(x._2.toDouble / 10))).take(N))
      } else {
        (itemx, listA.get.toList.map(x => (x._1, Math.round(x._2.toDouble / 10))).take(N))
      }
    })

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)
    itemSimMerge.map(x => x._1 + " " + x._2.map(x => x._1 + ":" + x._2).mkString(" ")).saveAsTextFile(outputPath + "/" + sdf.format(calendar.getTime))
  }
}