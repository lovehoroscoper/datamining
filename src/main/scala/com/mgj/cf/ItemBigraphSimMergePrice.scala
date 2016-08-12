package com.mgj.cf

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import com.google.gson.Gson
import com.mgj.utils.NormalizeUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaonuo on 3/24/16.
  */
object ItemBigraphSimMergePrice {
  val const = 1e5d
  val N = 50

  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("bigraph sim")
      .set("spark.sql.parquet.binaryAsString", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    val itemSimPath = args(0)
    val itemBigraphSimPath = args(1)
    val outputPath = args(2)
    val dumpOutputPath = args(3)
    val pricePath = args(4)

    println(s"itemSimPath:${itemSimPath}")
    println(s"itemBigraphSimPath:${itemBigraphSimPath}")

    val priceMap = sc.textFile(pricePath).map(x => (x.split(" ")(0).toInt, x.split(" ")(1).toInt)).collect().toMap

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
          val w1 = 1
          val w2 = 10
          val value = (w1 * scoreA + w2 * scoreB) / (w1 + w2)
          (key, value)
        }).toList.sortWith((a, b) => a._2 > b._2)

        // normalize
        val max = list.map(x => x._2).max
        val min = list.map(x => x._2).min

        (itemx, list.map(x => {
          val score = NormalizeUtil.minMaxScaler(min, max, x._2, 1d / const)
          (x._1, Math.round(score * const))
        }).filter(x => priceMap.contains(itemx) && priceMap.contains(x._1) && priceMap.get(itemx).get <= priceMap.get(x._1).get).take(N))
      } else if (listA == None) {
        (itemx, listB.get.toList.filter(x => priceMap.contains(itemx) && priceMap.contains(x._1) && priceMap.get(itemx).get <= priceMap.get(x._1).get).map(x => (x._1, x._2.toInt)).take(N))
      } else {
        (itemx, listA.get.toList.filter(x => priceMap.contains(itemx) && priceMap.contains(x._1) && priceMap.get(itemx).get <= priceMap.get(x._1).get).map(x => (x._1, x._2.toInt)).take(N))
      }
    }).filter(x => x._2.size > 0)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)

    itemSimMerge.map(x => x._1 + " " + x._2.map(x => {
      val score = if (x._2 == 0l) 1 else x._2
      x._1 + ":" + score
    }).mkString(",")).saveAsTextFile(outputPath + "/" + sdf.format(calendar.getTime))

    // dump to search.
    itemSimMerge.map(x => {
      val gson = new Gson()
      val list = new util.ArrayList[Int]()
      x._2.foreach(x => list.add(x._1))
      x._2.foreach(x => if (x._2 == 0l) list.add(1) else list.add(x._2.toString.toInt))
      x._1 + " " + gson.toJson(list)
    }).saveAsTextFile(dumpOutputPath)
  }
}
