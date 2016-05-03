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

    val itemSim = sc.textFile(itemSimPath).map(x => (x.split(" ")(0).toInt, x.split(" ")(1).split(",").map(x => (x.split(":")(0).toInt, x.split(":")(1).toDouble)))).coalesce(2000)

    val itemSet = itemSim.map(x => (Set(x._1) | x._2.map(x => x._1).toSet)).flatMap(x => x).distinct()
    println(s"item count:${itemSet.count}")

    val itemTitleSeg = sqlContext.sql("select tradeitemid, title from v_dw_trd_tradeitem").map(x => (x(0).toString.toInt, x(1).toString)).join(itemSet.map(x => (x, 1))).map(x => (x._1, x._2._1)).collect()
      .map(x => (x._1, WordSegUtil.process(x._2).take(50))).toMap
    itemSet.unpersist(blocking = false)

    WordSegUtil.loadDict(dictPath)
    val wordTag = sc.textFile(wordTagPath).map(x => (x.split("@")(0), x.split("@")(1))).collect().toMap
    val wordSim = sc.textFile(wordSimPath).map(x => ((x.split(" ")(0), x.split(" ")(1)), x.split(" ")(2).toDouble)).groupBy(x => x._1._1).map(x => x._2.toList.sortWith((a, b) => a._2 > b._2).take(100)).flatMap(x => x).collect().toMap
    val wordIdf = sc.textFile(idfPath).map(x => (x.split(" ")(0), x.split(" ")(1).toDouble)).collect().toMap

    def contentMerge(x: (Int, Array[(Int, Double)]), w1: Double, w2: Double): (Int, Array[(Int, Double)]) = {
      val itemx = x._1
      val listMerge = x._2.toList.map(x => (itemx, x._1, x._2, GetSimUtil.getSimScore(wordSim, wordTag, wordIdf, itemTitleSeg.get(itemx).get, itemTitleSeg.get(x._1).get)))
      val maxItemSim = listMerge.map(x => x._3).max
      val minItemSim = listMerge.map(x => x._3).min
      val maxWordSim = listMerge.map(x => x._4).max
      val minWordSim = listMerge.map(x => x._4).min

      val list = listMerge.map(x => {
        val scoreItemSim = NormalizeUtil.minMaxScaler(minItemSim, maxItemSim, x._3, 1d / const)
        val scoreWordSim = NormalizeUtil.minMaxScaler(minWordSim, maxWordSim, x._4, 1d / const)
        (x._2, Math.round(const * (w1 * scoreItemSim + w2 * scoreWordSim) / (w1 + w2)).toDouble)
      }).sortWith((a, b) => a._2 > b._2)
      (itemx, list.take(N).toArray)
    }

    val itemSimWithContent = itemSim.map(x => contentMerge(x, w1, w2))
    itemSim.unpersist(blocking = false)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)
    itemSimWithContent.map(x => x._1 + " " + x._2.map(x => x._1 + ":" + x._2).mkString(",")).saveAsTextFile(outputPath)
  }
}