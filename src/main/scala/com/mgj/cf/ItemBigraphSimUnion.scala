package com.mgj.cf

/**
  * Created by xiaonuo on 9/9/15.
  */

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mgj.feature.FeatureType
import com.mgj.utils.{HiveUtil, NormalizeUtil}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ItemBigraphSimUnion {
  val const = 1e5d
  val N = 50

  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf().
      setAppName("bigraph sim").
      set("spark.sql.parquet.binaryAsString", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val inputPath = args(0)
    val outputPath = args(1)
    val outputGroupPath = args(2)
    val dayCount = args(3).toInt
    val outputGroupGlobalNormalizePath = args(4)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)
    var i2i = sc.textFile(inputPath + "/" + sdf.format(calendar.getTime)).map(x => ((x.split(" ")(0), x.split(" ")(1), x.split(" ")(2), 0)))
    for (i <- 2 to dayCount) {
      calendar.add(Calendar.DAY_OF_MONTH, -1)
      i2i = i2i.union(sc.textFile(inputPath + "/" + sdf.format(calendar.getTime)).map(x => ((x.split(" ")(0), x.split(" ")(1), x.split(" ")(2), i - 1)))).coalesce(2000)
      println(s"day count:${i}")
    }

    val i2iUnion = i2i.groupBy(x => (x._1, x._2)).map(x => {
      val score = x._2.map(x => x._3.toDouble * Math.pow(1.5, -x._4)).sum
      (x._1._1, x._1._2, score)
    })
    i2i.unpersist(blocking = false)
    //    val itemInfo = sqlContext.sql("select tradeitemid, cid from v_dw_trd_tradeitem").rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString))
    //    val i2iFilter = i2iUnion.map(x => (x._1, x))
    //      .join(itemInfo).map(x => (x._2._1._2, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2)))
    //      .join(itemInfo).map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2))
    //      .filter(x => x._4.equals(x._5))


    //    i2iFilter.map(x => x._1 + " " + x._2 + " " + x._3).saveAsTextFile(outputPath + "/" + sdf.format(calendarOutput.getTime))
    //    i2i.groupBy(x => x._1).map(x => x._1 + " " + x._2.toList.sortWith((a, b) => a._3 > b._3).map(x => x._2).mkString(" "))

    def sort(x: Iterable[(String, String, Double)], N: Int): String = {
      val max = x.map(x => x._3).max
      val min = x.map(x => x._3).min
      val list = x.toList.sortWith((a, b) => a._3 > b._3).take(N).map(x => {
        val score = NormalizeUtil.minMaxScaler(min, max, x._3, 1d / const)
        x._2 + ":" + Math.round(score * const)
      }).mkString(",")
      return list
    }

    val max = i2iUnion.map(x => x._3).max
    val min = i2iUnion.map(x => x._3).min

    println(s"max value:${max}")
    println(s"min value:${min}")

    val calendarOutput = Calendar.getInstance()
    calendarOutput.add(Calendar.DAY_OF_MONTH, -1)

    i2iUnion.map(x => {
      val score = NormalizeUtil.minMaxScaler(min, max, x._3, 0d)
      (x._1, x._2, score)
    }).groupBy(_._1).map(x => x._1 + " " + x._2.toList.sortWith((a, b) => a._3 > b._3).map(x => x._2 + ":" + x._3).take(N).mkString(",")).coalesce(2000).saveAsTextFile(outputGroupGlobalNormalizePath + "/" + sdf.format(calendarOutput.getTime))

    i2iUnion.map(x => x._1 + " " + x._2 + " " + x._3).saveAsTextFile(outputPath + "/" + sdf.format(calendarOutput.getTime))

    i2iUnion.map(x => (x._1, x._2, x._3)).groupBy(_._1).map(x => x._1 + " " + sort(x._2, N)).coalesce(2000).saveAsTextFile(outputGroupPath + "/" + sdf.format(calendarOutput.getTime))

    HiveUtil.featureHdfsToHive(sc, sqlContext, "item_sim", outputGroupPath + "/" + sdf.format(calendarOutput.getTime), sdf.format(calendarOutput.getTime), "s_dg_item_sim", FeatureType.ITEM)
  }
}
