package com.mgj.cf

import java.text.SimpleDateFormat
import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 12/26/15.
  */
object ItemGraph {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("calculate item graph")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    val bigraphSimPath = args(0)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val originalSimFeature = sqlContext.sql("select * from s_dg_item_sim_original_feature")
      .map(x => (x(0).toString, x(1).toString, x(2).toString.toInt, x(3).toString.toInt, x(4).toString.toDouble, x(5).toString.toDouble, x(6).toString.toDouble, x(7).toString.toDouble))

    val bigraphSimFeature = sc.textFile(bigraphSimPath).map(x => (x.split(" ")(0), x.split(" ")(1), x.split(" ")(2).toDouble))
    //    val bigraphSimFeatureCut = bigraphSimFeature.groupBy(x => x._1).map(x => x._2.toList.sortWith((a, b) => a._3 > b._3).take(50)).flatMap(x => x)
    val rawFeatureMerge = originalSimFeature.map(x => (x._1 + x._2, x)).fullOuterJoin(bigraphSimFeature.map(x => (x._1 + x._2, x)))
      .map(x => {
        if (x._2._1 != None && x._2._2 != None) {
          (x._2._1.get._1, x._2._1.get._2, x._2._1.get._5 * Math.random(), x._2._1.get._6 * Math.random(), x._2._1.get._7 * Math.random(), x._2._1.get._8, x._2._2.get._3)
        } else if (x._2._1 != None && x._2._2 == None) {
          (x._2._1.get._1, x._2._1.get._2, x._2._1.get._5 * Math.random(), x._2._1.get._6 * Math.random(), x._2._1.get._7 * Math.random(), x._2._1.get._8, 0d)
        } else {
          (x._2._2.get._1, x._2._2.get._2, 0d, 0d, 0d, 0d, x._2._2.get._3)
        }
      })

    val schema =
      StructType(
        StructField("itemx", StringType, true) ::
          StructField("itemy", StringType, true) ::
          StructField("sim_scoreA", StringType, true) ::
          StructField("sim_scoreB", StringType, true) ::
          StructField("sim_scoreC", StringType, true) ::
          StructField("sim_scoreD", StringType, true) ::
          StructField("sim_scoreE", StringType, true) ::
          Nil)

    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    val featureDF = sqlContext.createDataFrame(rawFeatureMerge.map(x => Row(x._1, x._2, x._3.toString, x._4.toString, x._5.toString, x._6.toString, x._7.toString)), schema)
    rawFeatureMerge.unpersist(blocking = false)
    featureDF.registerTempTable("s_dg_item_sim_raw_feature_temp")
    sqlContext.sql("drop table if exists s_dg_item_sim_raw_feature")
    sqlContext.sql("create table s_dg_item_sim_raw_feature as select * from s_dg_item_sim_raw_feature_temp")
  }
}
