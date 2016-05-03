package com.mgj.cf

import java.util

import com.mgj.utils.PartitionUtil
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 1/30/16.
  */
object ItemGraphSimFeature {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("calculate item cf score")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val bigraphSimPath = args(0)

    // User click log: user_id, item_id, visit_time.

    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    val rawFeature = sqlContext.sql("select * from s_dg_item_sim_original_feature").map(x => (x(0).toString, x(1).toString, x(2).toString.toInt, x(3).toString.toInt, x(4).toString.toDouble, x(5).toString.toDouble, x(6).toString.toDouble, x(7).toString.toDouble))
    rawFeature.take(10).foreach(println)

    val bigraphSim = sc.textFile(bigraphSimPath).map(x => (x.split(" ")(0), x.split(" ")(1), x.split(" ")(2).toDouble))
    val rawFeatureMerge = rawFeature.map(x => (x._1 + x._2, x)).fullOuterJoin(bigraphSim.map(x => (x._1 + x._2, x)))
      .map(x => {
        if (x._2._1 != None && x._2._2 != None) {
          (x._2._1.get._1, x._2._1.get._2, x._2._1.get._3, x._2._1.get._4, x._2._1.get._5, x._2._1.get._6, x._2._1.get._7, x._2._1.get._8, x._2._2.get._3)
        } else if (x._2._1 != None && x._2._2 == None) {
          (x._2._1.get._1, x._2._1.get._2, x._2._1.get._3, x._2._1.get._4, x._2._1.get._5, x._2._1.get._6, x._2._1.get._7, x._2._1.get._8, 0d)
        } else {
          (x._2._2.get._1, x._2._2.get._2, 1, 1, 0d, 0d, 0d, 0d, x._2._2.get._3)
        }
      })

    // Join item infomation.
    val itemSet = rawFeatureMerge.map(x => List(x._1, x._2)).flatMap(x => x).distinct().map(x => (x, 1))
    println("item set size:" + itemSet.count())
    val maxpt = PartitionUtil.getMaxPt(sqlContext, "dw_trd_tradeitem_snapshot")
    val itemInfo = sqlContext.sql("select tradeitemid, cid, price, discount from dw_trd_tradeitem_snapshot where inst_date = '" + maxpt + "'").rdd.filter(x => x.anyNull == false).repartition(200)
      .map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString, (x(2).toString.toDouble * x(3).toString.toDouble).toString)).map(x => (x._1, x)).join(itemSet).map(x => x._2._1)
    itemInfo.take(10).foreach(println)

    def isEqual(ex: String, ey: String): Double = {
      if (ex.equals(ey)) {
        return 1d
      } else {
        return 0d
      }
    }

    def dist(ex: String, ey: String): Double = {
      return Math.abs(ex.toDouble - ey.toDouble)
    }

    def distLog(ex: String, ey: String): Double = {
      return Math.log(1 + Math.abs(ex.toDouble - ey.toDouble))
    }

    def distDecay(ex: String, ey: String): Double = {
      return 1 / (1 + Math.abs(ex.toDouble - ey.toDouble))
    }

    // tradeitemid, cid, price, discount, price*discount
    val cfSimFeature = rawFeatureMerge.map(x => (x._1.toString(), x)).join(itemInfo.map(x => (x._1.toString(), x)))
      .map(x => (x._2._1._2.toString(), (x._2._1, x._2._2))).join(itemInfo.map(x => (x._1.toString(), x))).map(x => (x._2._1._1, x._2._1._2, x._2._2))
      .map(x => (x._1._1, x._1._2, Array(distLog(x._1._3.toString, x._1._4.toString), x._1._5, x._1._6, x._1._7, x._1._8, x._1._9, isEqual(x._2._2, x._3._2), distLog(x._2._3, x._3._3), dist(x._2._4, x._3._4), distLog(x._2._5, x._3._5))))
    cfSimFeature.take(10).foreach(println)

    val schema =
      StructType(
        StructField("itemx", StringType, true) ::
          StructField("itemy", StringType, true) ::
          StructField("feature", StringType, true) :: Nil)

    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    sqlContext.createDataFrame(cfSimFeature.map(x => Row(x._1, x._2, util.Arrays.toString(x._3))), schema).registerTempTable("s_dg_item_sim_feature_temp")
    cfSimFeature.unpersist(blocking = false)
    sqlContext.sql("drop table if exists s_dg_item_sim_feature")
    sqlContext.sql("create table s_dg_item_sim_feature as select * from s_dg_item_sim_feature_temp")
  }
}
