package com.mgj.cf

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import com.mgj.utils.NormalizeUtil
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 1/30/16.
  */
object ItemGraphSimRawFeature {
  val const = 1e5d
  val N = 50

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("calculate item cf score")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // User click log: user_id, item_id, visit_time.
    val bizdate = args(1)
    val bizdateSub = args(0)
    val itemSimResultPath = args(2)
    val itemSimGlobalNormalizeResultPath = args(3)

    val clickLogSql = "select user_id, item_id, time, category_id from s_dg_user_base_log where pt >= '" + bizdateSub + "' and pt <= '" + bizdate + "' and action_type = 'click' and platform_type = 'app'"
    val userBaseLog = sqlContext.sql(clickLogSql).rdd.filter(r => r.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString)).repartition(500)

    // Count user click number.
    val userWeight = userBaseLog.map(x => (x._1, 1)).groupBy(x => x._1).map(x => (x._1, x._2.map(x => x._2).sum)).map(x => (x._1, x._2, 1.0 / Math.log(3 + x._2))).filter(x => x._2 <= 1000 && x._2 > 1)

    // item_id, item_weight, item_cnt.
    // sqrt(sum(user_weight*user_weight))
    val itemWeight = userBaseLog.map(x => (x._1, x._2)).join(userWeight.map(x => (x._1, x._3)))
      .map(x => (x._2._1, (x._2._2, x._1)))
      .groupBy(x => x._1).map(x => (x._1, x._2.toList.distinct.map(x => x._2._1 * x._2._1).sum, x._2.map(x => x._2._2).toSet.size))
      .map(x => (x._1, Math.sqrt(x._2), x._3)).filter(x => x._3 > 1)

    // user_id, item_id, visit_time, user_weight, item_weight, item_cnt,
    // userBaseLog: user_id, item_id, visit_time
    // userWeightLog: user_id, user_weight
    // item_id -> user_id, item_id, visit_time, user_weight
    // itemWeight: item_id, item_weight, cnt
    // Join.
    val userItemWeight = userBaseLog.map(x => (x._1, (x._2, x._3, x._4))).join(userWeight.map(x => (x._1, x._3)))
      .map(x => (x._2._1._1, (x._1, x._2._1._1, x._2._1._2, x._2._2, x._2._1._3))).join(itemWeight.map(x => (x._1, (x._2, x._3))))
      .map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2._1, x._2._2._2, x._2._1._5))
    userWeight.unpersist(blocking = false)
    itemWeight.unpersist(blocking = false)
    userBaseLog.unpersist(blocking = false)

    // user_id, item_id, visit_time, user_weight, item_weight, item_cnt
    // Generate item pair.
    def generatePair(x: Iterable[(String, String, String, Double, Double, Int, String)], alpha: Double, beta: Double, N: Int): Array[(String, String, Int, Int, Double, Double, Double)] = {
      def getTimeDiff(visitTimex: String, visitTimey: String): Double = {
        val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        val datex = df.parse(visitTimex)
        val datey = df.parse(visitTimey)
        return Math.abs(1.0 * (datex.getTime() - datey.getTime()) / 1000 / 60)
      }

      def compare(visitTimex: String, itemCntx: Int, visitTimey: String, itemCnty: Int): Boolean = {
        val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        val datex = df.parse(visitTimex)
        val datey = df.parse(visitTimey)

        if (itemCntx == itemCnty) {
          if (datex.after(datey)) {
            return true
          } else {
            return false
          }
        } else if (itemCntx > itemCnty) {
          return true
        } else {
          return false
        }
      }

      // user_id, item_id, visit_time, user_weight, item_weight, item_cnt
      val list = x.toList.sortWith((a, b) => compare(a._3, a._6, b._3, b._6) == true).groupBy(x => x._2).map(x => x._2.head).toList.sortWith((a, b) => compare(a._3, a._6, b._3, b._6) == true)

      val output: util.ArrayList[(String, String, Int, Int, Double, Double, Double)] = new util.ArrayList[(String, String, Int, Int, Double, Double, Double)]()
      for (ex <- list.take(N); ey <- list.take(N)) {
        if (ex._2 != ey._2 && ex._7 == ey._7) {
          output.add((ex._2, ey._2, ex._6, ey._6, ex._4 * ey._4 / (ex._5 * ey._5 * (1 + alpha * getTimeDiff(ex._3, ey._3))), 1.0 / (1 + beta * getTimeDiff(ex._3, ey._3)), (ex._4 + ey._4) / 2))
        }
      }

      return output.toList.toArray
    }

    // itemx, itemy, item_cntx, item_cnty, user_weight, weight_time.
    // Make item cross.
    val cfSimCross = userItemWeight.groupBy(_._1).map(x => generatePair(x._2, 1.0 / 24 / 60, 1.0 / 24 / 60, 50)).flatMap(x => x)
    userItemWeight.unpersist(blocking = false)

    // Get similar weight.
    def getWeight(values: Iterable[(String, String, Int, Int, Double, Double, Double)]): (String, String, Int, Int, Double, Double, Double, Double) = {
      val head = values.head
      val itemx = head._1
      val itemy = head._2
      val itemCntx = head._3
      val itemCnty = head._4
      val sum = values.toList.map(x => (x._5, 1.0, x._6, x._7)).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
      return (itemx, itemy, itemCntx, itemCnty, sum._1, Math.log(1.0 + sum._3) * sum._3 / (itemCntx.toDouble + itemCnty.toDouble - sum._3 + 10.0), Math.log(1 + sum._2) * sum._2 / (itemCntx.toDouble + itemCnty.toDouble - sum._2 + 10.0), sum._4 / Math.sqrt(itemCntx.toDouble * itemCnty.toDouble + 10.0))
    }

    val cfSim = cfSimCross.groupBy(x => x._2 + x._1).mapValues(x => getWeight(x)).map(x => x._2).cache()
    cfSimCross.unpersist(blocking = false)

    //    val summary = Statistics.colStats(cfSim.map(x => Vectors.dense(Array(x._5, x._6, x._7))))
    //    println("mean:" + summary.mean)
    //    println("max:" + summary.max)
    //    println("min:" + summary.min)

    val schema =
      StructType(
        StructField("itemx", StringType, true) ::
          StructField("itemy", StringType, true) ::
          StructField("item_cntx", StringType, true) ::
          StructField("item_cnty", StringType, true) ::
          StructField("sim_scoreA", StringType, true) ::
          StructField("sim_scoreB", StringType, true) ::
          StructField("sim_scoreC", StringType, true) ::
          StructField("sim_scoreD", StringType, true) ::
          Nil)

    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    val featureDF = sqlContext.createDataFrame(cfSim.map(x => Row(x._1, x._2, x._3.toString, x._4.toString, x._5.toString, x._6.toString, x._7.toString, x._8.toString)), schema)
    cfSim.unpersist(blocking = false)
    featureDF.registerTempTable("s_dg_item_sim_original_feature_temp")
    sqlContext.sql("drop table if exists s_dg_item_sim_original_feature")
    sqlContext.sql("create table s_dg_item_sim_original_feature as select * from s_dg_item_sim_original_feature_temp")

    def sort(x: Iterable[(String, String, Double)], N: Int): String = {
      val max = x.map(x => x._3).max
      val min = x.map(x => x._3).min
      val list = x.toList.sortWith((a, b) => a._3 > b._3).take(N).map(x => {
        val score = NormalizeUtil.minMaxScaler(min, max, x._3, 1d / const)
        x._2 + ":" + Math.round(score * const)
      }).mkString(",")
      return list
    }

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)

    cfSim.map(x => (x._1, x._2, x._6)).groupBy(_._1).map(x => x._1 + " " + sort(x._2, N)).saveAsTextFile(itemSimResultPath + "/" + sdf.format(calendar.getTime))

    val max = cfSim.map(x => x._6).max
    val min = cfSim.map(x => x._6).min
    cfSim.map(x => {
      val score = NormalizeUtil.minMaxScaler(min, max, x._6, 1d / const)
      (x._1, x._2, Math.round(score * const))
    }).groupBy(_._1).map(x => x._1 + " " + x._2.map(x => x._2 + ":" + x._3).take(N).mkString(",")).saveAsTextFile(itemSimGlobalNormalizeResultPath + "/" + sdf.format(calendar.getTime))
  }
}
