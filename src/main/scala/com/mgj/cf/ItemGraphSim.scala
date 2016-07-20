package com.mgj.cf

import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, WordDictionary}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 12/28/15.
  */
object ItemGraphSim {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("calculate item graph")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    // User click log: user_id, item_id, visit_time, category_level1.
    val bizdate = args(1)
    val bizdateSub = args(0)
    val dictPath = args(3)
    val ctrDiff = args(4).toDouble
    val itemCtrPath = args(6)

    //    val dict = WordDictionary.getInstance()
    //    val path = Paths.get(dictPath)
    //    dict.loadUserDict(path)
    //    val segmenter = sc.broadcast(new JiebaSegmenter())

    val userClickLogSql = "select device_id, item_id, time from s_dg_user_base_log where pt >= '" + bizdateSub + "' and pt <= '" + bizdate + "' and action_type = 'click' and platform_type = 'app'"
    val userBaseLog = sqlContext.sql(userClickLogSql).rdd.filter(r => r.anyNull == false).map(x => (x(0), x(1), x(2)))
    //    val itemInfo = sqlContext.sql("select tradeitemid, cid, title from v_dw_trd_tradeitem").map(x => (x(0), x(1), x(2))).filter(x => x._1 != null)
    val itemInfo = sqlContext.sql("select tradeitemid, cid from v_dw_trd_tradeitem").map(x => (x(0), x(1))).filter(x => x._1 != null)
    val itemCtr = sc.textFile(itemCtrPath).map(x => (x.split(" ")(0), x.split(" ")(1).toDouble / 1000000d))
    val userClickLogBase = userBaseLog.map(x => (x._2.toString, x)).join(itemInfo.map(x => (x._1.toString, x._2))).map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2))
    val userClickLog = userClickLogBase.map(x => (x._2.toString, x)).leftOuterJoin(itemCtr).map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2.getOrElse(0d)))
    itemCtr.unpersist(blocking = false)
    itemInfo.unpersist(blocking = false)
    userBaseLog.unpersist(blocking = false)

    // Count user click number.
    val userClickCount = userClickLog.map(x => (x._1, 1)).groupBy(x => x._1).map(x => (x._1, x._2.map(x => x._2).sum))
    val itemClickCount = userClickLog.map(x => (x._2, x._1.toString)).groupBy(x => x._1).map(x => (x._1, x._2.map(x => x._2).toSet.size))

    val userClickLogFilter = userClickLog.map(x => (x._1.toString, x)).join(userClickCount.map(x => (x._1.toString, x._2))).filter(x => x._2._2 < 500 && x._2._2 > 1)
      .map(x => (x._2._1._2.toString, x._2._1)).join(itemClickCount.map(x => (x._1.toString, x._2))).filter(x => x._2._2 > 1).map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._2))
    userClickLog.unpersist(blocking = false)

    // user_id, item_id, visit_time, category_id_level1, item_cnt
    // Generate item pair.
    def generatePair(x: Iterable[(Any, Any, Any, Any, Double, Int)], alpha: Double, beta: Double, N: Int, ctrDiff: Double): Array[(Any, Any, Int, Int, Double, Double)] = {
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

      // user_id, item_id, visit_time, category_id_level1, item_cnt
      val list = x.toList.sortWith((a, b) => compare(a._3.toString, a._6, b._3.toString, b._6) == true).groupBy(x => x._2).map(x => x._2.head).toList.sortWith((a, b) => compare(a._3.toString, a._6, b._3.toString, b._6) == true)

      val output: util.ArrayList[(Any, Any, Int, Int, Double, Double)] = new util.ArrayList[(Any, Any, Int, Int, Double, Double)]()
      for (ex <- list; ey <- list) {
        //        val setA = ex._5.toSet
        //        val setB = ey._5.toSet
        //        val common = setA & setB
        //        val union = setA | setB
        if (ex._2 != ey._2 && ex._4 == ey._4 && Math.abs(ex._5 - ey._5) < ctrDiff) {
          output.add((ex._2, ey._2, ex._6, ey._6, 1.0 / (1 + beta * getTimeDiff(ex._3.toString, ey._3.toString)), 1))
        }
      }

      return output.toList.toArray
    }

    // itemx, itemy, item_cntx, item_cnty, user_weight.
    // Make item cross.
    val cfSimCross = userClickLogFilter.groupBy(_._1).map(x => generatePair(x._2, 1.0 / 24 / 60, 1.0 / 24 / 60, 50, ctrDiff)).flatMap(x => x)
    cfSimCross.take(10).foreach(println)
    userClickLogFilter.unpersist(blocking = false)

    // Get similar weight.
    def getWeight(values: Iterable[(Any, Any, Int, Int, Double, Double)]): (Any, Any, Int, Int, Double, Double, Double, Double) = {
      val sum = values.map(x => (1, x._5)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      val item = values.head
      val scoreA = Math.log(1 + sum._2) * sum._2 / (item._3 + item._4 - sum._2 + 10)
      val scoreB = Math.log(1 + sum._1) * sum._1 / (item._3 + item._4 - sum._1 + 10)
      return (item._1, item._2, item._3, item._4, scoreA, scoreB, scoreA * item._6, scoreB * item._6)
    }

    val cfSim = cfSimCross.groupBy(x => x._2.toString() + x._1.toString()).mapValues(x => getWeight(x)).map(x => x._2)
    cfSim.take(10).foreach(println)
    cfSimCross.unpersist(blocking = false)

    //    val schema =
    //      StructType(
    //        StructField("itemx", StringType, true) ::
    //          StructField("itemy", StringType, true) ::
    //          StructField("item_cntx", IntegerType, true) ::
    //          StructField("item_cnty", IntegerType, true) ::
    //          StructField("sim_scoreA", DoubleType, true) ::
    //          StructField("sim_scoreB", DoubleType, true) ::
    //          StructField("sim_scoreC", DoubleType, true) ::
    //          StructField("sim_scoreD", DoubleType, true) :: Nil)
    //
    //    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    //    sqlContext.createDataFrame(cfSim.map(x => Row(x._1.toString, x._2.toString, x._3, x._4, x._5, x._6, x._7, x._8)), schema).registerTempTable("s_dg_item_sim_graph_with_title_temp")
    //    cfSim.unpersist(blocking = false)
    //    sqlContext.sql("drop table if exists s_dg_item_sim_graph_with_title")
    //    sqlContext.sql("create table s_dg_item_sim_graph_with_title as select * from s_dg_item_sim_graph_with_title_temp")
    //
    //    val outputDir = args(2)
    //    val outputDirV2 = args(5)
    //    sqlContext.sql("select * from s_dg_item_sim_graph_with_title").rdd.map(x => {
    //      var score = Math.round(x(4).toString.toDouble * 10000d)
    //      if (score <= 0) {
    //        score = 1
    //      }
    //      x(0).toString + " " + x(1).toString + " " + score.toString
    //    }).saveAsTextFile(outputDir)
    //    sqlContext.sql("select * from s_dg_item_sim_graph_with_title").rdd.map(x => {
    //      var score = Math.round(x(6).toString.toDouble * 1000000d)
    //      if (score <= 0) {
    //        score = 1
    //      }
    //      x(0).toString + " " + x(1).toString + " " + score.toString
    //    }).saveAsTextFile(outputDirV2)
    val outputDir = args(2)
    val outputDirV2 = args(5)
    cfSim.map(x => {
      var score = Math.round(x._7.toString.toDouble * 10000d)
      if (score <= 0) {
        score = 1
      }
      x._1.toString + " " + x._2.toString + " " + score.toString
    }).saveAsTextFile(outputDirV2)

    //    cfSim.map(x => {
    //      var score = Math.round(x._5.toString.toDouble * 10000d)
    //      if (score <= 0) {
    //        score = 1
    //      }
    //      x._1.toString + " " + x._2.toString + " " + score.toString
    //    }).saveAsTextFile(outputDir)
  }
}
