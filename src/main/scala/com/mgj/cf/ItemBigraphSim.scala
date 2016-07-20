package com.mgj.cf

/**
  * Created by xiaonuo on 9/9/15.
  */

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import com.mgj.utils.PartitionUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._

object ItemBigraphSim {
  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf().
      setAppName("bigraph sim").
      set("spark.sql.parquet.binaryAsString", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    val bizdate = args(1)
    val bizdateSub = args(0)
    val outputPath = args(2)
    val N = 60
    //        val bizdate = "2016-03-11"
    //        val bizdateSub = "2016-03-11"

    // user_id, item_id, time
    PartitionUtil.checkAppLog(sqlContext, bizdate, "click")
    val clickLogSql = "select user_id, item_id, time, category_id from s_dg_user_base_log where pt >= '" + bizdateSub + "' and pt <= '" + bizdate + "' and action_type = 'click' and platform_type = 'app'"
    val userBaseLog = sqlContext.sql(clickLogSql).rdd.filter(r => r.anyNull == false).map(x => (x(0).toString.toInt, x(1).toString.toInt, x(2).toString, x(3).toString.toInt)).repartition(2000)
    //    println("user base log count:" + userBaseLog.count())

    // user_id, item_count.
    val userItemSet = userBaseLog.groupBy(_._1).map(x => (x._1, x._2.map(x => x._2).toSet)).filter(x => x._2.size <= N && x._2.size > 1)
    val userItemSetLocal = userItemSet.collect.toMap

    // user_id, item_id, time.
    // Filter user who behaves bad.
    val userItemLog = userBaseLog.map(x => (x._1, x)).join(userItemSet.map(x => (x._1, 1))).map(x => x._2._1)
    userItemSet.unpersist(blocking = false)

    // Item user set.
    val itemUserSet = userItemLog.groupBy(_._2).map(x => (x._1, x._2.map(x => x._1).toSet)).filter(x => x._2.size > 1)

    val userItemLogWithSet = userItemLog.map(x => (x._2, x)).join(itemUserSet).map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2))
    itemUserSet.unpersist(blocking = false)

    // Generate item pair.
    // user_id, item_id, visit_time, category_id.
    def generatePair(x: Iterable[(Int, Int, String, Int, Set[Int])], N: Int): Array[(Int, Int, Set[Int], Int)] = {

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

      // user_id, item_id, visit_time, category_id, item_user_set. session 1h
      val list = x.toList.sortWith((a, b) => compare(a._3, a._5.size, b._3, b._5.size) == true).distinct
      val output = new util.ArrayList[(Int, Int, Set[Int], Int)]()

      for (ex <- list.take(N); ey <- list.take(N)) {
        if (ex._2 != ey._2 && getTimeDiff(ex._3, ey._3) <= 60) {
          val commonUserSet = ex._5 & ey._5
          if (commonUserSet.size > 1 && ex._4 == ey._4) {
            output.add((ex._2, ey._2, commonUserSet, ey._5.size))
          }
        }
      }

      return output.toList.toArray
    }

    val i2iPair = userItemLogWithSet.groupBy(_._1).map(x => generatePair(x._2, N)).flatMap(x => x).groupBy(x => (x._1, x._2)).map(x => x._2.head)
    //    i2iPair.take(10).foreach(println)
    //    println("pair count:" + i2iPair.count)

    def getWeight(x: (Int, Int, Set[Int], Int), alpha: Double, beta: Double): (Int, Int, Double) = {
      val commonUserSet = x._3
      val yUV = x._4

      var sum: Double = 0
      for (ux <- commonUserSet; uy <- commonUserSet) {
        if (ux != uy) {
          val uxSet = userItemSetLocal.get(ux).toSet
          val uySet = userItemSetLocal.get(uy).toSet
          val commonSize = (uxSet & uySet).size
          sum += 1.0 / (Math.sqrt((alpha + uxSet.size) * (alpha + uySet.size))) * 1 / (alpha + commonSize) * 1 / Math.pow(yUV, beta)
        }
      }
      return (x._1, x._2, sum)
    }

    val i2iScore = i2iPair.map(x => getWeight(x, 10, 0.316)).filter(x => x._3 > 0)
    i2iPair.unpersist(blocking = false)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)
    i2iScore.map(x => x._1 + " " + x._2 + " " + x._3).saveAsTextFile(outputPath + "/" + sdf.format(calendar.getTime))
  }
}
