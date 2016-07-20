package com.mgj.useritemprefer

/**
  * Created by xiaonuo on 8/27/15.
  */

import java.text.SimpleDateFormat
import java.util

import com.mgj.utils.{NormalizeUtil, PartitionUtil}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Calendar, Date}
import scala.collection.JavaConversions._

object UserItemHistoryPrefer {
  val const = 1e5

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("calculate user history item prefer")
      .set("spark.sql.parquet.binaryAsString", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    // User click log: user_id, item_id, visit_time.
    val biztime = args(0)
    val bizdate = args(2)
    val bizdateSub = args(1)
    val outputPath = args(3)
    val outputGroupPath = args(4)

    PartitionUtil.checkAppLog(sqlContext, bizdate, "click")
    val itemViewSql = "select user_id, item_id, time, category_id from s_dg_user_base_log where pt >= '" + bizdateSub + "' and pt <= '" + bizdate + "' and action_type='click' and platform_type='app'"
    val userBaseLog = sqlContext.sql(itemViewSql).rdd.filter(r => r.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString))

    // User click log: user_id, item_id, visit_time, category_id.
    val userClickLog = userBaseLog.map(x => (x._1, x._2, x._3, x._4))
    userBaseLog.unpersist(blocking = false)

    // max_visit_time, category_count, day_count.
    def getCategoryInfo(x: Iterable[(String, String, String, String)]): (String, String, String, Int, Int) = {
      val sdfTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val sdfDate = new SimpleDateFormat("yyyy-MM-dd")
      var maxVisitTime = new Date()
      val set = new util.HashSet[String]()
      val userId = x.head._1
      val categoryId = x.head._4
      var count = 0
      for (e <- x) {
        val visitTime = e._3
        val time = sdfTime.parse(visitTime)
        val date = sdfDate.format(time)
        count += 1
        set.add(date)
        if (time.after(maxVisitTime)) {
          maxVisitTime = time
        }
      }

      return (userId, categoryId, sdfTime.format(maxVisitTime), count, set.size)
    }

    // User category info: user_id, category_id, max_visit_time, category_count, day_count
    val userCategoryInfo = userClickLog.groupBy(x => (x._1 + x._4)).map(x => getCategoryInfo(x._2))

    // User click log: user_id, item_id, visit_time, category_id.
    // User category info: user_id, category_id, max_visit_time, category_count, day_count.
    // user_id, item_id, category_id, visit_time, max_visit_time, category_count, day_count.
    val userItemPreferLog = userCategoryInfo.map(x => (x._1 + x._2, x)).join(userClickLog.map(x => (x._1 + x._4, x))).map(x => (x._2._1._1, x._2._2._2, x._2._1._2, x._2._2._3, x._2._1._3, x._2._1._4, x._2._1._5))
    userClickLog.unpersist(blocking = false)
    userCategoryInfo.unpersist(blocking = false)

    def getPreferItem(x: Iterable[(String, String, String, String, String, Int, Int)], date: String, N: Int, M: Int): List[(String, String, Long)] = {
      // user_id, item_id, category_id, visit_time, max_visit_time, category_count, day_count.
      def getScore(x: (String, String, String, String, String, Int, Int), date: String): Double = {
        // hour.
        def getTimeDiff(visitTimex: String, visitTimey: String): Double = {
          val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
          val datex = df.parse(visitTimex)
          val datey = df.parse(visitTimey)
          return Math.abs(1.0 * (datex.getTime() - datey.getTime()) / 1000 / 60 / 60)
        }

        // category_count*day_count/(1+abs(current_date-max_visit_time))/(1+abs(current_date-visit_time)).
        return x._6 * x._7 / (1 + Math.abs(getTimeDiff(date, x._5))) / (1 + Math.abs(getTimeDiff(date, x._4)))
      }

      // user_id, item_id, category_id.
      val list = x.toList.map(x => (x._1, x._2, x._3, getScore(x, date))).sortWith((a, b) => a._4 > b._4)
      val max = list.map(x => x._4).max
      val min = list.map(x => x._4).min

      // normalize.
      val listNormalize = list.map(x => {
        val score = NormalizeUtil.minMaxLogScaler(min, max, x._4, 1 / const)
        (x._1, x._2, x._3, score)
      })

      val map = new util.HashMap[String, Int]()
      val set = new util.HashSet[String]()
      val result = new util.ArrayList[(String, String, Long)]()
      val userId = x.head._1
      for (e <- listNormalize) {
        val categoryId = e._3
        val itemId = e._2
        if (!map.containsKey(categoryId)) {
          map.put(categoryId, 1)
        }

        if (map.get(categoryId) < M && !set.contains(itemId)) {
          set.add(itemId)
          val score = Math.round(e._4 * const)
          result.add((userId, itemId, score))
          if (result.size >= N) {
            return result.toList
          }
          map.put(categoryId, map.get(categoryId) + 1)
        }
      }

      return result.toList
    }

    // date must match the format yyyy-MM-dd hh:mm:ss.
    val userItemPrefer = userItemPreferLog.groupBy(_._1).map(x => (x._1, getPreferItem(x._2, biztime, 50, 10)))
    userItemPreferLog.unpersist(blocking = false)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)
    userItemPrefer.flatMap(x => x._2).map(x => x._1 + "\t" + x._2 + "\t" + x._3).saveAsTextFile(outputPath + "/" + sdf.format(calendar.getTime))
    userItemPrefer.map(x => x._1 + " " + x._2.map(x => x._2 + ":" + x._3).mkString(",")).saveAsTextFile(outputGroupPath)
    //    dataToRedis.take(30).foreach({ case (key, value) =>
    //      println("digu_offline_" + key.toString + ":" + value.toString)
    //    })
    //    println("Data to redis...")
    //
    //    dataToRedis.foreachPartition(iter => {
    //      lazy val jedis = new Jedis("chenyang.cache.mogujie.org", 6379)
    //
    //      lazy val pipline = {
    //        jedis.select(99)
    //        jedis.connect()
    //        jedis.pipelined()
    //      }
    //
    //      iter.foreach {
    //        case (key, value) =>
    //          // jedis.set("digu_offline_" + key.toString, value.toString)
    //          pipline.set("digu_offline_" + key.toString, value.toString)
    //      }
    //
    //      pipline.sync()
    //    })

    val count = userItemPrefer.count()
    println("record number: " + count)
  }
}
