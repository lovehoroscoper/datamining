package com.mgj.ml.nlp

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import com.mgj.utils.{TfIdfUtil, WordSegUtil}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 1/5/16.
  */
object UserWordPrefer {
  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("calculate user word prefer")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val dictPath = args(0)
    val resultPath = args(1)
    val bizdate = args(2)
    val bizdateSub = args(3)
    val resultIdfPath = args(4)

    val clickSql = "select user_id, item_id, time, category_id from s_dg_user_base_log where pt >= '" + bizdateSub + "' and pt <= '" + bizdate + "' and action_type = 'click' and platform_type = 'app'"
    val userBaseLog = sqlContext.sql(clickSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString))

    val itemSet = userBaseLog.map(x => x._2).distinct()
    val itemTitle = sqlContext.sql("select tradeitemid, title from v_dw_trd_tradeitem").map(x => (x(0).toString, x(1).toString))

    WordSegUtil.loadDict(dictPath)
    val itemTitleSeg = itemSet.map(x => (x, 1)).join(itemTitle).map(x => (x._1, x._2._2)).collect.map(x => (x._1, WordSegUtil.process(x._2)))

    val itemTitleSegRDD = sc.parallelize(itemTitleSeg)
    val userBaseLogWithSeg = userBaseLog.map(x => (x._2, x)).join(itemTitleSegRDD).map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2, x._2._1._4))

    val doc = userBaseLogWithSeg.groupBy(x => x._1).filter(x => x._2.size <= 100).map(x => {
      def getTimeDiff(visitTimex: String, visitTimey: String): Double = {
        val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        val datex = sdf.parse(visitTimex)
        val datey = sdf.parse(visitTimey)
        return Math.abs(1.0 * (datex.getTime() - datey.getTime()) / 1000 / 60)
      }

      val clickLog = x._2.toList.sortWith((a, b) => a._3 > b._3)

      val docMap = new util.HashMap[Int, util.ArrayList[String]]()
      val docCount = new util.HashMap[Int, Int]()

      //      var k = 0
      //      docMap.put(k, new util.ArrayList[String]())
      //      docMap.get(k).addAll(clickLog.apply(0)._4)
      //      docCount.put(k, 1)
      //      var time = clickLog.apply(0)._3
      //      for (i <- 1 to clickLog.size - 1) {
      //        val e = clickLog.apply(i)
      //        if (getTimeDiff(time, e._3) <= 30) {
      //          val cnt = docCount.get(k)
      //          docCount.put(k, cnt + 1)
      //          docMap.get(k).addAll(e._4)
      //        } else {
      //          k += 1
      //          docMap.put(k, new util.ArrayList[String]())
      //          docMap.get(k).addAll(e._4)
      //          docCount.put(k, 1)
      //        }
      //        time = e._3
      //      }
      //
      //      for (e <- docMap) yield {
      //        (x._1 + e._1, e._2, docCount.get(e._1))
      //      }

      val data = clickLog.groupBy(x => x._5).map(x => (x._1, x._2.map(x => x._4))).map(x => (x._1, x._2.flatMap(x => x)))
      for (e <- data) yield {
        (x._1 + e._1, e._2, e._2.size)
      }
    }).flatMap(x => x).filter(x => x._3 >= 10).map(x => (x._1, x._2))

    val (tfidf, tf, idf) = TfIdfUtil.getTfIdf(doc)
    tfidf.take(10).foreach(println)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)

    tfidf.groupBy(x => x._1).map(x => x._1 + " " + x._2.toList.sortWith((a, b) => a._3 > b._3).map(x => x._2 + ":" + x._3).mkString(",")).saveAsTextFile(resultPath)
    idf.map(x => x._1 + " " + x._2).saveAsTextFile(resultIdfPath + "/" + sdf.format(calendar.getTime))
  }
}
