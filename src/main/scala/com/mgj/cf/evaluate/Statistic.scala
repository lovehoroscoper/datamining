package com.mgj.cf.evaluate

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by xiaonuo on 9/9/15.
  */
object Statistic {

  def main(args: Array[String]) = {
    val conf = new SparkConf().
      setAppName("dump item cf score").
      set("spark.sql.parquet.binaryAsString", "true")

    val filePath = args(0)

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val data = sc.textFile(filePath).map(x => (x.split(" ")(0), x.split(" ")(1).split(",").toList.map(x => x.split(":")(0))))

    val bizdate = args(2)
    val bizdateSub = args(1)
    val N = args(3).toInt

    val clickLogSql = "select user_id, item_id, time from s_dg_user_base_log where pt >= '" + bizdateSub + "' and pt <= '" + bizdate + "' and action_type = 'click' and platform_type = 'app'"
    val userBaseLog = sqlContext.sql(clickLogSql).rdd.filter(r => r.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString))

    // User click log: user_id, item_id, visit_time.
    def getEvaluateSet(x: Iterable[(String, String, String)], N: Int): (String, Set[String]) = {
      def compare(visitTimex: String, visitTimey: String): Boolean = {
        val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        val datex = df.parse(visitTimex)
        val datey = df.parse(visitTimey)

        if (datex.before(datey)) {
          return true
        } else {
          return false
        }
      }

      val itemList = x.map(x => (x._2, x._3)).toList.sortWith((a, b) => compare(a._2, b._2) == true).map(x => x._1)
      if (itemList.size < 2) {
        return (itemList(0), Set[String]())
      } else if (itemList.size >= 2 && itemList.size <= N) {
        return (itemList(0), itemList.toSet - itemList(0))
      } else {
        val num: Int = (itemList.size - N) / 2
        return (itemList(num), itemList.take(num + N).takeRight(N).toSet - itemList(num))
      }
    }

    val predictSequence = data.map(x => (x._1, x._2.take(N).toSet))
    val viewSequence = userBaseLog.groupBy(_._1).map(x => getEvaluateSet(x._2, 10))

    def getResult(x: ((String, Set[String]), (String, Set[String]))): (Int, Int, Int) = {
      val hit = (x._1._2 & x._2._2).size
      val predict = x._1._2.size
      val test = x._2._2.size
      return (hit, predict, test)
    }

    val temp = predictSequence.map(x => (x._1.toString, x)).join(viewSequence.map(x => (x._1.toString, x))).map(x => getResult(x._2))
    val count = temp.count()
    if (count > 0) {
      val result = temp.reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
      val precision = 1.0 * result._1 / result._2
      val recall = 1.0 * result._1 / result._3
      println("Evaluate result: ")
      println("item number: " + count)
      println("hit: " + result._1)
      println("precision: " + precision)
      println("recall: " + recall)
      println("F1 score: " + 2 * precision * recall / (precision + recall))
    } else {
      println("there is no item matched")
    }
  }
}
