package com.mgj.ml.nlp

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 3/24/16.
  */
object WordSim {
  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("query classify")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    val inputPath = args(0)
    val outputPath = args(1)
    val outputGroupPath = args(2)

    val userBaseLog = sc.textFile(inputPath).map(x =>
      x.split(" ")(1).split(",").map(t => (x.split(" ")(0), t.split(":")(0).trim, t.split(":")(1).toDouble))
    ).flatMap(x => x).repartition(2000)

    val itemWeight = userBaseLog.groupBy(x => x._2).map(x => (x._1, x._2.map(x => x._3 * x._3).sum)).map(x => (x._1, Math.sqrt(x._2)))

    // Join.
    val userItemWeight = userBaseLog.map(x => (x._2, (x._1, x._3))).join(itemWeight.map(x => (x._1, x._2))).map(x => (x._2._1._1, x._1, x._2._1._2, x._2._2))
    itemWeight.unpersist(blocking = false)
    userBaseLog.unpersist(blocking = false)

    // Generate item pair.
    def generatePair(x: Iterable[(String, String, Double, Double)], N: Int): Array[(String, String, Double)] = {

      // user_id, item_id, visit_time, user_weight, item_weight, item_cnt
      val list = x.toList.sortWith((a, b) => a._3 > b._3).groupBy(x => x._2).map(x => x._2.head).toList.sortWith((a, b) => a._3 > b._3 || a._3 == b._3 && a._4 > b._4)

      val output: util.ArrayList[(String, String, Double)] = new util.ArrayList[(String, String, Double)]()
      for (ex <- list.take(N); ey <- list.take(N)) {
        if (ex._2 != ey._2) {
          output.add((ex._2, ey._2, ex._3 * ey._3 / (ex._4 * ey._4)))
        }
      }

      return output.toList.toArray
    }

    // Make item cross.
    val simPair = userItemWeight.groupBy(_._1).map(x => generatePair(x._2, 100)).flatMap(x => x)
    userItemWeight.unpersist(blocking = false)

    // Get similar weight.
    def getWeight(values: Iterable[(String, String, Double)]): (String, String, Double) = {
      val head = values.head
      val itemx = head._1
      val itemy = head._2
      return (itemx, itemy, values.toList.map(x => x._3).sum)
    }

    val simWeight = simPair.groupBy(x => x._2 + x._1).mapValues(x => getWeight(x)).map(x => x._2).cache()

    //    val sim = simWeight.groupBy(x => x._1).map(x => x._2.toList.sortWith((a, b) => a._3 > b._3).take(100))
    //
    //    val docNum = sim.count()
    //    println("doc number: " + docNum)
    //
    //    val idf = sim.flatMap(x => x).map(x => (x._1, x._2)).distinct().map(x => (x._2, 1)).reduceByKey(_ + _).map(x => {
    //      (x._1, Math.log((1.0 * docNum.toDouble + 1.0) / (1.0 * x._2.toDouble + 1.0)))
    //    }).collect().toMap
    //
    //    val simIdf = sim.map(x => {
    //      (x.head._1, x.map(x => (x._2, x._3 * idf.get(x._2).get)).sortWith((a, b) => a._2 > b._2).take(50))
    //    })

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)

    simWeight.map(x => x._1 + " " + x._2 + " " + x._3).saveAsTextFile(outputPath + "/" + sdf.format(calendar.getTime))
    simWeight.groupBy(x => x._1).map(x => x._1 + " " + x._2.toList.sortWith((a, b) => a._3 > b._3).take(50).map(x => x._2 + ":" + x._3).mkString(",")).saveAsTextFile(outputGroupPath + "/" + sdf.format(calendar.getTime))
    //    simIdf.map(x => x._1 + " " + x._2.map(x => x._1 + ":" + x._2).mkString(",")).saveAsTextFile(outputPath + "/" + sdf.format(calendar.getTime))
    //    sim.map(x => (x.head._1, x.map(x => (x._2, x._3)))).map(x => x._1 + " " + x._2.map(x => x._1 + ":" + x._2).mkString(",")).saveAsTextFile(outputPath + "/" + sdf.format(calendar.getTime))
    //    sim.msc.textFile("wordSim/2016-03-27").filter(x => x.split(" ")(0).equals("印花")).take(100).foreach(println)ap(x => {
    //      (x.head._1, x.map(x => (x._2, x._3)).take(50))
    //    }).map(x => x._1 + " " + x._2.take(50).map(x => x._1 + ":" + x._2).mkString(",")).saveAsTextFile("temp")
    //    simWeight.map(x => x._1 + " " + x._2 + " " + x._3).saveAsTextFile("simWeight")
  }
}
