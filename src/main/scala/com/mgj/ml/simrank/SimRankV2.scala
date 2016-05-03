package com.mgj.ml.simrank

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
  * Created by xiaonuo on 2/18/16.
  */
object SimRankV2 {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("similarity rank")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("spark.driver.maxResultSize", "4g")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val N = 2

    var graphSim = sqlContext.sql("select * from s_dg_item_sim_merge").sample(false, 1e-1).map(x => {
      val itemx = x(0).toString.toInt
      val itemy = x(1).toString.toInt

      //      if (itemx == itemy) {
      //        ((itemx, itemy), (x(2).toString.toDouble, true))
      //      } else {
      //        ((itemx, itemy), (x(2).toString.toDouble, false))
      //      }
      ((itemx, itemy), x(2).toString.toDouble)
    }).cache()

    val graphLocal = graphSim.collect().groupBy(x => x._1._1)
      .map(x => (x._1, x._2.sortWith((a, b) => (a._2 > b._2)).take(25).map(x => x._1._2).toSet))
    //      .map(x => (x._1, x._2.sortWith((a, b) => (a._2._1 > b._2._1)).take(25).map(x => x._1._2).toSet))

    println("graph local:")
    graphLocal.take(10).foreach(println)

    val graph = sc.broadcast(graphLocal)
    println("graph broadcast:")
    graph.value.take(10).foreach(println)

    for (i <- 1 to N) {
      println("iteration " + i)
      graphSim = graphSim.map(x => {
        val setA = graph.value.getOrElse(x._1._1, Set[Int]())
        val setB = graph.value.getOrElse(x._1._2, Set[Int]())

        for (u <- setA; v <- setB) yield {
          //          if (v == u) {
          //            ((u, v), (1, true))
          //          } else {
          //            ((u, v), (x._2._1 * 0.8 / (setA.size * setB.size), false))
          //          }
          ((u, v), x._2 * 0.8 / (setA.size * setB.size))
        }
      }).flatMap(x => x).union(graphSim)
        .combineByKey[Double](
        (v: Double) => v,
        (c: Double, v: Double) => c + v,
        (c1: Double, c2: Double) => c1 + c2
      ).coalesce(500, false)
      //        .combineByKey[(Double, Boolean)](
      //        (v: (Double, Boolean)) => v,
      //        (c: (Double, Boolean), v: (Double, Boolean)) => if (c._2) (1, true) else (c._1 + v._1, false),
      //        (c1: (Double, Boolean), c2: (Double, Boolean)) => if (c1._2) (1, true) else (c1._1 + c2._1, false)
      //      )
      graphSim.take(10).foreach(println)
    }

    println("final:")
    //    graphSim.map(x => (x._1._1, x._1._2, x._2)).take(10).foreach(println)
    graphSim.map(x => (x._1._1 + " " + x._1._2 + " " + x._2)).saveAsTextFile("/user/digu/temp2")
    //    graphSim.map(x => x._1+" "+x._2+" "+x._3)
    //    sc.textFile("/user/digu/temp").map(x => (x.split(" ")(0), x.split(" ")(1), x.split(" ")(2).toDouble)).groupBy(x => x._1)
    //      .map(x => (x._1, x._2.toList.sortWith((a, b) => a._3 > b._3).map(x => x._2).take(30).mkString(" ")))
  }
}
