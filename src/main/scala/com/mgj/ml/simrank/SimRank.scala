package com.mgj.ml.simrank

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, HashPartitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object SimRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("similarity rank")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("spark.driver.maxResultSize", "2g")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val graphSim = sqlContext.sql("select * from s_dg_item_sim_merge").map(x => (x(0).toString.toInt, x(1).toString.toInt, x(2).toString.toDouble))

    val edgeRDD = graphSim.groupBy(x => x._1).map(x => {
      val list = x._2.toList.sortWith((a, b) => {
        if (a._3 == b._3) {
          a._2 > b._2
        } else {
          a._3 > b._3
        }
      }).take(50)
      (x._1, list)
    }).flatMap(x => x._2).map(x => new Edge(x._1, x._2, x._3))

    println("edge count: " + edgeRDD.count)
    val graph = Graph.fromEdges(edgeRDD, None)

    //    graph.vertices
    //    graph.edges
    //    graph.pageRank()
    //    graph.mapTriplets()
    val nodeWeights = graph.aggregateMessages[Double](
      triplet => {
        triplet.sendToDst(triplet.attr)
        triplet.sendToSrc(triplet.attr)
      },
      (a, b) => a + b
    )

    //      val adj = accumulateEdges(e, vertex._1)
    //        .map(x => x.srcId)
    //        .toSeq
    //      val candidates = adj cross adj
    //      candidates.filter(pair => pair._1 != pair._2)
    //        .map(pair => {
    //          (pair, 1)
    //        })
    sc.textFile("itemSim/2016-02-21").filter(x => x.split(" ")(0).equals("260020611")).map(x => (x.split(" ")(0) + " " + x.split(" ")(1).split(",").map(x => x.split(":")(0)).mkString(" ")))
  }
}
