package com.mgj.ml.pagerank

import org.apache.spark.graphx.{Graph, Edge, GraphLoader}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by xiaonuo on 12/26/15.
 */
object PageRank {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("item ranking")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("es.read.metadata", "true")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new HiveContext(sc)

    val outputDir = args(0)
    val inputDir = args(1)

    val data = sc.textFile(inputDir).map(x => {
      val meta = x.split(" ")
      meta.length match {
        case 2 => (meta(0).toLong, meta(1).toLong, 1L)
        case 3 => (meta(0).toLong, meta(1).toLong, meta(2).toLong)
        case _ => throw new IllegalArgumentException("invalid input line: " + meta)
      }
    })

    val edge = data.map(x => (x._1, x)).groupByKey().map(x => {
      val list = x._2.toList.sortWith((a, b) => {
        if (a._3 == b._3) {
          a._2 > b._2
        } else {
          a._3 > b._3
        }
      }).take(50)
      (x._1, list)
    }).flatMap(x => x._2).map(x => new Edge(x._1, x._2, x._3))

    val graph = Graph.fromEdges(edge, None)

    val ranks = graph.pageRank(0.0001).vertices.sortBy(x => x._2, false)
    ranks.take(10).foreach(println)
    ranks.map(x => x._1 + " " + x._2).saveAsTextFile(outputDir)

  }
}
