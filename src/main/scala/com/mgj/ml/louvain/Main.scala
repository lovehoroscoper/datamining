package com.mgj.ml.louvain

import java.util

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import scala.collection.JavaConversions._

object Main {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("calculate item graph")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("es.read.metadata", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new HiveContext(sc)

    val outputDir = args(0)
    val inputDir = args(1)
    val inputDirSupplement = args(2)
    val N = args(3).toInt

    val data = sc.textFile(inputDir).map(x => {
      val itemx = x.split(" ")(0)
      x.split(" ")(1).split(",").toList.map(x => {
        var score = x.split(":")(1).toLong
        if (score == 0l) {
          score = 1l
        }
        (itemx.toLong, x.split(":")(0).toLong, score)
      })
    }).flatMap(x => x).repartition(500)

    val dataSupplement = sc.textFile(inputDirSupplement).map(x => {
      val itemx = x.split(" ")(0)
      x.split(" ")(1).split(",").toList.map(x => {
        var score = x.split(":")(1).toLong
        if (score == 0l) {
          score = 1l
        }
        (itemx.toLong, x.split(":")(0).toLong, score)
      })
    }).flatMap(x => x).repartition(500)

    var edgeRDD = data.map(x => (x, 1)).union(dataSupplement.map(x => (x, 2))).groupBy(x => x._1._1).map(x => {
      val list = x._2.toList.sortWith((a, b) => a._2 < b._2 || a._2 == b._2 && a._1._3 > b._1._3).map(x => x._1)
      val result = new util.ArrayList[(Long, Long, Long)]()
      val set = new util.HashSet[Long]()
      var j = 1l
      for (i <- 0 to list.size - 1) {
        val element = list.apply(i)
        if (!set.contains(element._2) && set.size <= N) {
          result.add((element._1, element._2, j))
          j += 1
          set.add(element._2)
        }
      }
      result.map(x => (x._1, x._2, 1000l / x._3))
    }).flatMap(x => x).map(x => new Edge(x._1, x._2, x._3))

    // note: not score but the distance.
    //    var edgeRDD = data.map(x => (x._1, x)).groupByKey().map(x => {
    //      val list = x._2.toList.sortWith((a, b) => {
    //        if (a._3 == b._3) {
    //          a._2 > b._2
    //        } else {
    //          a._3 > b._3
    //        }
    //      }).take(50)
    //      (x._1, list)
    //    }).flatMap(x => x._2).map(x => new Edge(x._1, x._2, x._3))

    // var edgeRDD = data.map(x => new Edge(x._1, x._2, x._3))

    println("edge count: " + edgeRDD.count)
    // if the parallelism option was set map the input to the correct number of partitions,
    // otherwise parallelism will be based off number of HDFS blocks
    val parallelism = -1
    if (parallelism != -1) edgeRDD = edgeRDD.coalesce(parallelism, shuffle = true)

    // create the graph
    val graph = Graph.fromEdges(edgeRDD, None)

    // use a helper class to execute the louvain
    // algorithm and save the output.
    // to change the outputs you can extend LouvainRunner.scala

    val minProgress = 2000
    val progressCounter = 1
    val runner = new HDFSLouvainRunner(minProgress, progressCounter, outputDir)
    runner.run(sc, graph)
  }
}