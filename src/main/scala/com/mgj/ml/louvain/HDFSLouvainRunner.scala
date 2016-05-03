package com.mgj.ml.louvain

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * Execute the louvain algorithm and save the vertices and edges in hdfs at each level.
 * Can also save locally if in local mode.
 *
 * See LouvainHarness for algorithm details
 */
class HDFSLouvainRunner(minProgress: Int = 2000, progressCounter: Int = 1, outputDir: String) extends LouvainHarness(minProgress: Int, progressCounter: Int) {

  var qValues = Array[(Int, Double)]()
  var levelCounter = 0

  override def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]) = {
    graph.vertices.map(x => x._1 + " " + x._2.community).saveAsTextFile(outputDir + "/level_" + level + "_vertices")
    graph.edges.map(x => x.srcId + " " + x.dstId + " " + x.attr).saveAsTextFile(outputDir + "/level_" + level + "_edges")
    levelCounter += 1
    println(s"Q value: $q")
  }

  override def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]) = {
    var param = Map[String, String]()
    param += ("levelCounter" -> levelCounter.toString)
    sc.parallelize(param.toSeq.map(x => x._1 + ":" + x._2)).saveAsTextFile(outputDir + "/param")
  }
}