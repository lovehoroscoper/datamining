package com.mgj.ml.nlp

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.graphx._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable

/**
  * Created by xiaonuo on 4/14/16.
  */
object LPA {
  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("LPA")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    val wordTag = args(0)
    val wordSim = args(1)
    val outputPath = args(2)
    val tagType = args(3).split(",").toSet

    println("tag type")
    tagType.foreach(println)

    val tagTypeShare = sc.broadcast(tagType)
    val baseTag = sc.textFile(wordTag).filter(x => {
      val temp = x.split("@")(1)
      tagTypeShare.value.contains(temp)
    })
    println(s"baseTag count:${baseTag.count()}")

    val tagId = baseTag.map(x => x.split("@")(1)).distinct().sortBy(x => x).zipWithIndex().collect().toMap

    val wordTagTrain = baseTag.map(x => (x.split("@")(0), tagId.get(x.split("@")(1)).get)).collect().toMap

    val wordId = sc.textFile(wordSim).map(x => List(x.split(" ")(0), x.split(" ")(1))).flatMap(x => x).distinct().sortBy(x => x).zipWithIndex().collect.toMap

    val edgeRDD = sc.textFile(wordSim).map(x => (x.split(" ")(0), x.split(" ")(1), x.split(" ")(2))).distinct().map(x => {
      new Edge(wordId.get(x._1).get, wordId.get(x._2).get, x._3.toDouble)
    })

    val vertexRDD = sc.parallelize(wordId.toSeq).map(x => {
      val tag = wordTagTrain.get(x._1)
      val tagIdMap = if (tag == None) Map[Long, Double]() else Map[Long, Double](tag.get -> 1d)
      val tagIdEntry = if (tagIdMap.isEmpty) (-1l, 0d) else tagIdMap.maxBy(x => x._2)
      (x._2, (tagIdMap, tagIdEntry, tag != None))
    })

    val graph: Graph[(Map[Long, Double], (Long, Double), Boolean), Double] = Graph(vertexRDD, edgeRDD)
    var tagRatio = graph.vertices.map(x => if (x._2._2._1 == -1l) (0, 1) else (1, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    println(s"tag number:${tagRatio._1}")
    println(s"word number:${tagRatio._2}")

    def sendMessage(edge: EdgeTriplet[(Map[Long, Double], (Long, Double), Boolean), Double]): Iterator[(VertexId, mutable.Map[Long, Double])] = {
      if (edge.dstAttr._2._1 == -1l && edge.srcAttr._2._1 == -1l) {
        Iterator.empty
      } else if (edge.dstAttr._2._1 == -1l) {
        Iterator((edge.dstId, mutable.Map[Long, Double](edge.srcAttr._2._1 -> edge.srcAttr._2._2 * edge.attr)))
      } else if (edge.srcAttr._2._1 == -1l) {
        Iterator((edge.srcId, mutable.Map[Long, Double](edge.dstAttr._2._1 -> edge.dstAttr._2._2 * edge.attr)))
      } else {
        Iterator((edge.srcId, mutable.Map[Long, Double](edge.dstAttr._2._1 -> edge.dstAttr._2._2 * edge.attr)), (edge.dstId, mutable.Map[Long, Double](edge.srcAttr._2._1 -> edge.srcAttr._2._2 * edge.attr)))
      }
    }

    def mergeMessage(attrA: mutable.Map[Long, Double], attrB: mutable.Map[Long, Double]): mutable.Map[Long, Double] = {
      val result = (attrA.keySet ++ attrB.keySet).map(key => {
        val valA = attrA.get(key).getOrElse(0d)
        val valB = attrB.get(key).getOrElse(0d)
        key -> (valA + valB)
      }).toMap
      mutable.Map(result.toSeq: _*)
    }

    def vertexProgram(vid: VertexId, attr: (Map[Long, Double], (Long, Double), Boolean), msg: mutable.Map[Long, Double]): (Map[Long, Double], (Long, Double), Boolean) = {
      if (msg.isEmpty || attr._3) {
        attr
      } else {
        val attrMap = attr._1.toMap
        val msgMap = msg.toMap
        val mergeMap = (attrMap.keySet ++ msgMap.keySet).map(key => {
          val valA = attrMap.get(key).getOrElse(0d)
          val valB = msgMap.get(key).getOrElse(0d)
          key -> (valA + valB)
        }).toMap
        val maxEntry = mergeMap.maxBy(x => x._2)
        val newAttr = mergeMap.toList.sortWith((a, b) => a._2 > b._2).take(3).toMap
        (newAttr, maxEntry, false)
      }
    }

    val result = Pregel(graph, mutable.Map[Long, Double](), maxIterations = 2)(vertexProgram, sendMessage, mergeMessage)
    tagRatio = result.vertices.map(x => if (x._2._2._1 == -1l) (0, 1) else (1, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    println(s"tag number:${tagRatio._1}")
    println(s"word number:${tagRatio._2}")

    val id2tag = tagId.map(x => (x._2, x._1))
    val id2word = wordId.map(x => (x._2, x._1))
    val resultFinal = result.vertices.map(x => {
      if (x._2._2._1 != -1l) {
        (id2word.get(x._1).get, id2tag.get(x._2._2._1).get)
      } else {
        (id2word.get(x._1).get, "空白")
      }
    })

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)

    resultFinal.map(x => x._1 + "@" + x._2).saveAsTextFile(outputPath + "/" + sdf.format(calendar.getTime))
  }
}