package com.mgj.ml.nlp

import java.nio.file.Paths
import java.util

import com.huaban.analysis.jieba.{WordDictionary, JiebaSegmenter}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.mgj.utils.WordSegUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 12/29/15.
  */
object SegWords {

  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("calculate user gene prefer")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val dictPath = args(0)
    val geneDir = args(1)
    val outputDir = args(2)
    //    val geneDir = "/user/digu/itemGroup/data"

    val itemTitle = sqlContext.sql("select tradeitemid, title from v_dw_trd_tradeitem").map(x => (x(0).toString, x(1).toString))
    val gene = sc.textFile(geneDir).map(x => (x.split(" ")(0), x.split(" ")(1)))
    val geneTitle = itemTitle.join(gene).map(x => (x._1, x._2._2, x._2._1))

    WordSegUtil.loadDict(dictPath)
    val geneSeg = geneTitle.collect.map(x => (x._1, x._2, WordSegUtil.process(x._3).mkString(",")))
    geneSeg.take(10).foreach(println)

    val doc = sc.parallelize(geneSeg)
    //    val doc = sc.textFile("/user/digu/geneWords").map(_.split(" ")).filter(x => x.size ==3).map(x => (x(0),x(1),x(2)))
    val docNum = doc.map(x => x._2).distinct.count
    println("document number: " + docNum)

    val wordMap = doc.groupBy(x => x._2)
      .map(x => x._2.map(x => x._3.split(",")).flatMap(x => x).map(x => (x, 1)).toList.distinct).flatMap(x => x)
      .groupBy(x => x._1).map(x => (x._1, x._2.map(x => x._2).sum)).collect.toMap

    val idf = wordMap.map(x => (x._1, Math.log((1.0 * docNum.toDouble + 1.0) / (1.0 * x._2.toDouble + 1.0))))
    idf.take(10).foreach(println)

    val tfidf = doc.groupBy(x => x._2).map(x => {
      val tf = new util.HashMap[String, Int]()
      var count = 0
      x._2.map(x => x._3.split(",")).foreach(x => x.foreach(x => {
        if (!tf.containsKey(x)) {
          tf.put(x, 0)
        }
        tf.put(x, tf.get(x) + 1)
        count += 1
      }))
      val result = tf.map(x => (x._1, 1.0 * x._2 / count * idf.get(x._1).get)).toList.sortWith((a, b) => a._2 > b._2).filter(x => StringUtils.isNotBlank(x._1.trim)).map(x => x._1 + ":" + x._2)
      (x._1, result.mkString(","))
    })
    tfidf.take(10).foreach(println)

    tfidf.map(x => x._1 + " " + x._2).saveAsTextFile(outputDir)
  }
}
