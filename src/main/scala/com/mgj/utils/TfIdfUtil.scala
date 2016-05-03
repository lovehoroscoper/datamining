package com.mgj.utils

import org.apache.spark.rdd.RDD

/**
  * Created by xiaonuo on 4/6/16.
  */
object TfIdfUtil {

  // input (docId, doc)

  def getIdf(doc: RDD[(String, List[String])]): RDD[(String, Double)] = {
    val docCnt = doc.count()
    println(s"document count:${docCnt}")
    val idf = doc.map(x => x._2.map(t => (x._1, t))).flatMap(x => x).distinct().map(x => (x._2, 1)).reduceByKey(_ + _).map(x => {
      (x._1, Math.log((1.0 * docCnt.toDouble + 1.0) / (1.0 * x._2.toDouble + 1.0)))
    })
    return idf
  }

  def getTf(doc: RDD[(String, List[String])]): RDD[(String, String, Double)] = {
    val tf = doc.map(x => {
      val docId = x._1
      val wordNum = x._2.size
      val tf = x._2.groupBy(x => x).map(x => (docId, x._1, 1.0 * x._2.size / wordNum))
      tf
    }).flatMap(x => x)
    return tf
  }

  def getTfIdf(doc: RDD[(String, List[String])]): (RDD[(String, String, Double)], RDD[(String, String, Double)], RDD[(String, Double)]) = {
    val tf = getTf(doc)
    val idf = getIdf(doc)
    val tfidf = tf.map(x => (x._2, x)).join(idf).map(x => (x._2._1._1, x._2._1._2, x._2._1._3 * x._2._2))
    return (tfidf, tf, idf)
  }

  def getTfIdfLocal(doc: RDD[(String, List[String])]): (RDD[(String, String, Double)], RDD[(String, String, Double)], Map[String, Double]) = {
    val tf = getTf(doc)
    val idf = getIdf(doc).collect().toMap
    val tfidf = tf.map(x => (x._1, x._2, 1.0 * x._3 * idf.get(x._2).get))
    return (tfidf, tf, idf)
  }
}
