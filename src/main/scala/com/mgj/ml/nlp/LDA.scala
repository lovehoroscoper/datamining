package com.mgj.ml.nlp

/**
  * Created by xiaonuo on 3/21/16.
  */

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel}
import org.apache.spark.mllib.linalg.Vectors

object LDA {

  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("test")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    // Load and parse the data
    val data = sc.textFile("/user/digu/geneWordsNew").map(x => (x.split(" ")(0), x.split(" ")(1).split(",").map(x => (x.split(":")(0), x.split(":")(1).toDouble))))
    val wordsSet = data.flatMap(x => x._2).map(x => (x._1, 1)).reduceByKey(_ + _).sortBy(x => x._2, ascending = false).collect
    wordsSet.take(10).foreach(println)
    val wordsMap = new util.HashMap[String, Int]()
    val wordsIndex = new util.HashMap[Int, String]()
    var i = 0
    for (e <- wordsSet) {
      wordsMap.put(e._1, i)
      wordsIndex.put(i, e._1)
      i += 1
    }
    val wordsNum = wordsMap.keySet().size()
    println("word size:" + wordsNum)
    val dataVector = data.map(x => {
      val array = new Array[Double](wordsNum)
      for (e <- x._2) {
        if (wordsMap.containsKey(e._1)) {
          array.update(wordsMap.get(e._1), e._2)
        }
      }
      (x._1.toLong, Vectors.dense(array))
    })

    val topicNumber = 500
    val ldaModel = new LDA().setK(topicNumber).run(dataVector)

    // Save and load model.
    ldaModel.save(sc, "myLDAModel")
    val sameModel = DistributedLDAModel.load(sc, "myLDAModel")
    sameModel.describeTopics(20)
  }
}
