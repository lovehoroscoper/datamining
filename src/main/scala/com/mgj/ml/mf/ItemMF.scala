package com.mgj.ml.mf

import com.mgj.utils.MFLearner
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xiaonuo on 3/8/16.
  */
object ItemMF {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("mf test")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("es.read.metadata", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new HiveContext(sc)

    val inputPath = args(0)
    val outputPath = args(1)

    val mfLearner = new MFLearner
    val sample = sc.textFile(inputPath).map(x => {
      val userId = x.split(" ")(0)
      x.split(" ")(1).split(",").toList.map(x => (userId, x.split(":")(0), x.split(":")(1)))
    }).flatMap(x => x)

    val model = mfLearner.train(sc, sqlContext, sample, 50)
    model.save(sc, outputPath + "/model")
    model.userFeatures.map(x => x._1 + " " + x._2.map(x => Math.round(x * 100)).mkString(",")).saveAsTextFile(outputPath + "/user")
    model.productFeatures.map(x => x._1 + " " + x._2.map(x => Math.round(x * 100)).mkString(",")).saveAsTextFile(outputPath + "/product")

    mfLearner.predict(model, sample.map(x => (x._1, x._2))).take(10).foreach(println)
  }
}
