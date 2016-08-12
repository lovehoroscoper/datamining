package com.mgj.userprefer

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.mgj.feature.FeatureType
import com.mgj.utils.{HdfsUtil, HiveUtil, LRLearner, PartitionUtil}
import org.apache.commons.lang3.Validate
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaonuo on 8/3/16.
  */
object Train {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("calculate user prefer")
      .set("spark.cores.max", "28")

    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlContext.setConf("fs.defaultFS", "hdfs://mgjcluster")

    VariableFactory.init(args)

    val toVector = udf { (vector: String) => (Vectors.parse(vector)) }

    var i = 0
    for (sampleType <- VariableFactory.sampleTypeList) {
      val sample = sqlContext.sql(s"select * from ${VariableFactory.sampleList.apply(i)}")

      val ratioCount = sample.groupBy("label").count().rdd.map(x => (x(0).toString, x(1).toString.toDouble)).collect().toMap
      val ratio = ratioCount.get("1.0").get / (ratioCount.get("1.0").get + ratioCount.get("0.0").get)

      println(s"sample type:${sampleType}")
      println(s"total sample count:${(ratioCount.get("1.0").get + ratioCount.get("0.0").get)}")
      println(s"total positive sample count:${ratioCount.get("1.0").get}")
      println(s"positive negtive sample ratio:${ratio}")

      val posSample = sample.where(sample("label") > 0.5)
      val negSample = sample.where(sample("label") < 0.5).sample(false, ratio)
      sample.unpersist(blocking = false)

      val sampleDF = posSample.unionAll(negSample)
      posSample.unpersist(blocking = false)
      negSample.unpersist(blocking = false)

      val model = VariableFactory.learner.train(sc, sqlContext, sampleDF.select(toVector(sample("feature")).as("feature"), sample("label").as("label")))
      sample.unpersist(blocking = false)
      sc.parallelize(Seq(model), 1).saveAsObjectFile(VariableFactory.modelList.apply(i))
      i += 1
    }
  }
}
