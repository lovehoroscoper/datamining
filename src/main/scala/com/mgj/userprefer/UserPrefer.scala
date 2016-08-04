package com.mgj.userprefer

import com.mgj.utils.{LRLearner, HdfsUtil, PartitionUtil}
import org.apache.commons.lang3.Validate
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xiaonuo on 8/3/16.
  */
object UserPrefer {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("calculate user prefer")
      .set("spark.cores.max", "28")

    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val bizdate = args(0)
    val bizdateSubA = args(1)
    val bizdateSubB = args(2)
    val featureTypeList = args(3).split(",")
    val sampleTypeList = args(4).split(",")
    val entity = args(5)
    val entityMapPath = args(6)
    val entitySimPath = args(7)
    val sampleList = args(8).split(",")
    val modelList = args(9).split(",")

    Validate.isTrue(args.size == 10, "input param error, param size must be 9.")
    Validate.isTrue(sampleList.size == sampleTypeList.size, "sample list size and sample type size must be the same.")
    Validate.isTrue(sampleList.size == modelList.size, "sample list size and model list size must be the same.")

    println(s"bizdate:${bizdate}")
    println(s"bizdateSubA:${bizdateSubA}")
    println(s"bizdateSubB:${bizdateSubB}")
    println(s"featureTypeList:${featureTypeList.toList}")
    println(s"sampleTypeList:${sampleTypeList.toList}")
    println(s"entity:${entity}")
    println(s"entityMapPath:${entityMapPath}")
    println(s"entitySimPath:${entitySimPath}")
    println(s"sampleList:${sampleList.toList}")
    println(s"modelList:${modelList.toList}")

    PartitionUtil.checkAppLog(sqlContext, bizdate, "click")
    PartitionUtil.checkAppLog(sqlContext, bizdate, "order")

    if (HdfsUtil.isExists(sc, entityMapPath)) {
      val entityMap = sc.textFile(entityMapPath).map(x => (x.split(" ")(0), x.split(" ")(1))).collect().toMap
      sqlContext.udf.register("to_entity", (itemId: String) =>
        if (entityMap.contains(itemId)) {
          entityMap.get(itemId).get
        } else {
          "-1"
        }
      )
    }
    sqlContext.udf.register("to_vector", (vector: String) => (Vectors.parse(vector)))
    sqlContext.udf.register("to_double", (label: String) => (label.toDouble))
    val toVector = udf { (vector: String) => (Vectors.parse(vector)) }

    val userPreferProcessor = new UserPreferProcessor()
    val learner = new LRLearner()
    val feature = userPreferProcessor.buildFeature(sc, sqlContext, bizdateSubA, bizdateSubB, entity, featureTypeList: _*)

    var i = 0
    for (sampleType <- sampleTypeList) {
      val sample = userPreferProcessor.buildSample(sc, sqlContext, feature, bizdate, entity, sampleType)
      sqlContext.sql(s"drop table if exists ${sampleList.apply(i)}")
      sample.write.saveAsTable(sampleList.apply(i))
      val model = learner.train(sc, sqlContext, sample.select(toVector(sample("feature")).as("feature"), sample("label").as("label")))
      sample.unpersist(blocking = false)
      sc.parallelize(Seq(model), 1).saveAsObjectFile(modelList.apply(i))
      i += 1
    }
    feature.unpersist(blocking = false)
  }
}
