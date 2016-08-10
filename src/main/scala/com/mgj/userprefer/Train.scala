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

    Validate.isTrue(args.size == 18, "input param error, param size must be 18.")

    val bizdate = args(0)
    val bizdateSubA = args(1)
    val bizdateSubB = args(2)
    val featureTypeList = args(3).split(",")
    val sampleTypeList = args(4).split(",")
    val entity = args(5)
    val entityFeatureName = args(6)
    val entityTableName = args(7)
    val entityMapPath = args(8)
    val entitySimPath = args(9)
    val sampleList = args(10).split(",")
    val modelList = args(11).split(",")
    val predictBizdate = args(12)
    val predictBizdateSub = args(13)
    val predictResultList = args(14).split(",")
    val predictTableList = args(15).split(",")
    val featureNameList = args(16).split(",")
    val successTag = args(17)

    Validate.isTrue(sampleList.size == sampleTypeList.size, "sample list size and sample type size must be the same.")
    Validate.isTrue(sampleList.size == modelList.size, "sample list size and model list size must be the same.")
    Validate.isTrue(predictResultList.size == predictTableList.size, "predict result list size and predict table list size must be the same.")
    Validate.isTrue(predictResultList.size == featureNameList.size, "predict result list size and feature name list size must be the same.")

    println(s"bizdate:${bizdate}")
    println(s"bizdateSubA:${bizdateSubA}")
    println(s"bizdateSubB:${bizdateSubB}")
    println(s"featureTypeList:${featureTypeList.toList}")
    println(s"sampleTypeList:${sampleTypeList.toList}")
    println(s"entity:${entity}")
    println(s"entityFeatureName:${entityFeatureName}")
    println(s"entityTableName:${entityTableName}")
    println(s"entityMapPath:${entityMapPath}")
    println(s"entitySimPath:${entitySimPath}")
    println(s"sampleList:${sampleList.toList}")
    println(s"modelList:${modelList.toList}")
    println(s"predictBizdate:${predictBizdate}")
    println(s"predictBizdateSub:${predictBizdateSub}")
    println(s"predictResultList:${predictResultList.toList}")
    println(s"predictTableList:${predictTableList.toList}")
    println(s"featureNameList:${featureNameList.toList}")
    println(s"successTag:${successTag}")

    PartitionUtil.checkAppLog(sqlContext, bizdate, "click")
    PartitionUtil.checkAppLog(sqlContext, bizdate, "order")

    val sdf = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    if (HdfsUtil.isExists(sc, entityMapPath)) {
      val entityMap = sc.textFile(entityMapPath).map(x => (x.split(" ")(0), x.split(" ")(1))).collect().toMap
      sqlContext.udf.register("to_entity", (itemId: String) =>
        if (entityMap.contains(itemId)) {
          entityMap.get(itemId).get
        } else {
          "-1"
        }
      )
      Validate.notBlank(entityFeatureName)
      Validate.notBlank(entityTableName)
      HiveUtil.featureHdfsToHive(sc, sqlContext, entityFeatureName, entityMapPath, sdf.format(calendar.getTime), entityTableName, FeatureType.ITEM)
    }

    val toVector = udf { (vector: String) => (Vectors.parse(vector)) }

    val userPreferProcessor = new UserPreferProcessor()
    val learner = new LRLearner()
    val feature = userPreferProcessor.buildFeature(sc, sqlContext, bizdateSubA, bizdateSubB, entity, featureTypeList: _*)

    var i = 0
    for (sampleType <- sampleTypeList) {
      val sample = userPreferProcessor.buildSampleV2(sc, sqlContext, feature, bizdate, entity, sampleType)
      sqlContext.sql(s"drop table if exists ${sampleList.apply(i)}")
      sample.write.saveAsTable(sampleList.apply(i))
      i += 1
    }
    feature.unpersist(blocking = false)

    //    val featurePredict = userPreferProcessor.buildFeature(sc, sqlContext, predictBizdateSub, predictBizdate, entity, featureTypeList: _*)
    //      .map(x => Row(x._1.toString, x._2.toString, x._3.toString))
    //
    //    i = 0
    //    for (path <- predictResultList) {
    //      val model = sc.objectFile[LogisticRegressionModel](modelList.apply(i)).first()
    //      println(model)
    //      println(model.coefficients)
    //
    //      val schema =
    //        StructType(
    //          StructField("user_id", StringType, true)
    //            :: StructField(entityFeatureName, StringType, true)
    //            :: StructField("feature", StringType, true)
    //            :: Nil
    //        )
    //
    //      val getReason = udf { (vector: String) => (userPreferProcessor.getReason(vector, model, featureTypeList.size)) }
    //      val featureDF: DataFrame = sqlContext.createDataFrame(featurePredict, schema)
    //      val result = learner.predict(model, featureDF.select(toVector(featureDF("feature")).as("feature"), featureDF("user_id").as("user_id"), featureDF(entityFeatureName).as(entityFeatureName), getReason(featureDF("feature")).as("reason_id")), "user_id", entityFeatureName, "reason_id")
    //
    //      def sort(x: Iterable[(String, String, String, Double)], N: Int): String = {
    //        val list = x.toList.sortWith((a, b) => a._4.compareTo(b._4) > 0).take(N).map(x => x._2 + ":" + Math.round(x._4 * 100000) + ":" + x._3).mkString(",")
    //        return list
    //      }
    //
    //      result.map(x => (x(0), x(1), x(2), x(3).toDouble)).groupBy(_._1).filter(x => x._2.size > 0 && x._1.toLong > 0).map(x => x._1 + " " + sort(x._2, 50)).saveAsTextFile(predictResultList.apply(i))
    //      HiveUtil.featureHdfsToHive(sc, sqlContext, featureNameList.apply(i), predictResultList.apply(i), sdf.format(calendar.getTime), predictTableList.apply(i), FeatureType.USER)
    //    }

    val writer = new PrintWriter(new File(successTag))
    writer.write("DONE!")
    writer.close()
  }
}
