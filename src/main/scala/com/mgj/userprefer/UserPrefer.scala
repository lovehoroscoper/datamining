package com.mgj.userprefer

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.mgj.feature.FeatureType
import com.mgj.utils.{HiveUtil, LRLearner, HdfsUtil, PartitionUtil}
import org.apache.commons.lang3.Validate
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
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

    Validate.isTrue(args.size == 18, "input param error, param size must be 9.")

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

    //    val featurePredict = userPreferProcessor.buildFeature(sc, sqlContext, predictBizdateSub, predictBizdate, entity, featureTypeList: _*)

    //    i = 0
    //    for (path <- predictResultList) {
    //      val model = sc.objectFile[LogisticRegressionModel](modelList.apply(i)).first()
    //      println(model)
    //      println(model.coefficients)
    //
    //      val schema =
    //        StructType(
    //          StructField("user_id", StringType, true)
    //            :: StructField("entity_id", StringType, true)
    //            :: StructField("feature", StringType, true)
    //            :: Nil
    //        )
    //
    //      val predictDF: DataFrame = sqlContext.createDataFrame(featurePredict, schema)
    //      predictDF.registerTempTable("s_dg_user_gene_prefer_feature_spark")
    //      val featureDF = sqlContext.sql("select to_vector(feature) as feature, user_id, gene_id from s_dg_user_gene_prefer_feature_spark")
    //      val learner = new LRLearner()
    //      val result = learner.predict(model, predictDF.select(toVector(predictDF("feature")).as("feature"), predictDF("user_id").as("user_id"), predictDF("gene_id").as("gene_id"))
    //        , "user_id", "gene_id")
    //
    //      def sort(x: Iterable[(String, String, Double)], N: Int): String = {
    //        val list = x.toList.sortWith((a, b) => a._3.compareTo(b._3) > 0).take(N).map(x => x._2 + ":" + Math.round(x._3 * 100000)).mkString(",")
    //        return list
    //      }
    //
    //      result.map(x => (x(0), x(1), x(2).toDouble)).groupBy(_._1).filter(x => x._2.size > 0 && x._1.toLong > 0).map(x => x._1 + " " + sort(x._2, 50)).saveAsTextFile(userGenePreferPath)
    //      resultOrder.map(x => (x(0), x(1), x(2).toDouble)).groupBy(_._1).filter(x => x._2.size > 0 && x._1.toLong > 0).map(x => x._1 + " " + sort(x._2, 50)).saveAsTextFile(userGenePreferOrderPath)
    //
    //      val writer = new PrintWriter(new File(isSuccessFile))
    //      writer.write("DONE!")
    //      writer.close()
    //
    //      val sdf = new SimpleDateFormat("yyyyMMdd")
    //      val calendar = Calendar.getInstance()
    //      HiveUtil.featureHdfsToHive(sc, sqlContext, "user_gene_prefer", userGenePreferPath, sdf.format(calendar.getTime), "s_dg_user_gene_prefer", FeatureType.USER)
    //      HiveUtil.featureHdfsToHive(sc, sqlContext, "user_gene_prefer_order", userGenePreferOrderPath, sdf.format(calendar.getTime), "s_dg_user_gene_prefer_order", FeatureType.USER)
    //      HiveUtil.featureHdfsToHive(sc, sqlContext, "gene_id", geneMapDir, sdf.format(calendar.getTime), "s_dg_gene_id", FeatureType.ITEM)
    //  }
  }

}
