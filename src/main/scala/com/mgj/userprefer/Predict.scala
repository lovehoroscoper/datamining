package com.mgj.userprefer

import java.io.{File, PrintWriter}

import com.mgj.feature.FeatureType
import com.mgj.utils.HiveUtil
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegressionModel

/**
  * Created by xiaonuo on 8/3/16.
  */
object Predict {
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

    val featurePredict = VariableFactory.userPreferProcessor.buildFeature(sc, sqlContext, VariableFactory.predictBizdateSub, VariableFactory.predictBizdate, VariableFactory.entity, VariableFactory.featureTypeList: _*)
      .map(x => Row(x._1.toString, x._2.toString, x._3.toString))

    var i = 0
    for (path <- VariableFactory.predictResultList) {
      val model = sc.objectFile[LogisticRegressionModel](VariableFactory.modelList.apply(i)).first()
      println(model)
      println(model.coefficients)

      val schema =
        StructType(
          StructField("user_id", StringType, true)
            :: StructField(VariableFactory.entityFeatureName, StringType, true)
            :: StructField("feature", StringType, true)
            :: Nil
        )

      val getReason = udf { (vector: String) => (VariableFactory.userPreferProcessor.getReason(vector, model, VariableFactory.featureTypeList.size)) }
      val featureDF: DataFrame = sqlContext.createDataFrame(featurePredict, schema)
      val result = VariableFactory.learner.predict(model, featureDF.select(toVector(featureDF("feature")).as("feature"), featureDF("user_id").as("user_id"), featureDF(VariableFactory.entityFeatureName).as(VariableFactory.entityFeatureName), getReason(featureDF("feature")).as("reason_id")), "user_id", VariableFactory.entityFeatureName, "reason_id")

      def sort(x: Iterable[(String, String, String, Double)], N: Int): String = {
        val list = x.toList.sortWith((a, b) => a._4.compareTo(b._4) > 0).take(N).map(x => x._2 + ":" + Math.round(x._4 * 100000) + ":" + x._3).mkString(",")
        return list
      }

      result.map(x => (x(0), x(1), x(2), x(3).toDouble)).groupBy(_._1).filter(x => x._2.size > 0 && x._1.toLong > 0).map(x => x._1 + " " + sort(x._2, 50)).saveAsTextFile(VariableFactory.predictResultList.apply(i))
      HiveUtil.featureHdfsToHive(sc, sqlContext, VariableFactory.featureNameList.apply(i), VariableFactory.predictResultList.apply(i), VariableFactory.sdf.format(VariableFactory.calendar.getTime), VariableFactory.predictTableList.apply(i), FeatureType.USER)
      i += 1
    }

    val writer = new PrintWriter(new File(VariableFactory.successTag))
    writer.write("DONE!")
    writer.close()
  }
}
