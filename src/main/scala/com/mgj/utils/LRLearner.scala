package com.mgj.utils

import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{LogisticRegressionModel, BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, DataFrame}
import org.springframework.stereotype.Service

/**
  * Created by xiaonuo on 11/27/15.
  */
@Service
class LRLearner {
  val predictSchema = "probability"

  def train(sc: SparkContext, sqlContext: HiveContext, sampleDF: DataFrame, splitRatio: Double = 0.7): LogisticRegressionModel = {

    val dataDF: DataFrame = sampleDF.select("feature", "label").toDF()
    println("dataDF:")
    println(dataDF.first)

    val Array(trainDF, testDF) = dataDF.randomSplit(Array(splitRatio, 1 - splitRatio), System.currentTimeMillis())

    val lr = new LogisticRegression()
      .setMaxIter(50)
      .setRegParam(0.01)
      .setFeaturesCol("feature")
      .setLabelCol("label")
      .setRawPredictionCol("raw_predict")
      .setPredictionCol("predict")
      .setProbabilityCol(predictSchema)

    val model = lr.fit(trainDF)

    val featureWeights = model.coefficients.toArray.toList
    println(s"featureWeights:${featureWeights}")

    val trainingSummary = model.summary
    val objectiveHistory = trainingSummary.objectiveHistory
    println("train history")
    objectiveHistory.foreach(loss => println(loss))

    // Obtain the metrics useful to judge performance on test data.
    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
    // binary classification problem.
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    val trainROC = binarySummary.roc
    trainROC.show()
    println("train auc")
    println(binarySummary.areaUnderROC)

    val predictDF = model.transform(testDF).cache()
    println("predict dataframe")
    predictDF.show()

    val predictionAndLabels = predictDF.select(predictSchema, "label")
      .map(r => (r.getAs[Vector](0)(1), r.getDouble(1)))

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    val testROC = metrics.roc
    println(testROC)

    val testAUC = metrics.areaUnderROC
    println("test auc")
    println(testAUC)

    return model
  }

  def predict(model: LogisticRegressionModel, featureDF: DataFrame, schema: String*): RDD[Seq[String]] = {
    return model.transform(featureDF).select((schema :+ predictSchema).map(x => col(x)): _*).map(x => schema.map(t => x.getAs[String](t)) :+ x.getAs[Vector](schema.size)(1).toString)
  }
}
