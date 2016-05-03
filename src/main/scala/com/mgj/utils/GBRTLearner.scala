package com.mgj.utils

import com.mgj.feature.FeatureConstant
import org.apache.commons.lang3.Validate
import org.apache.spark.SparkContext
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.springframework.stereotype.Service
import org.apache.spark.sql.functions._

import scala.reflect.internal.util.TableDef.Column

@Service
class GBRTLearner {
  val predictSchema = "prediction"

  def train(sc: SparkContext, sqlContext: HiveContext, sampleDF: DataFrame, splitRatio: Double = 0.7): GBTRegressionModel = {
    Validate.notNull(sc, "sc can not be null")
    Validate.notBlank(FeatureConstant.LABEL_KEY, "Constants.LABEL can not be blank")
    Validate.isTrue(sampleDF.columns.contains(FeatureConstant.LABEL_KEY),
      s"sampleDF's columns should contain Constants.LABEL, columns : ${sampleDF.columns}, Constants.LABEL : ${FeatureConstant.LABEL_KEY}")

    Validate.isTrue(splitRatio > 0 && splitRatio < 1,
      s"splitRatio should be in (0, 1), splitRatio: ${splitRatio}")

    println("sampleDF description:")
    sampleDF.describe().show()

    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testingData) = sampleDF.randomSplit(Array(splitRatio, 1 - splitRatio), System.currentTimeMillis())

    // Train a GBT model.
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("feature")
      .setMaxIter(50)
      .setMaxDepth(5)

    // Train model.  This also runs the indexer.
    val model = gbt.fit(trainingData)

    evaluate(trainingData, model)
    evaluate(testingData, model)

    return model
  }

  def evaluate(dateSetDF: DataFrame, model: GBTRegressionModel): Unit = {
    // Make predictions.
    val predictions = model.transform(dateSetDF)

    // Select example rows to display.
    predictions.select(predictSchema, "label", "feature").show(5)

    // Select (prediction, true label) and compute test error
    val predictionAndLabels = predictions.select(predictSchema, "label")
      .map(r => (r.getDouble(0), r.getDouble(1)))

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    val roc = metrics.roc
    println(roc)

    val auc = metrics.areaUnderROC
    println("auc")
    println(auc)
  }

  def predict(model: GBTRegressionModel, featureDF: DataFrame, schema: String*): RDD[Seq[String]] = {
    return model.transform(featureDF).select((schema :+ predictSchema).map(x => col(x)): _*).map(x => schema.map(t => x.getAs[String](t)) :+ x.getDouble(schema.size).toString)
  }
}
