package com.mgj.utils

import com.mgj.feature.FeatureConstant
import org.apache.commons.lang3.Validate
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegressionModel, BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.springframework.stereotype.Service

@Service
class LRLearnerV2 {
  def run(sc: SparkContext, sampleDF: DataFrame, splitRatio: Double = 0.7): LogisticRegressionModel = {
    Validate.notNull(sc, "sc can not be null")
    Validate.notBlank(FeatureConstant.LABEL_KEY, "Constants.LABEL can not be blank")
    Validate.isTrue(sampleDF.columns.contains(FeatureConstant.LABEL_KEY),
      s"sampleDF's columns should contain Constants.LABEL, columns : ${sampleDF.columns}, Constants.LABEL : ${FeatureConstant.LABEL_KEY}")

    Validate.isTrue(splitRatio > 0 && splitRatio < 1,
      s"splitRatio should be in (0, 1), splitRatio: ${splitRatio}")

    println("sampleDF description:")
    sampleDF.describe().show()

    val featureColumns: Array[String] = sampleDF.columns.filter(!_.equals(FeatureConstant.LABEL_KEY))

    //    sampleDF = sampleDF.na.fill(0)

    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("feature_tmp_1")

    val scaler = new MinMaxScaler()
      .setInputCol("feature_tmp_1")
      .setOutputCol(FeatureConstant.FEATURE_KEY)

    val preprocessing = new Pipeline()
      .setStages(Array(assembler, scaler))

    val dataDF: DataFrame = preprocessing.fit(sampleDF).transform(sampleDF)
      .select(FeatureConstant.FEATURE_KEY, FeatureConstant.LABEL_KEY).cache()
    println("dataDF:")
    println(dataDF.first)

    //    val dataRDD = dataDF.map(r => new LabeledPoint(r.getAs[Double](AlgoOfflineConstants.LABEL),
    //      r.getAs[Vector](AlgoOfflineConstants.FEATURES)))
    //    println("dataRDD")
    //    dataRDD.take(2).foreach(println)
    //
    //    val Array(trainRDD, testRDD) = dataRDD.randomSplit(
    //      Array(splitRatio, 1.0 - splitRatio), System.currentTimeMillis())
    //
    //    trainRDD.cache()
    //    testRDD.cache()

    val Array(trainDF, testDF) = dataDF.randomSplit(
      Array(splitRatio, 1 - splitRatio), System.currentTimeMillis())

    val lr = new LogisticRegression()
      .setMaxIter(20)
      .setRegParam(0.01)
      .setFeaturesCol(FeatureConstant.FEATURE_KEY)
      .setLabelCol(FeatureConstant.LABEL_KEY)
      .setRawPredictionCol(FeatureConstant.RAW_PREDICTION)
      .setPredictionCol(FeatureConstant.PREDICTION)
      .setProbabilityCol(FeatureConstant.PROBABILITY)

    val model = lr.fit(trainDF)

    val featureWeights = featureColumns.zip(model.coefficients.toArray.toList).toMap
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
    val tRoc = binarySummary.roc
    tRoc.show()
    println("train roc")
    println(binarySummary.areaUnderROC)

    val predictDF = model.transform(testDF).cache()
    println("predict dataframe")
    predictDF.show()

    val predictionAndLabels = predictDF.select(FeatureConstant.PROBABILITY, FeatureConstant.LABEL_KEY)
      .map(r => (r.getAs[Vector](0)(1), r.getDouble(1)))

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    // ROC Curve
    val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)
    return model
  }
}
