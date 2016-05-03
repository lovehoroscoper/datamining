package com.mgj.utils

import java.util
import com.mgj.feature.FeatureConstant
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{MinMaxScalerUtil, VectorAssembler}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 3/11/16.
  */
class DiscreteLearner {
  val predictSchema = "probability"

  def train(sc: SparkContext, sqlContext: HiveContext, sampleDF: DataFrame, discreteFeature: Array[String], totalFeature: Array[String], label: String = FeatureConstant.LABEL_KEY, splitRatio: Double = 0.7): (LogisticRegressionModel, util.HashMap[String, List[Double]]) = {
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 16

    val splitMap = new util.HashMap[String, List[Double]]()
    val discreteFeatureSet = discreteFeature.toSet
    val discreteFeatureNames: Array[String] = sampleDF.columns.filter(!_.equals(label)).filter(discreteFeatureSet.contains(_))
    for (featureName <- discreteFeatureNames) {
      val trainData = sampleDF.select(featureName, label).map(x => LabeledPoint(x(1).toString.toDouble, Vectors.dense(x(0).toString.toDouble)))
      val model: DecisionTreeModel = DecisionTree.trainClassifier(trainData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
      val list = TreeUtil.getSplitValue(model)
      splitMap.put(featureName, list)
      println(featureName + ":" + list)
    }

    val sampleDiscreteDF = buildDiscreteDataFrame(sqlContext, sampleDF, discreteFeatureNames, totalFeature, splitMap, true, label)
    println("discrete data frame:")
    sampleDiscreteDF.show()

    val schemaSet = sampleDiscreteDF.columns.toSet
    val featureColumns = sampleDiscreteDF.columns.filter(schemaSet.contains(_)).filter(!_.equals(label))
    println("schema set:" + schemaSet)
    println("feature columns:" + featureColumns.toList)

    val assembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol(FeatureConstant.FEATURE_KEY)
    val dataDF: DataFrame = assembler.transform(sampleDiscreteDF).select(FeatureConstant.FEATURE_KEY, label).cache()
    //    val assembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("temp")
    //    val scaler = new MinMaxScalerUtil().setInputCol("temp").setOutputCol(FeatureConstant.FEATURE_KEY)
    //    val preprocessing = new Pipeline().setStages(Array(assembler, scaler))
    //    val dataDF: DataFrame = preprocessing.fit(sampleDiscreteDF).transform(sampleDiscreteDF).select(FeatureConstant.FEATURE_KEY, label).cache()
    println("dataDF:")
    println(dataDF.first)
    dataDF.show()

    val Array(trainDF, testDF) = dataDF.randomSplit(Array(splitRatio, 1 - splitRatio), System.currentTimeMillis())

    val lr = new LogisticRegression()
      .setMaxIter(30)
      //      .setRegParam(1e-6)
      .setFeaturesCol(FeatureConstant.FEATURE_KEY)
      .setLabelCol(label)
      .setRawPredictionCol(FeatureConstant.RAW_PREDICTION)
      .setPredictionCol(FeatureConstant.PREDICTION)
      .setProbabilityCol(FeatureConstant.PROBABILITY)

    val model = lr.fit(trainDF)

    val featureWeights = featureColumns.zip(model.weights.toArray.toList).toMap
    println(s"featureWeights:${featureWeights}")
    featureWeights.toList.sortWith((a, b) => a._1 > b._1).foreach(println)

    val trainingSummary = model.summary
    val objectiveHistory = trainingSummary.objectiveHistory

    println("train history")
    objectiveHistory.foreach(loss => println(loss))
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    val tRoc = binarySummary.roc
    tRoc.show()

    println("train roc")
    println(binarySummary.areaUnderROC)

    val predictDF = model.transform(testDF).cache()
    println("predict data frame")
    predictDF.show()

    val predictionAndLabels = predictDF.select(FeatureConstant.PROBABILITY, label)
      .map(r => (r.getAs[Vector](0)(1), r.getDouble(1)))

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)

    return (model, splitMap)
  }

  def predict(sqlContext: HiveContext, model: LogisticRegressionModel, splitMap: util.HashMap[String, List[Double]], featureDF: DataFrame, totalFeature: Array[String], schema: String*): RDD[Seq[String]] = {
    val featureColumns = featureDF.columns.filter(!schema.contains(_)).filter(totalFeature.toSet.contains(_))
    val featureDiscreteDF = buildDiscreteDataFrame(sqlContext, featureDF, splitMap.keySet().map(x => x).toArray, featureColumns, splitMap, false, schema: _*)

    val featureDiscreteSchemaSet = featureDiscreteDF.columns.toSet
    val featureDiscreteColumns = featureDiscreteDF.columns.filter(featureDiscreteSchemaSet.contains(_)).filter(!schema.contains(_))
    println("schema set:" + featureDiscreteSchemaSet)
    println("feature columns:" + featureDiscreteColumns.toList)

    val assembler = new VectorAssembler().setInputCols(featureDiscreteColumns).setOutputCol(FeatureConstant.FEATURE_KEY)
    val dataDF: DataFrame = assembler.transform(featureDiscreteDF).select((schema :+ FeatureConstant.FEATURE_KEY).map(x => col(x)): _*).cache()
    //    val assembler = new VectorAssembler().setInputCols(featureDiscreteColumns).setOutputCol("temp")
    //    val scaler = new MinMaxScalerUtil().setInputCol("temp").setOutputCol(FeatureConstant.FEATURE_KEY)
    //    val preprocessing = new Pipeline().setStages(Array(assembler, scaler))
    //    val dataDF: DataFrame = preprocessing.fit(featureDiscreteDF).transform(featureDiscreteDF).select((schema :+ FeatureConstant.FEATURE_KEY).map(x => col(x)): _*).cache()
    dataDF.show()

    return model.transform(dataDF).select((schema :+ predictSchema).map(x => col(x)): _*).map(x => schema.map(t => x.getAs[String](t)) :+ x.getAs[Vector](schema.size)(1).toString)
  }

  private def buildDiscreteDataFrame(sqlContext: HiveContext, sampleDF: DataFrame, discreteFeatureNames: Array[String], totalFeatureNames: Array[String], splitMap: util.HashMap[String, List[Double]], flag: Boolean, schema: String*): DataFrame = {
    val schemaList = new util.ArrayList[String]()
    for (featureName <- discreteFeatureNames) {
      val splitValue = splitMap.get(featureName)
      for (i <- 1 to splitValue.size - 1) {
        schemaList.add(featureName + "_" + i)
      }
    }
    //    val continuesFeatureNames = totalFeatureNames.toSet -- discreteFeatureNames.toSet
    val continuesFeatureNames = totalFeatureNames.toSet
    for (featureName <- continuesFeatureNames) {
      schemaList.add(featureName)
    }

    val schemaAll = schemaList.map(x => StructField(x, DoubleType, true))
    for (x <- schema) {
      if (flag) {
        schemaAll.add(StructField(x, DoubleType, true))
      } else {
        schemaAll.add(StructField(x, StringType, true))
      }
    }

    val sampleDiscreteRow = sampleDF.map(x => {
      val featureList = new util.ArrayList[Double]()
      for (featureName <- discreteFeatureNames) {
        val featureValue = x.getAs[Double](featureName)
        val splitValue = splitMap.get(featureName)
        for (i <- 1 to splitValue.size - 1) {
          if (featureValue < splitValue.get(i) && featureValue >= splitValue.get(i - 1)) {
            featureList.add(1d)
          } else {
            featureList.add(0d)
          }
        }
      }
      //      val continuesFeatureNames = totalFeatureNames.toSet -- discreteFeatureNames.toSet
      val continuesFeatureNames = totalFeatureNames.toSet
      for (featureName <- continuesFeatureNames) {
        val featureValue = x.getAs[Double](featureName)
        featureList.add(featureValue)
      }

      if (flag) {
        val reserveValue = new util.ArrayList[Double]()
        for (name <- schema) {
          val featureValue = x.getAs[Double](name)
          reserveValue.add(featureValue)
        }
        Row((featureList.toList ::: reserveValue.toList): _*)
      } else {
        val reserveValue = new util.ArrayList[String]()
        for (name <- schema) {
          val featureValue = x.getAs[String](name)
          reserveValue.add(featureValue)
        }
        Row((featureList.toList ::: reserveValue.toList): _*)
      }
    })
    val sampleDiscreteDF = sqlContext.createDataFrame(sampleDiscreteRow, StructType(schemaAll.toList))
    return sampleDiscreteDF
  }
}