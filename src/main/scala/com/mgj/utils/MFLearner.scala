package com.mgj.utils

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by xiaonuo on 3/7/16.
  */
class MFLearner {
  val predictSchema = "probability"

  def train(sc: SparkContext, sqlContext: HiveContext, sampleRDD: RDD[(String, String, String)], rank: Int = 20, splitRatio: Double = 0.7): MatrixFactorizationModel = {
    // Load and parse the data
    val ratings = sampleRDD.map(x => Rating(x._1.toInt, x._2.toInt, x._3.toDouble))

    // Build the recommendation model using ALS
    val numIterations = 20
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    println("user count: " + model.userFeatures.count)
    println("product count: " + model.productFeatures.count)
    return model
  }

  def predict(model: MatrixFactorizationModel, featureRDD: RDD[(String, String)]): RDD[Seq[String]] = {
    val usersProducts = featureRDD.map(x => (x._1.toInt, x._2.toInt))
    return model.predict(usersProducts).map(x => Seq(x.user.toString, x.product.toString, x.rating.toString))
  }
}
