package com.mgj.usercategoryprefer

import com.mgj.utils.LRLearner
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaonuo on 11/28/15.
  */
object Train {
  var learner: LRLearner = null

  def init() = {
    learner = new LRLearner()
  }

  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("calculate user prefer")
      .set("spark.cores.max", "28")
    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    val userCategoryPreferModel = args(0)
    val userCategoryPreferFeatureTable = args(1)

    init()
    sqlContext.udf.register("to_vector", (vector: String) => (Vectors.parse(vector)))
    sqlContext.udf.register("to_double", (label: String) => (label.toDouble))
    //    val sampleDF: DataFrame = sqlContext.sql("select to_vector(feature) as feature, to_double(label) as label from s_dg_user_Category_prefer_sample")
    val sampleDF: DataFrame = sqlContext.sql("select to_vector(feature) as feature, to_double(label) as label from " + userCategoryPreferFeatureTable)
    val model = learner.train(sc, sqlContext, sampleDF)

    sc.parallelize(Seq(model), 1).saveAsObjectFile(userCategoryPreferModel)

    //    val sampleOrderDF: DataFrame = sqlContext.sql("select to_vector(feature) as feature, to_double(label) as label from s_dg_user_Category_prefer_order_sample")
    //    val modelOrder = learner.train(sc, sqlContext, sampleOrderDF)
    //
    //    sc.parallelize(Seq(modelOrder), 1).saveAsObjectFile("hdfs://mgjcluster/user/digu/userCategoryPreferOrderModel")
  }
}
