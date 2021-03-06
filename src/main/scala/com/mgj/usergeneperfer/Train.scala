package com.mgj.usergeneperfer

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
      .setAppName("calculate user gene prefer")
      .set("spark.cores.max", "28")
    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //    val userGenePreferModel = args(0)
    //    val userGenePreferOrderModel = args(1)
    val userGenePreferModel = args(0)
    val userGenePreferFeatureTable = args(1)

    init()
    //    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    sqlContext.udf.register("to_vector", (vector: String) => (Vectors.parse(vector)))
    sqlContext.udf.register("to_double", (label: String) => (label.toDouble))
    //    val sampleDF: DataFrame = sqlContext.sql("select to_vector(feature) as feature, to_double(label) as label from s_dg_user_gene_prefer_sample")
    val sampleDF: DataFrame = sqlContext.sql("select to_vector(feature) as feature, to_double(label) as label from " + userGenePreferFeatureTable)
    val model = learner.train(sc, sqlContext, sampleDF)

    sc.parallelize(Seq(model), 1).saveAsObjectFile(userGenePreferModel)

    //    val sampleOrderDF: DataFrame = sqlContext.sql("select to_vector(feature) as feature, to_double(label) as label from s_dg_user_gene_prefer_order_sample")
    //    val modelOrder = learner.train(sc, sqlContext, sampleOrderDF)
    //
    //    sc.parallelize(Seq(modelOrder), 1).saveAsObjectFile(userGenePreferOrderModel)
  }
}
