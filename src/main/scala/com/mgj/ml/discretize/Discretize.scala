package com.mgj.ml.discretize

import java.util

import com.mgj.utils.{DiscreteLearner, TreeUtil}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Split}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 1/11/16.
  */
object Discretize {
  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf().
      setAppName("test").
      set("spark.sql.parquet.binaryAsString", "true")
    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //    val bizdate = args(0)
    //    val bizdateSub = args(1)
    //
    //    val geneMapDir = args(2)
    //    val geneMap = sc.textFile(geneMapDir).map(x => (x.split(" ")(0), x.split(" ")(1))).collect().toMap
    //
    //    sqlContext.udf.register("to_gene", (itemId: String) => {
    //      if (geneMap.contains(itemId)) {
    //        geneMap.get(itemId).get
    //      } else {
    //        "-1"
    //      }
    //    })
    //    //        val geneMapDir = "/user/digu/itemGroup/data"
    //    //        val bizdate = "2016-01-10"
    //    //        val bizdateSub = "2015-01-08"
    //    // User click log: user_id, item_id, visit_time.
    //
    //    val itemInfoSql = "select tradeitemid, price, discount from dw_trd_tradeitem_snapshot where inst_date = '" + bizdate + "'"
    //    val clickLogSql = "select item_id, to_gene(item_id) as gene_id from s_dg_user_base_log where pt >= '" + bizdateSub + "' and pt <= '" + bizdate + "' and action_type = 'click' and platform_type = 'app'"
    //    val orderLogSql = "select item_id, to_gene(item_id) as gene_id from s_dg_user_base_log where pt >= '" + bizdateSub + "' and pt <= '" + bizdate + "' and action_type = 'order'"
    //
    //    val clickLog = sqlContext.sql(clickLogSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString)).filter(x => !x._2.equals("-1"))
    //    val orderLog = sqlContext.sql(orderLogSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString)).filter(x => !x._2.equals("-1"))
    //    val itemInfo = sqlContext.sql(itemInfoSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString)).map(x => (x._1, Math.round(x._2.toDouble * x._3.toDouble)))
    //
    //    val groupMap = sc.textFile(geneMapDir).map(x => (x.split(" ")(1), 1)).reduceByKey((a, b) => a + b).sortBy(x => x._2, false).filter(x => x._2 > 500).collect.toMap
    //
    //    //    orderLog.map(x => (x._1, x)).join(itemInfo.map(x => (x._1, x))).map(x => (x._2._1._2, x._2._2._2)).filter(x => groupMap.contains(x._1)).groupBy(x => x._1).map(x => (x._1, x._2.size)).sortBy(x => x._2, false)
    //    val priceTag = orderLog.map(x => (x._1, x)).join(itemInfo.map(x => (x._1, x))).map(x => (x._2._1._2, x._2._2._2)).filter(x => groupMap.contains(x._1)).groupBy(x => x._1).map(x => {
    //      val N = 3
    //      val sorted = x._2.map(x => x._2).toList.sortWith((a, b) => a > b)
    //      val step = (sorted.size - 1) / N
    //      val list = new util.ArrayList[Long]()
    //      var tag = -1L
    //      for (i <- 1 to N - 1) {
    //        if (tag != sorted(i * step)) {
    //          tag = sorted(i * step)
    //        } else {
    //          tag = sorted(i * step) + 1
    //        }
    //        list.add(tag)
    //      }
    //      (x._1, list.toList)
    //    })
    //
    //    priceTag.take(10).foreach(println)

    //    val feature = sqlContext.read.load("/user/fst/algo/ltr/search_train_parquet/20160311")

    val sample = sqlContext.read.load("/user/fst/algo/ltr/search_sample_parquet/20160324")
    val featureNames = Array(
      "dpv0d_log",
      "dpv1d_log",
      "dpv3d_log",
      "dpv7d_log",
      "dpv15d_log",
      "order0d_log",
      "order1d_log",
      "order3d_log",
      "order7d_log",
      "order15d_log",
      "gmv0d_log",
      "gmv1d_log",
      "gmv3d_log",
      "gmv7d_log",
      "gmv15d_log",
      "search_clk_rate_0d",
      "search_clk_rate_1d",
      "search_clk_rate_3d",
      "search_clk_rate_7d",
      "search_clk_rate_15d",
      "search_order_rate_0d",
      "search_order_rate_1d",
      "search_order_rate_3d",
      "search_order_rate_7d",
      "search_order_rate_15d",
      "search_gmv_rate_0d",
      "search_gmv_rate_1d",
      "search_gmv_rate_3d",
      "search_gmv_rate_7d",
      "search_gmv_rate_15d",
      "search_order_clk_rate_0d",
      "search_order_clk_rate_1d",
      "search_order_clk_rate_3d",
      "search_order_clk_rate_7d",
      "search_order_clk_rate_15d",
      "search_gmv_clk_rate_0d",
      "search_gmv_clk_rate_1d",
      "search_gmv_clk_rate_3d",
      "search_gmv_clk_rate_7d",
      "search_gmv_clk_rate_15d")


    val featureNamesDiscrete = Array(
      "dpv0d_log",
      "dpv1d_log",
      "dpv3d_log",
      "dpv7d_log",
      "dpv15d_log",
      "order0d_log",
      "order1d_log",
      "order3d_log",
      "order7d_log",
      "order15d_log",
      "gmv0d_log",
      "gmv1d_log",
      "gmv3d_log",
      "gmv7d_log",
      "gmv15d_log")

    val learner = new DiscreteLearner()
    val (model, splitMap) = learner.train(sc, sqlContext, sample, featureNames, featureNames)
    sc.parallelize(Seq(model), 1).saveAsObjectFile("discrete/model")
    sc.parallelize(Seq(splitMap), 1).saveAsObjectFile("discrete/splitMap")

    //    val model = sc.objectFile[LogisticRegressionModel]("discrete/model").first()
    //    val splitMap = sc.objectFile[util.HashMap[String, List[Double]]]("discrete/splitMap").first()
    val featureDF = sqlContext.read.load("/user/fst/algo/ltr/search_test_parquet/20160324")
    val result = learner.predict(sqlContext, model, splitMap, featureDF, featureNames, "tradeitemid").map(x => (x.get(0), x.get(1).toDouble))

    result.sortBy(x => x._2, ascending = false).take(50).foreach(println)
    result.sortBy(x => x._2, ascending = false).take(50).map(x => x._1).foreach(println)
  }
}
