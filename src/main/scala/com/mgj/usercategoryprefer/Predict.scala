package com.mgj.usercategoryprefer

import java.text.SimpleDateFormat
import java.util.{Calendar, HashMap}

import com.mgj.feature.FeatureType
import com.mgj.utils.{HiveUtil, LRLearner}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaonuo on 11/28/15.
  */
object Predict {
  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("calculate user prefer")
      .set("spark.cores.max", "28")
    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val bizdate = args(0)
    val bizdateSub30 = args(1)
    val userCategoryPerferPath = args(2)
    val userCategoryPerferOrderPath = args(3)

    val model = sc.objectFile[LogisticRegressionModel]("/user/digu/userCategoryPreferModel").first()
    println("model")
    println(model)
    println(model.coefficients)

    val modelOrder = sc.objectFile[LogisticRegressionModel]("/user/digu/userCategoryPreferOrderModel").first()
    println("model order")
    println(modelOrder)
    println(modelOrder.coefficients)

    sqlContext.udf.register("to_vector", (vector: String) => (Vectors.parse(vector)))
    sqlContext.udf.register("to_double", (label: String) => (label.toDouble))

    val clickSql = "select user_id, category_id, time from s_dg_user_base_log where pt >= '" + bizdateSub30 + "' and pt <= '" + bizdate + "' and action_type = 'click' and platform_type = 'app'"
    val orderSql = "select user_id, category_id, time from s_dg_user_base_log where pt >= '" + bizdateSub30 + "' and pt <= '" + bizdate + "' and action_type = 'order'"
    val addCartSql = "select user_id, category_id, time from s_dg_user_base_log where pt >= '" + bizdateSub30 + "' and pt <= '" + bizdate + "' and action_type = 'add_cart'"
    val favorSql = "select user_id, category_id, time from s_dg_user_base_log where pt >= '" + bizdateSub30 + "' and pt <= '" + bizdate + "' and action_type = 'favor'"

    println("get click log sql:")
    println("{" + clickSql + "}")
    println("get order log sql:")
    println("{" + orderSql + "}")
    println("get add cart log sql:")
    println("{" + addCartSql + "}")
    println("get favor log sql:")
    println("{" + favorSql + "}")

    val userBaseLogClick = sqlContext.sql(clickSql)
    val userBaseLogOrder = sqlContext.sql(orderSql)
    val userBaseLogAddCart = sqlContext.sql(addCartSql)
    val userBaseLogFavor = sqlContext.sql(favorSql)

    def getFeature(iterable: Iterable[(String, String, String)]): Array[Double] = {
      val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val dateCurrent = new SimpleDateFormat("yyyy-MM-dd").parse(bizdate)
      val feature: HashMap[Integer, Double] = new HashMap[Integer, Double]()
      for (log <- iterable) {
        val date = df.parse(log._3)
        val diff: Double = Math.ceil(1.0 * (dateCurrent.getTime - date.getTime) / (60 * 60 * 1000 * 24))
        if (!feature.containsKey(diff.toInt)) {
          feature.put(diff.toInt, 0d)
        }
        feature.put(diff.toInt, feature.get(diff.toInt) + 1d)
      }
      val featureArray: Array[Double] = new Array[Double](30)
      for (i <- 0 to 29) {
        if (feature.containsKey(i)) {
          featureArray(i) = feature.get(i)
        } else {
          featureArray(i) = 0
        }
      }
      return featureArray
    }

    def buildFeature(userBaseLogClick: DataFrame, userBaseLogOrder: DataFrame): RDD[(String, String, Vector)] = {
      val clickRawFeature = userBaseLogClick.rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString)).groupBy(x => x._1 + "_" + x._2).map(x => (x._1, getFeature(x._2)))

      val orderRawFeature = userBaseLogOrder.rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString)).groupBy(x => x._1 + "_" + x._2).map(x => (x._1, getFeature(x._2)))

      val addCartRawFeature = userBaseLogAddCart.rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString)).groupBy(x => x._1 + "_" + x._2).map(x => (x._1, getFeature(x._2)))

      val favorRawFeature = userBaseLogFavor.rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString)).groupBy(x => x._1 + "_" + x._2).map(x => (x._1, getFeature(x._2)))

      def joinFeature(arrayLeft: Option[Array[Double]], arrayRight: Option[Array[Double]], offSet: Int, N: Int): Array[Double] = {
        val featureArray: Array[Double] = new Array[Double](N)
        if (arrayLeft != None) {
          val array = arrayLeft.get
          val size = array.size - 1
          for (x <- 0 to size) {
            featureArray.update(x, array.apply(x))
          }
        }

        if (arrayRight != None) {
          val array = arrayRight.get
          val size = array.size - 1
          for (x <- 0 to size) {
            featureArray.update(x + offSet, array.apply(x))
          }
        }

        return featureArray
      }

      val allFeaturePartA = clickRawFeature.fullOuterJoin(orderRawFeature).map(x => (x._1, joinFeature(x._2._1, x._2._2, 30, 60)))
      val allFeaturePartB = addCartRawFeature.fullOuterJoin(favorRawFeature).map(x => (x._1, joinFeature(x._2._1, x._2._2, 30, 60)))
      val allFeature = allFeaturePartA.fullOuterJoin(allFeaturePartB).map(x => (x._1.split("_")(0), x._1.split("_")(1), Vectors.dense(joinFeature(x._2._1, x._2._2, 60, 120))))
      return allFeature
    }

    val allFeature = buildFeature(userBaseLogClick, userBaseLogOrder).map(x => Row(x._1.toString, x._2.toString, x._3.toString))

    val schema =
      StructType(
        StructField("user_id", StringType, true)
          :: StructField("category_id", StringType, true)
          :: StructField("feature", StringType, true)
          :: Nil
      )

    val predictDF: DataFrame = sqlContext.createDataFrame(allFeature, schema)
    predictDF.registerTempTable("s_dg_user_category_prefer_feature_spark")

    val featureDF = sqlContext.sql("select to_vector(feature) as feature, user_id, category_id from s_dg_user_category_prefer_feature_spark")
    val learner = new LRLearner()
    val result = learner.predict(model, featureDF, "user_id", "category_id")
    val resultOrder = learner.predict(modelOrder, featureDF, "user_id", "category_id")

    def sort(x: Iterable[(String, String, Double)], N: Int): String = {
      val list = x.toList.sortWith((a, b) => a._3.compareTo(b._3) > 0).take(N).map(x => x._2 + ":" + Math.round(x._3 * 100000)).mkString(",")
      return list
    }

    result.map(x => (x(0), x(1), x(2).toDouble)).groupBy(_._1).filter(x => x._2.size > 0 && x._1.toLong > 0).map(x => x._1 + " " + sort(x._2, 50)).saveAsTextFile(userCategoryPerferPath)
    resultOrder.map(x => (x(0), x(1), x(2).toDouble)).groupBy(_._1).filter(x => x._2.size > 0 && x._1.toLong > 0).map(x => x._1 + " " + sort(x._2, 50)).saveAsTextFile(userCategoryPerferOrderPath)

    val sdf = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    HiveUtil.featureHdfsToHive(sc, sqlContext, "user_category_prefer", userCategoryPerferPath, sdf.format(calendar.getTime), "s_dg_category_prefer", FeatureType.USER)
    HiveUtil.featureHdfsToHive(sc, sqlContext, "user_category_prefer_order", userCategoryPerferOrderPath, sdf.format(calendar.getTime), "s_dg_category_prefer_order", FeatureType.USER)
  }
}
