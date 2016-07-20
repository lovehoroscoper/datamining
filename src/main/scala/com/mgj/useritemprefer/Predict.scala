package com.mgj.useritemprefer

import java.text.SimpleDateFormat
import java.util.{Calendar, ArrayList, Arrays, HashSet}

import com.mgj.feature.FeatureType
import com.mgj.utils.{HiveUtil, LRLearner, GBRTLearner}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 1/22/16.
  */
object Predict {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("calculate user item prefer").
      set("spark.sql.parquet.binaryAsString", "true")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    val bizdate = args(0)
    val bizdateSub = args(1)
    val itemSimPath = args(2)
    val userItemPreferModel = args(3)
    val userItemPreferPath = args(4)
    val itemCtrPath = args(5)

    val model = sc.objectFile[LogisticRegressionModel](userItemPreferModel).first()
    println("model")
    println(model)
    println(model.coefficients)

    val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val itemViewSql = "select user_id, item_id, category_id, time, pt from s_dg_user_base_log where pt >= '" + bizdateSub + "' and pt <= '" + bizdate + "' and action_type='click' and platform_type='app'"

    val userItemView = sqlContext.sql(itemViewSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString))

    val N = 20
    val simItemNum = 5
    val userItemPrefer = userItemView.groupBy(x => x._1).map(x => {

      def getDateVar(timeList: List[String]): Double = {
        if (timeList.size == 1) {
          return 0d
        }
        val sdfDay = new SimpleDateFormat("yyyy-MM-dd")
        val list = timeList.map(x => sdfDay.parse(x).getTime / 1000)
        val avg = 1.0 * list.sum / list.size
        val variance = 1.0 * list.map(x => (x - avg) * (x - avg)).sum / (list.size - 1)
        return Math.sqrt(variance) / 24 / 60 / 60
      }

      def getDateDiff(dateA: String, dateB: String, pattern: String): Double = {
        val sdfDay = new SimpleDateFormat(pattern)
        return 1.0 * Math.abs(sdfDay.parse(dateA).getTime - sdfDay.parse(dateB).getTime) / 1000 / 60 / 60 / 24
      }

      // item_id,category_id,time,pt
      val userTrack = x._2.map(x => (x._2, x._3, x._4, x._5)).toList

      // item_id,category_id,pt
      val userCategoryScore = userTrack.groupBy(x => x._2).map(x => {
        val variance = getDateVar(x._2.map(x => x._4))
        val cntSum = x._2.map(x => 1.0 / (1 + Math.exp(0.5 * getDateDiff(bizdate, x._4, "yyyy-MM-dd")))).sum
        val minTime = x._2.map(x => getDateDiff(bizdateSub, x._4, "yyyy-MM-dd")).min
        (x._1, Math.log(cntSum + 1) / Math.log(1.5d) * (1 + variance), Math.log(cntSum + 1) / Math.log(1.5d), variance, minTime)
      }).toList.sortWith((a, b) => a._2 > b._2)

      val sumScore = userCategoryScore.map(x => x._2).sum
      val result = new ArrayList[(String, String, Double, Double, Double, Double, Double)]()
      var n = 0
      var i = 0
      val itemInCategory = x._2.groupBy(x => x._3).map(x => (x._1, x._2.groupBy(x => x._2).map(x => x._2.head)))
      while (n <= N && i < userCategoryScore.size) {
        val itemList = itemInCategory.get(userCategoryScore.apply(i)._1).get.toList.sortWith((a, b) => a._4 > b._4)
        val size = Math.min(itemList.size, Math.round(1.0 * N * userCategoryScore.apply(i)._2 / sumScore)).toInt
        result.addAll(itemList.slice(0, size).map(x => (x._1, x._2, getDateDiff(bizdate, x._5, "yyyy-MM-dd"), userCategoryScore.apply(i)._2, userCategoryScore.apply(i)._3, userCategoryScore.apply(i)._4, userCategoryScore.apply(i)._5)))
        n += size
        i += 1
      }
      result.toList.take(N)
    })
    userItemPrefer.take(20).foreach(println)

    val itemSim = sc.textFile(itemSimPath).map(x => (x.split(" ")(0), x.split(" ")(1).split(",").map(x => (x.split(":")(0), x.split(":")(1).toDouble / 100000)).toList.take(simItemNum))).collect().toMap
    val itemCtr = sc.textFile(itemCtrPath).map(x => (x.split(" ")(0), x.split(" ")(1).toDouble / 1000000d)).collect().toMap

    val userItemSimFeature = userItemPrefer.flatMap(x => x).map(x => {
      val result = new ArrayList[(String, String, Double, Double, Double, Double, Double, Double, Double, Double)]()
      val itemSet = new HashSet[String]()
      if (itemSim.contains(x._2)) {
        val list = itemSim.get(x._2).get
        for (i <- 1 to list.size) {
          val itemId = list.apply(i - 1)._1
          val score = list.apply(i - 1)._2
          val ctrScore = itemCtr.getOrElse(itemId, 0d)
          if (!itemSet.contains(itemId)) {
            result.add((x._1, itemId, x._3, x._4, x._5, x._6, x._7, score, i.toDouble, ctrScore))
            itemSet.add(itemId)
          }
        }
      } else {
        if (!itemSet.contains(x._2)) {
          val ctrScore = itemCtr.getOrElse(x._2, 0d)
          result.add((x._1, x._2, x._3, x._4, x._5, x._6, x._7, 1d, 0d, ctrScore))
          itemSet.add(x._2)
        }
      }
      result.toList
    }).flatMap(x => x).map(x => (x._1, x._2, Arrays.toString(Array(x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10))))
    userItemSimFeature.take(20).foreach(println)
    userItemPrefer.unpersist(blocking = false)

    val userItemSimFeatureRow = userItemSimFeature.map(x => Row(x._1, x._2, x._3.toString))
    val schema =
      StructType(
        StructField("user_id", StringType, true)
          :: StructField("item_id", StringType, true)
          :: StructField("feature", StringType, true)
          :: Nil
      )

    val predictDF: DataFrame = sqlContext.createDataFrame(userItemSimFeatureRow, schema)
    userItemSimFeatureRow.unpersist(blocking = false)
    predictDF.registerTempTable("s_dg_user_item_prefer_feature_spark")
    predictDF.show()

    sqlContext.udf.register("to_vector", (vector: String) => (Vectors.parse(vector)))
    sqlContext.udf.register("to_double", (label: String) => (label.toDouble))

    val featureDF = sqlContext.sql("select to_vector(feature) as feature, user_id, item_id from s_dg_user_item_prefer_feature_spark")
    predictDF.unpersist(blocking = false)
    val learner: LRLearner = new LRLearner()
    val result = learner.predict(model, featureDF, "user_id", "item_id")
    featureDF.unpersist(blocking = false)

    def sort(x: Iterable[(String, String, Double)], N: Int): String = {
      val itemSet = new HashSet[String]()
      val result = new ArrayList[String]()
      val list = x.toList.sortWith((a, b) => a._3.compareTo(b._3) > 0)
      for (e <- list) {
        if (result.size() < N) {
          if (!itemSet.contains(e._2)) {
            result.add(e._2 + ":" + Math.round(e._3 * 100000))
            itemSet.add(e._2)
          }
        }
      }
      return result.mkString(",")
    }

    result.map(x => (x(0), x(1), x(2).toDouble)).groupBy(x => x._1).filter(x => x._2.size > 0 && x._1.toLong > 0).map(x => x._1 + " " + sort(x._2, 50)).saveAsTextFile(userItemPreferPath)

    val sdfyyyyMMdd = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    HiveUtil.featureHdfsToHive(sc, sqlContext, "user_item_prefer", userItemPreferPath, sdfyyyyMMdd.format(calendar.getTime), "s_dg_user_item_prefer", FeatureType.USER)
  }
}
