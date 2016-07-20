package com.mgj.ml.sample

import java.text.SimpleDateFormat
import java.util

import com.mgj.utils.{LRLearner, SourceFilter}
import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 11/18/15.
  */
object BuildPairSample {

  val SPLITTER = "#"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("build sample").
      set("spark.sql.parquet.binaryAsString", "true")
    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    val bizDate = args(0)
    val bizDateFeature = args(1)
    val bizDateFeatureSub = args(2)
    val today = args(3)
    //    val bizDate = "20160117"
    val gmvSql = "select userid as user_id, tradeitemid as item_id, visit_time as time, refer, gmv from qd_order_log where visit_date = '" + bizDate + "'"

    println("get gmv sql:")
    println("{" + gmvSql + "}")

    val gmvLog = sqlContext.sql(gmvSql).rdd.filter(x => x.anyNull == false).filter(x => SourceFilter.isFrom("app_book", x(3).toString)).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString))
    val itemInfo = sqlContext.sql("select tradeitemid as item_id, cid as category_id from v_dw_trd_tradeitem").rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString))
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val sdfMaker = new SimpleDateFormat("yyyy-MM-dd")
    //    val pt = sdfMaker.format(sdf.parse(bizDate).getTime)
    val pt = sqlContext.sql("show partitions dw_trd_item_info").rdd.filter(x => x.anyNull == false).map(x => x(0).toString.split("=")(1)).sortBy(x => x, false).take(1).apply(0)
    val itemBaseInfo = sqlContext.sql("select tradeitemid,pv_1day,dpv_1day from dw_trd_item_info where visit_date = '" + pt + "'").rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString))
    val M = itemBaseInfo.map(x => (x._1, x._2.toDouble)).groupBy(x => x._1).map(x => (x._1, x._2.map(x => x._2).sum)).sortBy(x => x._2, false).map(x => x._2).take(1000).takeRight(1).apply(0)
    val ctrRaw = itemBaseInfo.map(x => (x._1, (x._2.toDouble, x._3.toDouble))).groupBy(x => x._1).map(x => (x._1, x._2.map(x => x._2._1).sum, x._2.map(x => x._2._2).sum)).map(x => (x._1, x._2, x._3))
    val avgCtrTemp = ctrRaw.map(x => (x._2, x._3)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val avgCtr = avgCtrTemp._2 / avgCtrTemp._1
    val ctr = ctrRaw.map(x => (x._1, x._3 / (x._2 + M) + avgCtr)).sortBy(x => x._2, false)
    ctr.take(100).foreach(println)
    val gmvLogWithInfo = gmvLog.map(x => (x._2, x)).join(itemInfo).map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._2))

    val itemGmvV2 = gmvLogWithInfo.groupBy(x => x._2 + SPLITTER + x._6).map(x => (x._1.split(SPLITTER)(0), x._1.split(SPLITTER)(1), x._2.map(x => x._5.toDouble).sum)).map(x => (x._1, x)).join(ctr).map(x => (x._2._1._1, x._2._1._2, x._2._1._3 * x._2._2))

    val itemGmv = gmvLogWithInfo.groupBy(x => x._2).map(x => (x._1, x._2.map(x => x._5.toDouble).sum)).join(ctr).map(x => (x._1, x._2._1 * x._2._2))

    val gmvSortedV2 = itemGmvV2.groupBy(x => x._2).map(x => {
      val list = x._2.map(x => (x._1, x._3)).toList.sortWith((a, b) => a._2 > b._2)
      (x._1, list)
    }).sortBy(x => x._2.map(x => x._2).sum, false)

    val gmvSorted = itemGmv.sortBy(x => x._2, false)

    gmvSortedV2.take(10).map(x => x._2).flatMap(x => x).take(100).foreach(println)

    gmvSorted.take(100).foreach(println)

    val sampleList = new util.ArrayList[(String, String, Double, Double, Double)]()
    val list = gmvSorted.collect()

    var posStep = 50
    var negStep = 50
    for (i <- 0 to list.size - 1) {
      if (i + posStep < list.size) {
        if (!list(i).equals(list(i + posStep))) {
          sampleList.add((list(i)._1, list(i + posStep)._1, 1d, list(i)._2, list(i + posStep)._2))
        }
      }
    }

    for (i <- list.size - 1 to 0 by -1) {
      if (i - negStep >= 0) {
        if (!list(i).equals(list(i - negStep))) {
          sampleList.add((list(i)._1, list(i - negStep)._1, 0d, list(i)._2, list(i - negStep)._2))
        }
      }
    }

    posStep = 100
    negStep = 100
    for (i <- 0 to list.size - 1) {
      if (i + posStep < list.size) {
        if (!list(i).equals(list(i + posStep))) {
          sampleList.add((list(i)._1, list(i + posStep)._1, 1d, list(i)._2, list(i + posStep)._2))
        }
      }
    }

    for (i <- list.size - 1 to 0 by -1) {
      if (i - negStep >= 0) {
        if (!list(i).equals(list(i - negStep))) {
          sampleList.add((list(i)._1, list(i - negStep)._1, 0d, list(i)._2, list(i - negStep)._2))
        }
      }
    }

    val step = Math.round(list.size / 2)

    for (i <- 0 to list.size - 1) {
      if (i + step < list.size) {
        if (!list(i).equals(list(i + step))) {
          sampleList.add((list(i)._1, list(i + step)._1, 1d, list(i)._2, list(i + step)._2))
        }
      }
    }

    for (i <- list.size - 1 to 0 by -1) {
      if (i - step >= 0) {
        if (!list(i).equals(list(i - step))) {
          sampleList.add((list(i)._1, list(i - step)._1, 0d, list(i)._2, list(i - step)._2))
        }
      }
    }
    sampleList.toList

    //    val sampleV2 = gmvSortedV2.map(x => {
    //      val sampleList = new util.ArrayList[(String, String, Double, Double, Double)]()
    //      val list = x._2
    //
    //      var posStep = 50
    //      var negStep = 50
    //      for (i <- 0 to list.size - 1) {
    //        if (i + posStep < list.size) {
    //          if (!list(i).equals(list(i + posStep))) {
    //            sampleList.add((list(i)._1, list(i + posStep)._1, 1d, list(i)._2, list(i + posStep)._2))
    //          }
    //        }
    //      }
    //
    //      for (i <- list.size - 1 to 0 by -1) {
    //        if (i - negStep >= 0) {
    //          if (!list(i).equals(list(i - negStep))) {
    //            sampleList.add((list(i)._1, list(i - negStep)._1, 0d, list(i)._2, list(i - negStep)._2))
    //          }
    //        }
    //      }
    //
    //      //      posStep = 25
    //      //      negStep = 25
    //      //      for (i <- 0 to list.size - 1) {
    //      //        if (i + posStep < list.size) {
    //      //          if (!list(i).equals(list(i + posStep))) {
    //      //            sampleList.add((list(i)._1, list(i + posStep)._1, 1d, list(i)._2, list(i + posStep)._2))
    //      //          }
    //      //        }
    //      //      }
    //      //
    //      //      for (i <- list.size - 1 to 0 by -1) {
    //      //        if (i - negStep >= 0) {
    //      //          if (!list(i).equals(list(i - negStep))) {
    //      //            sampleList.add((list(i)._1, list(i - negStep)._1, 0d, list(i)._2, list(i - negStep)._2))
    //      //          }
    //      //        }
    //      //      }
    //
    //      posStep = 100
    //      negStep = 100
    //      for (i <- 0 to list.size - 1) {
    //        if (i + posStep < list.size) {
    //          if (!list(i).equals(list(i + posStep))) {
    //            sampleList.add((list(i)._1, list(i + posStep)._1, 1d, list(i)._2, list(i + posStep)._2))
    //          }
    //        }
    //      }
    //
    //      for (i <- list.size - 1 to 0 by -1) {
    //        if (i - negStep >= 0) {
    //          if (!list(i).equals(list(i - negStep))) {
    //            sampleList.add((list(i)._1, list(i - negStep)._1, 0d, list(i)._2, list(i - negStep)._2))
    //          }
    //        }
    //      }
    //
    //      val step = Math.round(list.size / 2)
    //
    //      for (i <- 0 to list.size - 1) {
    //        if (i + step < list.size) {
    //          if (!list(i).equals(list(i + step))) {
    //            sampleList.add((list(i)._1, list(i + step)._1, 1d, list(i)._2, list(i + step)._2))
    //          }
    //        }
    //      }
    //
    //      for (i <- list.size - 1 to 0 by -1) {
    //        if (i - step >= 0) {
    //          if (!list(i).equals(list(i - step))) {
    //            sampleList.add((list(i)._1, list(i - step)._1, 0d, list(i)._2, list(i - step)._2))
    //          }
    //        }
    //      }
    //      sampleList.toList
    //    }).flatMap(x => x)

    val sample = sc.parallelize(sampleList.toList) //.union(sampleV2)
    println("sampleCount: " + sample.count)

    //    val LTRScoreOld = sc.textFile("/user/digu/LTR_FEATURE/old_ctr_score_sub").map(x => (x.split(" ")(0), x.split(" ")(1))).collect.toMap
    //    val LTRScoreNew = sc.textFile("/user/digu/LTR_FEATURE/new_ctr_score_sub").map(x => (x.split(",")(0), x.split(",")(1))).collect.toMap
    //
    //    val oldScore = sample.map(x => (LTRScoreOld.get(x._1).getOrElse(0).toString.toDouble - LTRScoreOld.get(x._2).getOrElse(0).toString.toDouble, x._3))
    //    val newScore = sample.map(x => (LTRScoreNew.get(x._1).getOrElse(0).toString.toDouble - LTRScoreNew.get(x._2).getOrElse(0).toString.toDouble, x._3))
    //
    //    println(new BinaryClassificationMetrics(oldScore).areaUnderROC())
    //    println(new BinaryClassificationMetrics(newScore).areaUnderROC())

    //    val schema =
    //      StructType(
    //        StructField("item_idA", StringType, true) ::
    //          StructField("item_idB", StringType, true) ::
    //          StructField("label", DoubleType, true) :: Nil)
    //
    //    val sampleDF = sqlContext.createDataFrame(sample.map(x => Row(x._1, x._2, x._3)), schema)
    //
    //    sampleDF.registerTempTable("s_dg_gmv_pair_sample_temp")
    //
    //    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    //    sqlContext.sql("drop table if exists s_dg_gmv_pair_sample")
    //    sqlContext.sql("create table s_dg_gmv_pair_sample as select * from s_dg_gmv_pair_sample_temp")

    //    val bizDateFeature = "20160114"
    //    val bizDateFeatureSub = "20160113"
    //    val path = "/user/wujia/ltr/cvr/" + bizDateFeature + "/" + bizDateFeatureSub + ".train.app"
    val path = "/user/wujia/ltr/bookorder/cvr/" + bizDateFeature + "/" + bizDateFeatureSub + ".train.app"
    println("path: " + path)
    val feature = sc.textFile(path).filter(x => NumberUtils.isNumber(x.split(",", 2)(0))).map(x => {
      val featureList = x.split(",", 2)(1).split(",")
      (x.split(",", 2)(0), featureList.toList.map(x => x.toDouble).takeRight(featureList.size - 1))
    })

    val sampleRow = sample.map(x => (x._1, x._2, x._3)).map(x => (x._1, x)).join(feature).map(x => (x._2._1._2, (x._2._2, x._2._1._3))).join(feature).map(x => (x._2._1._1, x._2._2, x._2._1._2)).map(x => {
      val feature: Array[Double] = new Array[Double](x._1.size)
      for (i <- 0 to x._1.size - 1) {
        feature(i) = x._1.get(i) - x._2.get(i)
      }
      Row(util.Arrays.toString(feature), x._3.toString)
    })

    val schema =
      StructType(
        StructField("feature", StringType, true)
          :: StructField("label", StringType, true)
          :: Nil)

    var sampleDF = sqlContext.createDataFrame(sampleRow, schema)
    sampleDF.show()

    //    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    //    sampleDF.registerTempTable("s_wj_sample_temp")
    //    sqlContext.sql("drop table if exists s_wj_sample")
    //    sqlContext.sql("create table s_wj_sample as select * from s_wj_sample_temp")

    sqlContext.sql("drop table if exists s_wj_sample")
    sampleDF.write.saveAsTable("s_wj_sample")

    sqlContext.udf.register("to_vector", (vector: String) => (Vectors.parse(vector)))
    sqlContext.udf.register("to_double", (label: String) => (label.toDouble))
    sampleDF = sqlContext.sql("select to_vector(feature) as feature, to_double(label) as label from s_wj_sample").cache()
    val learner = new LRLearner()
    val model = learner.train(sc, sqlContext, sampleDF)

    //    val pathPredict = "/user/wujia/ltr/cvr/" + today + "/" + today + ".train.app"
    val pathPredict = "/user/digu/LTR_FEATURE/feature_predict"
    val featurePredict = sc.textFile(pathPredict).filter(x => NumberUtils.isNumber(x.split(",", 2)(0))).map(x => {
      val featureList = x.split(",", 2)(1).split(",")
      (x.split(",", 2)(0), featureList.toList.map(x => x.toDouble).takeRight(featureList.size - 1))
    })

    val schemaPredict =
      StructType(
        StructField("item_id", StringType, true)
          :: StructField("feature", StringType, true)
          :: Nil)

    val featurePredictDF = sqlContext.createDataFrame(featurePredict.map(x => Row(x._1, util.Arrays.toString(x._2.toArray))), schemaPredict)
    featurePredictDF.registerTempTable("s_wj_sample_predict")
    featurePredictDF.show()

    val featureDF = sqlContext.sql("select to_vector(feature) as feature, item_id from s_wj_sample_predict")
    val result = model.transform(featureDF).select("item_id", "probability").map(x => (x(0).toString, x.getAs[Vector](1)(1)))
    val resultSorted = result.sortBy(x => x._2, false).collect()
    resultSorted.take(10).foreach(println)
    var rank = 1
    val rankResult = new util.ArrayList[(String, Int)]()
    for (x <- resultSorted) {
      rankResult.add((x._1, rank))
      rank += 1
    }
    sc.parallelize(rankResult).map(x => x._1 + "," + x._2 + "," + x._2).saveAsTextFile("/user/digu/LTR_FEATURE/pair_predict")

    //    sqlContext.sql("select * from")
    println("THE END")
  }
}
