package com.mgj.cf

import java.text.SimpleDateFormat
import java.util

import com.mgj.utils.{GBRTLearner, LRLearner}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 1/30/16.
  */
object ItemGraphSimSample {
  val SPLITTER = "#"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("calculate item cf score")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    // User click log: user_id, item_id, visit_time.
    val bizdate = args(1)
    val bizdateSub = args(0)
    val itemSimModel = args(2)

    //    val clickLogSql = "select user_id, item_id, category_id, time from s_dg_user_base_log where pt >= '" + bizdateSub + "' and pt <= '" + bizdate + "' and action_type = 'click' and platform_type = 'app'"
    val clickLogSql = "select user_id, item_id, category_id, time from s_dg_user_base_log where pt = '" + bizdateSub + "' and action_type = 'click' and platform_type = 'app'"
    val userBaseLog = sqlContext.sql(clickLogSql).rdd.filter(r => r.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString)).repartition(500)

    def compare(timex: String, timey: String): Boolean = {
      val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val datex = df.parse(timex)
      val datey = df.parse(timey)

      if (datex.after(datey)) {
        return true
      } else {
        return false
      }
    }

    def getTimeDiff(visitTimex: String, visitTimey: String): Double = {
      val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val datex = df.parse(visitTimex)
      val datey = df.parse(visitTimey)
      return Math.abs(1.0 * (datex.getTime() - datey.getTime()) / 1000 / 60)
    }

    val N = 50
    val itemPair = userBaseLog.groupBy(x => x._1 + x._3).map(x => {
      val itemList = x._2.toList.sortWith((a, b) => compare(a._4, b._4))
      val itemPair = new util.ArrayList[(String, String)]()
      if (itemList.size <= N) {
        for (i <- 0 to itemList.size - 2) {
          val itemx = itemList.get(i)
          val itemy = itemList.get(i + 1)
          if (getTimeDiff(itemx._4, itemy._4) < 5 && !itemx._2.equals(itemy._2)) {
            itemPair.add((itemx._2, itemy._2))
            itemPair.add((itemy._2, itemx._2))
          }
        }
      }
      itemPair.toList
    }).filter(x => x.size > 0).flatMap(x => x)

    //    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    val isSameCategory = udf { (vector: String) => if (Vectors.parse(vector).apply(6) == 1.0) true else false }
    val featureOriginal = sqlContext.sql("select * from s_dg_item_sim_feature")
    val feature = featureOriginal.filter(isSameCategory(featureOriginal("feature"))).map(x => (x(0).toString, x(1).toString, x(2).toString))

    val sample = feature.map(x => (x._1 + SPLITTER + x._2, x._3)).leftOuterJoin(itemPair.map(x => (x._1 + SPLITTER + x._2, 1d))).map(x => {
      if (x._2._2 == None || x._2._2 == null) {
        (x._1.split(SPLITTER)(0), x._1.split(SPLITTER)(1), x._2._1, 0d)
      } else {
        (x._1.split(SPLITTER)(0), x._1.split(SPLITTER)(1), x._2._1, x._2._2.get)
      }
    })

    val ratioCount = sample.map(x => (x._4, 1d)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    val ratio = ratioCount._1 / ratioCount._2 * 1d / 3d

    println("total sample count: " + ratioCount._2)
    println("total positive sample count: " + ratioCount._1)
    println("positive negtive sample ratio: " + ratio)

    val posSample = sample.filter(x => x._4 > 0.5)
    val negSample = sample.filter(x => x._4 < 0.5).sample(false, ratio)

    val sampleFinal = posSample.union(negSample).map(x => Row(x._1.toString, x._2.toString, x._3.toString, x._4.toString))

    val schema =
      StructType(
        StructField("itemx", StringType, true)
          :: StructField("itemy", StringType, true)
          :: StructField("feature", StringType, true)
          :: StructField("label", StringType, true)
          :: Nil)

    val resultDF = sqlContext.createDataFrame(sampleFinal, schema)
    //    resultDF.registerTempTable("s_dg_item_sim_sample_temp")
    //    sqlContext.sql("drop table if exists s_dg_item_sim_sample")
    //    sqlContext.sql("create table s_dg_item_sim_sample as select * from s_dg_item_sim_sample_temp")
    sqlContext.sql("drop table if exists s_dg_item_sim_sample")
    resultDF.write.saveAsTable("s_dg_item_sim_sample")

    sqlContext.udf.register("to_vector", (vector: String) => (Vectors.parse(vector)))
    sqlContext.udf.register("to_double", (label: String) => (label.toDouble))
    val sampleDF: DataFrame = sqlContext.sql("select to_vector(feature) as feature, to_double(label) as label from s_dg_item_sim_sample")
    //    val summary = Statistics.colStats(sampleDF.map(x => Vectors.parse(x(0).toString)))
    //    println("mean:" + summary.mean)
    //    println("max:" + summary.max)
    //    println("min:" + summary.min)
    val learner: LRLearner = new LRLearner()
    val model = learner.train(sc, sqlContext, sampleDF)
    sc.parallelize(Seq(model), 1).saveAsObjectFile(itemSimModel)

    //    val learner: GBRTLearner = new GBRTLearner()
    //    val model = learner.train(sc, sqlContext, sampleDF)
    //    sc.parallelize(Seq(model), 1).saveAsObjectFile(itemSimModel)

  }
}
