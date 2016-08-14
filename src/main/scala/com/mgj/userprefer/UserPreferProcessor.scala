package com.mgj.userprefer

import java.text.SimpleDateFormat
import java.util
import java.util.HashMap

import com.mgj.feature.FeatureConstant
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Created by xiaonuo on 8/3/16.
  */
class UserPreferProcessor extends java.io.Serializable {

  val USER_ID = "user_id"
  val ENTITY_ID = "entity_id"
  val TIME = "time"
  val N = 30
  val M = 2 * N

  private def getFeatureLog(sc: SparkContext, sqlContext: HiveContext, bizdateSubA: String, bizdateSubB: String, entity: String, logType: String): DataFrame = {
    val log = logType match {
      case "click" => sqlContext.sql(UserBaseLogSqlFactory.getClickFeatureSql(bizdateSubA, bizdateSubB, entity))
      case "order" => sqlContext.sql(UserBaseLogSqlFactory.getOrderFeatureSql(bizdateSubA, bizdateSubB, entity))
      case "favor" => sqlContext.sql(UserBaseLogSqlFactory.getFavorFeatureSql(bizdateSubA, bizdateSubB, entity))
      case "add_cart" => sqlContext.sql(UserBaseLogSqlFactory.getAddCartFeatureSql(bizdateSubA, bizdateSubB, entity))
      case _ => throw new Exception(s"log type ${logType} does not exists")
    }
    return log
  }

  private def getSampleLog(sc: SparkContext, sqlContext: HiveContext, bizdate: String, entity: String, sampleType: String): DataFrame = {
    val log = sampleType match {
      case "click" => sqlContext.sql(UserBaseLogSqlFactory.getClickSampleSql(bizdate, entity))
      case "order" => sqlContext.sql(UserBaseLogSqlFactory.getOrderSampleSql(bizdate, entity))
      case _ => throw new Exception(s"sample type ${sampleType} does not exists")
    }
    return log
  }

  private def getFeature(sc: SparkContext, sqlContext: HiveContext, bizdateSubA: String, bizdateSubB: String, entity: String, logType: String): RDD[((String, String), Array[Double])] = {
    val logDF = getFeatureLog(sc, sqlContext, bizdateSubA, bizdateSubB, entity, logType)
    import sqlContext.implicits._
    val logDS = logDF.as[(String, String, String)]
      .filter(x => x._1 != null && x._2 != null && x._3 != null && !x._2.equals("-1"))
    logDF.unpersist(blocking = false)

    val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val sdfConvert = new SimpleDateFormat("yyyy-MM-dd")
    val dateCurrent = sdfConvert.parse(bizdateSubB)

    def getDiff(x: String): Int = {
      val date = sdf.parse(x)
      val diff = Math.ceil(1.0 * (dateCurrent.getTime - date.getTime) / (60 * 60 * 1000 * 24)).toInt
      return diff
    }

    val totalCount = logDS.groupBy(x => getDiff(x._3)).count().collect().toMap
    val entityProb = logDS.groupBy(x => (x._2, getDiff(x._3))).count().map(x => (x._1, 1.0 * x._2 / totalCount.get(x._1._2).get)).cache()

    val smoothNum = entityProb.rdd.groupBy(x => x._1._2).map(x => {
      val list = x._2.toList.sortWith((a, b) => a._2 > b._2)
      val index = Math.floor(list.size * 0.618).toInt
      (x._1, list.apply(index)._2)
    }).collect().toMap

    val entityProbMap = entityProb.collect().toMap
    entityProb.unpersist(blocking = false)

    //    println("totalCount")
    //    totalCount.toList.sortWith((a, b) => a._2 > b._2).take(10).foreach(println)
    //    println("entityProbMap")
    //    entityProbMap.toList.sortWith((a, b) => a._2 > b._2).take(10).foreach(println)
    //    println("smoothNum")
    //    smoothNum.toList.sortWith((a, b) => a._2 > b._2).take(10).foreach(println)

    def featureExtract(iterable: Iterable[(String, String, String)]): List[((String, String), Array[Double])] = {

      val userId = iterable.head._1
      val feature: HashMap[(String, Int), Double] = new HashMap[(String, Int), Double]()

      for (log <- iterable) {
        val diff = getDiff(log._3)
        val key = (log._2, diff)
        if (!feature.containsKey(key)) {
          feature.put(key, 0d)
        }
        feature.put(key, feature.get(key) + 1d)
      }

      val featureArray: HashMap[String, Array[Double]] = new HashMap[String, Array[Double]]()
      val entitySet = iterable.map(x => x._2).toSet
      for (entityId <- entitySet) {
        featureArray.put(entityId, new Array[Double](N * 2))
      }
      val sum = feature.map(x => (x._1._2, x._2)).groupBy(x => x._1).map(x => (x._1, x._2.map(x => x._2).sum))

      for (entityId <- entitySet) {
        for (i <- 0 to N - 1) {
          val key = (entityId, i)
          if (feature.containsKey(key)) {
            featureArray.get(entityId)(i) = 1.0 * feature.get(key) / (sum.get(i).get * entityProbMap.get(key).get)
            //            featureArray.get(entityId)(i) = 1.0 * feature.get(key) / (sum.get(i).get * (entityProbMap.get(key).get + smoothNum.get(i).get))
            //            featureArray.get(entityId)(i) = feature.get(key) / sum.get(i).get
            //            featureArray.get(entityId)(i) = feature.get(key)
          }
        }

        for (i <- 0 to N - 1) {
          val key = (entityId, i)
          if (feature.containsKey(key)) {
            featureArray.get(entityId)(i + N) = Math.log(1 + entityProbMap.get(key).get)
          }
        }
      }
      val result = featureArray.map(x => ((userId, x._1), x._2)).toList
      return result
    }

    val feature = logDS.rdd
      .groupBy(x => x._1)
      .map(x => featureExtract(x._2))
      .flatMap(x => x)

    logDS.unpersist(blocking = false)

    return feature
  }

  private def joinFeature(featureList: util.ArrayList[RDD[((String, String), Array[Double])]]): RDD[(String, String, Vector)] = {
    val feature = joiner(featureList.toSeq).map(x => {
      val result = new Array[Double](2 * N * x._2.length)

      for (i <- 0 to x._2.length - 1) {
        val list = if (x._2(i).size > 0) {
          x._2(i).toList.apply(0).asInstanceOf[Array[Double]]
        } else {
          new Array[Double](2 * N)
        }

        for (j <- 0 to list.length - 1) {
          result.update(i * 2 * N + j, list.apply(j))
        }

      }
      (x._1._1, x._1._2, Vectors.dense(result))
    })

    return feature
  }

  private def joiner[K: ClassTag, V](seq: Seq[RDD[_ <: Product2[K, _]]]): CoGroupedRDD[K] = {
    val partitioner: org.apache.spark.Partitioner = new HashPartitioner(100)
    val cg = new CoGroupedRDD[K](seq, partitioner)
    return cg
  }

  def getReason(vector: String, model: LogisticRegressionModel, size: Int) = {
    def getReasonScore(feature: Array[Double], weight: Array[Double], start: Int, end: Int): Double = {
      var sum = 0d
      for (i <- start to end) {
        sum += feature.apply(i) * weight.apply(i)
      }
      return sum
    }
    val featureVector = Vectors.parse(vector).toArray
    val weightClick = model.coefficients.toArray

    val reasonScoreList: util.ArrayList[Double] = new util.ArrayList[Double]()
    for (i <- 0 to size) {
      reasonScoreList.add(getReasonScore(featureVector, weightClick, i * 2 * N, (i + 1) * 2 * N - 1))
    }
    val max = reasonScoreList.max
    val index = reasonScoreList.indexOf(max)
    index.toString
  }

  def buildFeature(sc: SparkContext, sqlContext: HiveContext, bizdateSubA: String, bizdateSubB: String, entity: String, logTypeList: String*): RDD[(String, String, Vector)] = {
    val featureList = new util.ArrayList[RDD[((String, String), Array[Double])]]()
    for (logType <- logTypeList) {
      featureList.add(getFeature(sc, sqlContext, bizdateSubA, bizdateSubB, entity, logType))
    }
    val feature = joinFeature(featureList).cache()
    featureList.map(x => x.unpersist(blocking = false))
    return feature
  }

  def buildSampleV2(sc: SparkContext, sqlContext: HiveContext, feature: RDD[(String, String, Vector)], bizdate: String, entity: String, sampleType: String): DataFrame = {
    val sampleLogRaw = getSampleLog(sc, sqlContext, bizdate, entity, sampleType)
      .where("user_id is not null and entity_id is not null and time is not null")
    val sampleLog = sampleLogRaw
      .select(sampleLogRaw("user_id").as("user_id_alias"), sampleLogRaw("entity_id").as("entity_id_alias"), sampleLogRaw("time").as("time"))

    val schema = StructType(
      StructField("user_id", StringType, true)
        :: StructField("entity_id", StringType, true)
        :: StructField("feature", StringType, true)
        :: Nil)

    val featureRow = feature.map(x => Row(x._1, x._2, x._3.toString))
    val featureDF = sqlContext.createDataFrame(featureRow, schema)
    featureRow.unpersist(blocking = false)

    val getLabel = udf { (label: String) => if (label == None || label == null) 0d else 1d }
    val sample = featureDF.join(sampleLog, featureDF("user_id") === sampleLog("user_id_alias") && featureDF("entity_id") === sampleLog("entity_id_alias"), "left_outer")
      .drop("user_id_alias")
    featureDF.unpersist(blocking = false)

    val sampleLabel = sample.select(sample("user_id"), sample("entity_id"), sample("feature"), getLabel(sample("entity_id_alias")).as("label")).repartition(2000).cache()
    val ratioCount = sampleLabel.groupBy("label").count().rdd.map(x => (x(0).toString, x(1).toString.toDouble)).collect().toMap
    val ratio = ratioCount.get("1.0").get / (ratioCount.get("1.0").get + ratioCount.get("0.0").get)

    println(s"sample type:${sampleType}")
    println(s"total sample count:${(ratioCount.get("1.0").get + ratioCount.get("0.0").get)}")
    println(s"total positive sample count:${ratioCount.get("1.0").get}")
    println(s"positive negtive sample ratio:${ratio}")

    val posSample = sampleLabel.where(sampleLabel("label") > 0.5)
    val negSample = sampleLabel.where(sampleLabel("label") < 0.5).sample(false, ratio)
    sampleLabel.unpersist(blocking = false)

    val sampleDF = posSample.unionAll(negSample)
    posSample.unpersist(blocking = false)
    negSample.unpersist(blocking = false)

    return sampleDF
  }

  def buildSample(sc: SparkContext, sqlContext: HiveContext, feature: RDD[(String, String, Vector)], bizdate: String, entity: String, sampleType: String): DataFrame = {
    val sampleLog = getSampleLog(sc, sqlContext, bizdate, entity, sampleType).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString))
    val sample = feature.map(x => ((x._1, x._2), x._3)).leftOuterJoin(sampleLog.map(x => ((x._1, x._2), 1d))).map(x => {
      if (x._2._2 == None) {
        (x._2._1, 0d)
      } else {
        (x._2._1, 1d)
      }
    })
    sampleLog.unpersist(blocking = false)

    val ratioCount = sample.map(x => (x._2.toDouble, 1d)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    val ratio = ratioCount._1 / ratioCount._2

    println(s"sample type:${sampleType}")
    println(s"total sample count:${ratioCount._2}")
    println(s"total positive sample count:${ratioCount._1}")
    println(s"positive negtive sample ratio:${ratio}")

    val posSample = sample.filter(x => x._2 > 0.5)
    val negSample = sample.filter(x => x._2 < 0.5).sample(false, ratio)
    sample.unpersist(blocking = false)

    val sampleFinal = posSample.union(negSample).map(x => Row(x._1.toString, x._2))
    posSample.unpersist(blocking = false)
    negSample.unpersist(blocking = false)

    val schema =
      StructType(
        StructField("feature", StringType, true)
          :: StructField("label", DoubleType, true)
          :: Nil)

    val sampleDF: DataFrame = sqlContext.createDataFrame(sampleFinal, schema)
    sampleFinal.unpersist(blocking = false)
    return sampleDF
  }
}
