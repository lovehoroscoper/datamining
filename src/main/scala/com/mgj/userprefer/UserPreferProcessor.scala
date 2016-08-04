package com.mgj.userprefer

import java.text.SimpleDateFormat
import java.util
import java.util.HashMap

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
class UserPreferProcessor {

  val USER_ID = "user_id"
  val ENTITY_ID = "entity_id"
  val TIME = "time"
  val N = 30

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
    val logDS = logDF.as[(String, String, String)].filter(x => x._1 != null && x._2 != null && x._3 != null)

    val totalCount = logDS.count()
    val entityProb = logDS.groupBy(x => x._2).count().map(x => (x._1, 1.0 * x._2 / totalCount))

    val entityCount = entityProb.count()
    val smoothNum = entityProb.rdd.map(_.swap).sortByKey().map(_.swap).zipWithIndex()
      .filter(x => x._2 == Math.round(entityCount * 0.618))
      .map(x => if (x._1._2 == 0) 0.5 else x._1._2).collect().apply(0)

    println(s"totalCount:${totalCount}")
    println(s"entityCount:${entityCount}")
    println(s"smoothNum:${smoothNum}")

    val entityProbMap = entityProb.collect().toMap
    println(s"entityProbMap:${entityProbMap}")

    def featureExtract(iterable: Iterable[(String, String, String)]): Array[Double] = {
      val entityId = iterable.head._2

      val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val dateCurrent = new SimpleDateFormat("yyyy-MM-dd").parse(bizdateSubB)

      val feature: HashMap[Integer, Double] = new HashMap[Integer, Double]()

      for (log <- iterable) {
        val date = df.parse(log._3)
        val diff: Double = Math.ceil(1.0 * (dateCurrent.getTime - date.getTime) / (60 * 60 * 1000 * 24))
        if (!feature.containsKey(diff.toInt)) {
          feature.put(diff.toInt, 0d)
        }
        feature.put(diff.toInt, feature.get(diff.toInt) + 1d)
      }

      val featureArray: Array[Double] = new Array[Double](N)
      for (i <- 0 to N - 1) {
        if (feature.containsKey(i)) {
          featureArray(i) = feature.get(i) / iterable.size / (entityProbMap.get(entityId).getOrElse(0d) + smoothNum)
        } else {
          featureArray(i) = 0
        }
      }
      return featureArray
    }

    val feature = logDS.rdd
      .groupBy(x => (x._1, x._2))
      .map(x => (x._1, featureExtract(x._2)))

    return feature
  }

  private def joinFeature(featureList: util.ArrayList[RDD[((String, String), Array[Double])]]): RDD[(String, String, Vector)] = {
    val feature = joiner(featureList.toSeq).map(x => {
      val result = new Array[Double](N * x._2.length)

      for (i <- 0 to x._2.length - 1) {
        val list = if (x._2(i).size > 0) {
          x._2(i).toList.apply(0).asInstanceOf[Array[Double]]
        } else {
          new Array[Double](N)
        }

        for (j <- 0 to list.length - 1) {
          result.update(i * N + j, list.apply(j))
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

  def buildFeature(sc: SparkContext, sqlContext: HiveContext, bizdateSubA: String, bizdateSubB: String, entity: String, logTypeList: String*): RDD[(String, String, Vector)] = {
    val featureList = new util.ArrayList[RDD[((String, String), Array[Double])]]()
    for (logType <- logTypeList) {
      featureList.add(getFeature(sc, sqlContext, bizdateSubA, bizdateSubB, entity, logType))
    }
    val feature = joinFeature(featureList)
    return feature
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

    val ratioCount = sample.map(x => (x._2.toDouble, 1d)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    val ratio = ratioCount._1 / ratioCount._2

    println(s"sample type:${sampleType}")
    println(s"total sample count:${ratioCount._2}")
    println(s"total positive sample count:${ratioCount._1}")
    println(s"positive negtive sample ratio:${ratio}")

    val posSample = sample.filter(x => x._2 > 0.5)
    val negSample = sample.filter(x => x._2 < 0.5).sample(false, ratio)

    val sampleFinal = posSample.union(negSample).map(x => Row(x._1.toString, x._2))

    val schema =
      StructType(
        StructField("feature", StringType, true)
          :: StructField("label", DoubleType, true)
          :: Nil)

    val sampleDF: DataFrame = sqlContext.createDataFrame(sampleFinal, schema)
    return sampleDF
  }
}
