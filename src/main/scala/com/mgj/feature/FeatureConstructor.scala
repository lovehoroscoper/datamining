package com.mgj.feature

import java.util

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Created by xiaonuo on 7/5/16.
  */
object FeatureConstructor {

  val tableName = "wonderful_feature_table"

  val userKeyAlias = FeatureConstant.USER_KEY + "_alias"
  val itemKeyAlias = FeatureConstant.ITEM_KEY + "_alias"

  def init(sqlContext: HiveContext, udfFactory: UdfFactory): Unit = {
    udfFactory.init(sqlContext)
  }

  def construct(sc: SparkContext, sqlContext: HiveContext, sampleDF: DataFrame, featureCalculatorFactory: FeatureCalculatorFactory, bizdate: String, features: String*): DataFrame = {

    val result = new util.ArrayList[(RDD[(String, List[String])], List[String], String)]()

    val tableSet = new util.HashSet[String]()
    for (feature <- features.toList) {
      if (featureCalculatorFactory.containsCalculator(feature)) {
        val calculator = featureCalculatorFactory.getCalculator(feature)
        calculator.setBizDate(bizdate)
        println(calculator)
        val tableMap = calculator.getTables().filter(x => !tableSet.contains(x._1))
        println(s"tableMap:${tableMap}")
        println(s"tableSet:${tableSet}")
        if (tableMap.size > 0) {
          val tableName = tableMap.map(x => s"${x._1}:${x._2}").mkString(",")
          calculator.setTableName(tableName)
          tableSet.addAll(tableMap.keySet)
          val featureRDD = calculator.getFeatureRDD(sc, sqlContext)
          featureRDD.take(10).foreach(println)
          result.addAll(featureRDD)
        }
      } else {
        println(s"feature calculator ${feature} dose not exists")
      }
    }

    val itemFeatureRDDList = result.toList.filter(x => x._3.equals(FeatureType.ITEM)).map(x => x._1)
    val itemFeatureSchemaList = result.toList.filter(x => x._3.equals(FeatureType.ITEM)).map(x => x._2)
    val userFeatureRDDList = result.toList.filter(x => x._3.equals(FeatureType.USER)).map(x => x._1)
    val userFeatureSchemaList = result.toList.filter(x => x._3.equals(FeatureType.USER)).map(x => x._2)

    val userFlag = userFeatureRDDList.size > 0
    val itemFlag = itemFeatureRDDList.size > 0

    println("itemFeatureRDDList")
    for (e <- itemFeatureRDDList.take(10)) {
      e.take(10).map(x => x._1 + ":" + x._2.mkString(",")).foreach(println)
    }
    println("userFeatureRDDList")
    for (e <- userFeatureRDDList.take(10)) {
      e.take(10).map(x => x._1 + ":" + x._2.mkString(",")).foreach(println)
    }

    var userFeatureDF: DataFrame = null
    if (userFlag) {
      val userFeatureRDD = joiner(userFeatureRDDList.toSeq).map(x => {
        val featureList = new util.ArrayList[String]()
        featureList.add(x._1)

        for (i <- 0 to x._2.length - 1) {
          val list = if (x._2(i).size > 0) {
            x._2(i).toList.get(0).asInstanceOf[List[String]]
          } else {
            val zeroList = new util.ArrayList[String]()
            for (k <- 1 to userFeatureSchemaList.get(i).size) {
              zeroList.add("0")
            }
            zeroList.toList
          }
          featureList.addAll(list)
        }
        featureList.toList
      })

      val userSchemaList = new util.ArrayList[String]()
      userSchemaList.add(userKeyAlias)
      for (e <- userFeatureSchemaList) {
        userSchemaList.addAll(e)
      }

      val userStructField: List[StructField] = userSchemaList.toList.map(name => StructField(name, StringType, true))
      println(s"schema:${StructType(userStructField)}")
      userFeatureDF = sqlContext.createDataFrame(userFeatureRDD.map(x => Row(x)), StructType(userStructField))
      println("userFeatureDF")
      userFeatureDF.show
    }

    var itemFeatureDF: DataFrame = null

    if (itemFlag) {
      val itemFeatureRDD = joiner(itemFeatureRDDList.toList.toSeq).filter(x => x._2(0).size > 0).map(x => {
        val featureList = new util.ArrayList[String]()
        featureList.add(x._1)

        for (i <- 0 to x._2.length - 1) {
          val list = if (x._2(i).size > 0) {
            x._2(i).toList.get(0).asInstanceOf[List[String]]
          } else {
            val zeroList = new util.ArrayList[String]()
            for (k <- 1 to itemFeatureSchemaList.get(i).size) {
              zeroList.add("0")
            }
            zeroList.toList
          }
          featureList.addAll(list)
        }
        featureList.map(x => x.toString).toList
      })

      val itemSchemaList = new util.ArrayList[String]()
      itemSchemaList.add(itemKeyAlias)
      for (e <- itemFeatureSchemaList) {
        itemSchemaList.addAll(e)
      }

      itemFeatureRDD.filter(x => x.size != itemSchemaList.size).take(10).foreach(println)
      itemFeatureRDD.take(10).foreach(println)
      println(s"count:${itemFeatureRDD.count()}")

      val itemStructField: List[StructField] = itemSchemaList.toList.map(name => StructField(name, StringType, true))
      println(s"schema:${StructType(itemStructField)}")
      itemFeatureDF = sqlContext.createDataFrame(itemFeatureRDD.map(x => Row(x)), StructType(itemStructField))
      println("itemFeatureDF")
      itemFeatureDF.show
    }

    var rawFeatureDF = sampleDF
    if (userFlag) {
      rawFeatureDF = rawFeatureDF.join(userFeatureDF, sampleDF(FeatureConstant.USER_KEY) === userFeatureDF(userKeyAlias), "left_outer").drop(userKeyAlias).coalesce(500)
    }
    if (itemFlag) {
      rawFeatureDF = rawFeatureDF.join(itemFeatureDF, sampleDF(FeatureConstant.ITEM_KEY) === itemFeatureDF(itemKeyAlias), "left_outer").drop(itemKeyAlias).coalesce(500)
    }
    rawFeatureDF.registerTempTable(tableName)
    rawFeatureDF.show

    val sql = buildSql(featureCalculatorFactory, features: _*)
    println(s"sql:${sql}")
    val featureDF = sqlContext.sql(s"select ${sql} from ${tableName}")
    return featureDF
  }

  private def buildSql(featureCalculatorFactory: FeatureCalculatorFactory, features: String*): String = {
    val list = new util.ArrayList[String]()

    for (feature <- features) {
      val computer = featureCalculatorFactory.getComputer(feature)
      val sql = s"${computer} as ${feature}"
      list.add(sql)
    }
    return list.mkString(", ")
  }

  private def joiner[K: ClassTag, V](seq: Seq[RDD[_ <: Product2[K, _]]]): CoGroupedRDD[K] = {
    val partitioner: org.apache.spark.Partitioner = new HashPartitioner(100)
    val cg = new CoGroupedRDD[K](seq, partitioner)
    return cg
  }
}