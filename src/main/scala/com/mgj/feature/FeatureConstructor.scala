package com.mgj.feature

import java.util

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.apache.spark.sql.{Dataset, Row, DataFrame}
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

    val itemFeatureDF: DataFrame = if (itemFlag) getRawFeatureDF(sqlContext, itemFeatureRDDList, itemFeatureSchemaList, itemKeyAlias).cache() else null
    for (e <- itemFeatureRDDList) {
      e.unpersist(blocking = false)
    }
    val userFeatureDF: DataFrame = if (userFlag) getRawFeatureDF(sqlContext, userFeatureRDDList, userFeatureSchemaList, userKeyAlias).cache() else null
    for (e <- userFeatureRDDList) {
      e.unpersist(blocking = false)
    }

    var rawFeatureDF = sampleDF
    if (userFlag) {
      rawFeatureDF = rawFeatureDF.join(userFeatureDF, sampleDF(FeatureConstant.USER_KEY) === userFeatureDF(userKeyAlias), "left_outer").drop(userKeyAlias).coalesce(1000).cache()
    }
    if (userFeatureDF != null) {
      userFeatureDF.unpersist(blocking = false)
    }
    if (itemFlag) {
      rawFeatureDF = rawFeatureDF.join(itemFeatureDF, sampleDF(FeatureConstant.ITEM_KEY) === itemFeatureDF(itemKeyAlias), "left_outer").drop(itemKeyAlias).coalesce(1000).cache()
    }
    if (itemFeatureDF != null) {
      itemFeatureDF.unpersist(blocking = false)
    }
    rawFeatureDF.registerTempTable(tableName)
    rawFeatureDF.show

    val sql = buildSql(featureCalculatorFactory, features: _*)
    println(s"sql:select ${sql} from ${tableName}")
    val featureDF = sqlContext.sql(s"select ${sql} from ${tableName}").cache()
    rawFeatureDF.unpersist(blocking = false)
    featureDF.show()
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

  private def dropDuplicate(featureSchemaList: List[List[String]], featureRDDList: List[RDD[(String, List[String])]]): (List[List[String]], List[RDD[(String, List[String])]]) = {
    val featureSchemaListDropDuplicate = new util.ArrayList[List[String]]()
    val featureRDDListDropDuplicate = new util.ArrayList[RDD[(String, List[String])]]()

    val schemaSet = new util.HashSet[String]()

    for (i <- 0 to featureSchemaList.size - 1) {
      val list = new util.ArrayList[String]()
      val indexSet = new util.HashSet[Int]()

      for (j <- 0 to featureSchemaList.get(i).size - 1) {
        val schema = featureSchemaList.get(i).get(j)
        if (!schemaSet.contains(schema)) {
          schemaSet.add(schema)
          list.add(schema)
          indexSet.add(j)
        }
      }

      if (list.size > 0) {
        val rdd = if (indexSet.size == featureSchemaList.get(i).size) featureRDDList.get(i)
        else featureRDDList.get(i).map(x => {
          val list = new util.ArrayList[String]()
          for (i <- 0 to x._2.size - 1) {
            if (indexSet.contains(i)) {
              list.add(x._2.get(i))
            }
          }
          (x._1, list.toList)
        })
        featureSchemaListDropDuplicate.add(list.toList)
        featureRDDListDropDuplicate.add(rdd)
      }
    }
    (featureSchemaListDropDuplicate.toList, featureRDDListDropDuplicate.toList)
  }

  private def getRawFeatureDF(sqlContext: HiveContext, featureRDDList: List[RDD[(String, List[String])]], featureSchemaList: List[List[String]], keySchema: String): DataFrame = {
    val (featureSchemaListDropDuplicate, featureRDDListDropDuplicate) = dropDuplicate(featureSchemaList, featureRDDList)
    for (e <- featureRDDList) {
      e.unpersist(blocking = false)
    }
    val featureRDD = joiner(featureRDDListDropDuplicate.toSeq).filter(x => x._2(0).size > 0).map(x => {
      val featureList = new util.ArrayList[String]()
      featureList.add(x._1)

      for (i <- 0 to x._2.length - 1) {
        val list = if (x._2(i).size > 0) {
          x._2(i).toList.get(0).asInstanceOf[List[String]]
        } else {
          val zeroList = new util.ArrayList[String]()
          for (k <- 1 to featureSchemaListDropDuplicate.get(i).size) {
            zeroList.add("0")
          }
          zeroList.toList
        }
        featureList.addAll(list)
      }
      featureList.map(x => x.toString).toList
    })

    val schemaList = new util.ArrayList[String]()
    schemaList.add(keySchema)
    for (e <- featureSchemaListDropDuplicate) {
      schemaList.addAll(e)
    }

    val structField: List[StructField] = schemaList.toList.map(name => StructField(name, StringType, true))
    println(s"schema:${StructType(structField)}")
    val featureDF = sqlContext.createDataFrame(featureRDD.map(x => Row(x.toSeq: _*)), StructType(structField))
    return featureDF
  }
}