package com.mgj.feature

import java.util

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Created by xiaonuo on 7/5/16.
  */
object FeatureConstructor {

  val tableName = "wonderful_feature_table"

  def init(sqlContext: HiveContext, udfFactory: UdfFactory): Unit = {
    udfFactory.init(sqlContext)
  }

  def construct(sc: SparkContext, sqlContext: HiveContext, sampleDF: DataFrame, featureCalculatorFactory: FeatureCalculatorFactory, bizdate: String, features: String*): DataFrame = {

    val sampleSchema = sampleDF.schema.map(x => x.name).toList
    println(s"sampleSchema:${sampleSchema}")

    val sampleRDD = sampleDF.rdd.map(x => {
      val itemKeyIndex = x.fieldIndex(FeatureConstant.ITEM_KEY)
      val userKeyIndex = x.fieldIndex(FeatureConstant.USER_KEY)
      val list = x.toSeq.map(x => x.toString).toList
      list.add(x.get(userKeyIndex).toString)
      list.add(x.get(itemKeyIndex).toString)
      ((x.get(userKeyIndex).toString, x.get(itemKeyIndex).toString), list)
    })

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

    val rddSeqA = new util.ArrayList[RDD[(String, List[String])]]()
    val sampleRDDA = sampleRDD.map(x => (x._2.get(x._2.size - 2), x._2))
    rddSeqA.add(sampleRDDA)
    rddSeqA.addAll(userFeatureRDDList)
    val resultA = joiner(rddSeqA.toList.toSeq).filter(x => x._2(0).size > 0).map(x => {
      val featureList = new util.ArrayList[String]()
      val sampleField = x._2(0).toList.map(x => x.toString)
      val key = sampleField.get(sampleField.size - 1)
      featureList.addAll(sampleField.take(sampleField.size - 2).toList)

      for (i <- 0 to x._2.length - 2) {
        val list = if (x._2(i + 1).size > 0) {
          x._2(i + 1).map(x => x.toString).toList
        } else {
          val zeroList = new util.ArrayList[String]()
          for (k <- 1 to userFeatureSchemaList.get(i).size) {
            zeroList.add("0")
          }
          zeroList.toList
        }
        featureList.addAll(list)
      }
      (key, featureList.toList)
    })

    val rddSeqB = new util.ArrayList[RDD[(String, List[String])]]()
    rddSeqB.add(resultA)
    rddSeqB.addAll(itemFeatureRDDList)

    val featureRDD = joiner(rddSeqB.toList.toSeq).filter(x => x._2(0).size > 0).map(x => {
      val featureList = new util.ArrayList[String]()
      val sampleField = x._2(0).toList.map(x => x.toString)
      val key = sampleField.get(sampleField.size - 1)
      featureList.addAll(sampleField.take(sampleField.size - 2).toList)

      for (i <- 0 to x._2.length - 2) {
        val list = if (x._2(i + 1).size > 0) {
          x._2(i + 1).map(x => x.toString).toList
        } else {
          val zeroList = new util.ArrayList[String]()
          for (k <- 1 to itemFeatureSchemaList.get(i).size) {
            zeroList.add("0")
          }
          zeroList.toList
        }
        featureList.addAll(list)
      }
      featureList.toList
    })

    for (e <- userFeatureSchemaList) {
      sampleSchema.addAll(e)
    }
    for (e <- itemFeatureSchemaList) {
      sampleSchema.addAll(e)
    }

    val structField: List[StructField] = sampleSchema.map(name => StructField(name, StringType, true))
    val schema = StructType(structField)

    val rawFeatureDF = sqlContext.createDataFrame(featureRDD.map(x => Row(x)), schema)

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
      val sql = s"${
        computer
      } as feature"
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
