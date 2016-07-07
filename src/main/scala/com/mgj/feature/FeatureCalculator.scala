package com.mgj.feature

import java.util

import com.mgj.utils.HiveUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 12/5/15.
  */
abstract class FeatureCalculator extends java.io.Serializable {

  var featureName: String
  var userField: String
  var userFieldPath: String
  var itemField: String
  var itemFieldPath: String
  var bizDate: String
  var maxValue: String
  var tableName: String

  def setUserFieldPath(userFieldPath: String): Unit = {
    this.userFieldPath = userFieldPath
  }

  def setFeatureName(featureName: String): Unit = {
    this.featureName = featureName
  }

  def setUserField(userField: String): Unit = {
    this.userField = userField
  }

  def setItemField(itemField: String): Unit = {
    this.itemField = itemField
  }

  def setItemFieldPath(itemFieldPath: String): Unit = {
    this.itemFieldPath = itemFieldPath
  }

  def setBizDate(bizDate: String): Unit = {
    this.bizDate = bizDate
  }

  def setMaxValue(maxValue: String): Unit = {
    this.maxValue = maxValue
  }

  def setTableName(tableName: String): Unit = {
    this.tableName = tableName
  }

  def compute(sampleDF: DataFrame, sc: SparkContext, sqlContext: HiveContext): DataFrame

  def getFeatureDF(sampleDF: DataFrame, sc: SparkContext, sqlContext: HiveContext): DataFrame

  def getFeatureRDD(sc: SparkContext, sqlContext: HiveContext): Seq[(RDD[(String, List[String])], List[String], String)] = {
    val table = this.getTable(tableName)
    println(s"table:${table}")
    val result = new util.ArrayList[(RDD[(String, List[String])], List[String], String)]()

    for (e <- table.keys) {
      val name = e
      val featureType = table.get(name).get
      val flag = HiveUtil.checkTable(sqlContext, name, bizDate)
      if (!flag) {
        featureType match {
          case FeatureType.ITEM => HiveUtil.featureHdfsToHive(sc, sqlContext, itemField, itemFieldPath, bizDate, name, FeatureType.ITEM)
          case FeatureType.USER => HiveUtil.featureHdfsToHive(sc, sqlContext, userField, userFieldPath, bizDate, name, FeatureType.USER)
        }
      }

      println(s"full table name:${HiveUtil.getFullTableName(name, bizDate)}")
      val featureDF = sqlContext.sql(s"select * from ${HiveUtil.getFullTableName(name, bizDate)}")
      featureDF.show()

      result.add(getFeature(featureDF, featureType))
    }

    return result.toList.toSeq
  }

  def getTables(): Map[String, String] = {
    return this.getTable()
  }

  protected def getFeature(featureDF: DataFrame, featureType: String): (RDD[(String, List[String])], List[String], String) = {
    val featureRDD = featureDF.map(x => {
      val keyIndex = featureType match {
        case FeatureType.ITEM => x.fieldIndex(FeatureConstant.ITEM_KEY)
        case FeatureType.USER => x.fieldIndex(FeatureConstant.USER_KEY)
      }
      val list = x.toSeq.map(x => x.toString).toList
      list.remove(keyIndex)
      (x.get(keyIndex).toString, list)
    })

    val featureSchema = featureDF.schema
      .filter(x => !x.equals(FeatureConstant.ITEM_KEY) && !x.equals(FeatureConstant.USER_KEY))
      .map(x => x.name).toList

    return (featureRDD, featureSchema, featureType)
  }

  protected def getTable(tableName: String = this.tableName): Map[String, String] = {
    return tableName.split(",").map(x => (x.split(":")(0), x.split(":")(1))).toMap
  }
}
