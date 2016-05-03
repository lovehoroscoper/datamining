package com.mgj.feature

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

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

  def compute(sampleDF: DataFrame, sc: SparkContext, sqlContext: HiveContext): DataFrame

  def getFeatureDF(sampleDF: DataFrame, sc: SparkContext, sqlContext: HiveContext): DataFrame
}
