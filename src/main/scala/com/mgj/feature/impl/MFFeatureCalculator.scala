package com.mgj.feature.impl

import com.mgj.feature.{FeatureType, FeatureCalculator, FeatureConstant}
import com.mgj.utils.HiveUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 12/5/15.
  */
class MFFeatureCalculator extends FeatureCalculator {

  override def compute(sampleDF: DataFrame, sc: SparkContext, sqlContext: HiveContext): DataFrame = {
    def score(userFeature: String, itemFeature: String): Double = {
      if (userFeature != null && itemFeature != null) {
        val userArray = userFeature.split(",")
        val itemArray = itemFeature.split(",")

        var s = 0d
        val size = Math.min(userArray.size, itemArray.size)

        for (i <- 0 to size - 1) {
          s += userArray.apply(i).toDouble * itemArray.apply(i).toDouble / 10000d
        }
        return Math.max(0, s)
      }
      return 0d
    }

    println("build feature: " + featureName)
    println("userField: " + userField)
    println("itemField: " + itemField)

    println("sample columns: " + sampleDF.columns.map(x => x.toString).mkString(","))

    println("FeatureConstant.USER_KEY: " + FeatureConstant.USER_KEY)
    println("FeatureConstant.ITEM_KEY: " + FeatureConstant.ITEM_KEY)

    val featureNames = sampleDF.columns
      .filter(x => !x.equals(FeatureConstant.USER_KEY))
      .filter(x => !x.equals(FeatureConstant.ITEM_KEY))
      .filter(x => !x.equals(itemField))
      .filter(x => !x.equals(userField)).toList

    sampleDF.printSchema()
    println("featureNames: " + featureNames)

    val feature = sampleDF.map(x => {
      val featureVals: List[String] = featureNames.map(name => x.getAs[String](name))
      val featureVal: String = score(x.getAs[String](userField), x.getAs[String](itemField)).toString
      val uVal: String = x.getAs[String](FeatureConstant.USER_KEY)
      val iVal: String = x.getAs[String](FeatureConstant.ITEM_KEY)
      Row((uVal :: iVal :: featureVal :: featureVals): _*)
    })

    val valFields: List[StructField] = featureNames.map(name => StructField(name, StringType, true))
    val valField: StructField = StructField(featureName, StringType, true)
    val uField: StructField = StructField(FeatureConstant.USER_KEY, StringType, true)
    val iField: StructField = StructField(FeatureConstant.ITEM_KEY, StringType, true)
    val schema = StructType(uField :: iField :: valField :: valFields)

    val result = sqlContext.createDataFrame(feature, schema)

    println(this.featureName + " DataFrame")
    result.show

    return result
  }

  override def getFeatureDF(sampleDF: DataFrame, sc: SparkContext, sqlContext: HiveContext): DataFrame = {
    val userKeyAlias = FeatureConstant.USER_KEY + "_alias"
    val itemKeyAlias = FeatureConstant.ITEM_KEY + "_alias"
    val itemMFFeature = sc.textFile(itemFieldPath).map(x => Row(x.split(" ")(0).trim, x.split(" ")(1).trim))
    val schemaItemMFFeature = StructType(StructField(itemKeyAlias, StringType, true) :: StructField(itemField, StringType, true) :: Nil)
    val itemMFFeatureDF = sqlContext.createDataFrame(itemMFFeature, schemaItemMFFeature)
    println(itemField + " DataFrame")
    itemMFFeatureDF.show

    val userMFFeature = sc.textFile(userFieldPath).map(x => Row(x.split(" ")(0).trim, x.split(" ")(1).trim))
    val schemaUserMFFeature = StructType(StructField(userKeyAlias, StringType, true) :: StructField(userField, StringType, true) :: Nil)
    val userMFFeatureDF = sqlContext.createDataFrame(userMFFeature, schemaUserMFFeature)
    println(userField + " DataFrame")
    userMFFeatureDF.show

    var dataDF = sampleDF.join(userMFFeatureDF, sampleDF(FeatureConstant.USER_KEY) === userMFFeatureDF(userKeyAlias), "left_outer").drop(userKeyAlias).coalesce(500)
    dataDF = dataDF.join(itemMFFeatureDF, sampleDF(FeatureConstant.ITEM_KEY) === itemMFFeatureDF(itemKeyAlias), "left_outer").drop(itemKeyAlias).coalesce(500)
    println("sample after join")
    dataDF.show

    return dataDF
  }

  override var featureName: String = _
  override var userField: String = _
  override var itemField: String = _
  override var userFieldPath: String = _
  override var itemFieldPath: String = _
  override var bizDate: String = _
  override var maxValue: String = _
  override var tableName: String = _

  override def toString = s"MFFeatureCalculator($featureName, $userField, $itemField, $userFieldPath, $itemFieldPath, $bizDate, $maxValue, $tableName)"
}
