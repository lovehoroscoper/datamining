package com.mgj.feature.impl

import com.mgj.feature.{FeatureType, FeatureCalculator, FeatureConstant}
import com.mgj.utils.{HiveUtil, HdfsUtil}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import scala.collection.JavaConversions._
/**
  * Created by xiaonuo on 12/5/15.
  */
class UserGenePreferFeatureCalculator extends FeatureCalculator {

  override def compute(sampleDF: DataFrame, sc: SparkContext, sqlContext: HiveContext): DataFrame = {
    def score(userFeature: String, itemFeature: String): Double = {
      if (userFeature != null && itemFeature != null) {
        userFeature.split(",").foreach(x => {
          val kv = x.split(":")
          if (kv(0).equals(itemFeature)) {
            return kv(1).toDouble / 100000d
          }
        })
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
    val itemFeaturePath = HdfsUtil.getDirWithDate(sc, itemFieldPath, bizDate)
    val itemGeneFeature = sc.textFile(itemFeaturePath).map(x => Row(x.split(" ")(0).trim, x.split(" ")(1).trim))
    val schemaItemGene = StructType(StructField(itemKeyAlias, StringType, true) :: StructField(itemField, StringType, true) :: Nil)
    val itemGeneFeatureDF = sqlContext.createDataFrame(itemGeneFeature, schemaItemGene)
    println(itemField + " DataFrame")
    itemGeneFeatureDF.show

    val userFeaturePath = HdfsUtil.getDirWithDate(sc, userFieldPath, bizDate)
    val userGenePreferBase = sc.textFile(userFeaturePath).map(x => Row(x.split(" ")(0).trim, x.split(" ")(1).trim))
    val schemaUserGenePrefer = StructType(StructField(userKeyAlias, StringType, true) :: StructField(userField, StringType, true) :: Nil)
    val userGenePreferDF = sqlContext.createDataFrame(userGenePreferBase, schemaUserGenePrefer)
    println(userField + " DataFrame")
    userGenePreferDF.show

    var dataDF = sampleDF.join(userGenePreferDF, sampleDF(FeatureConstant.USER_KEY) === userGenePreferDF(userKeyAlias), "left_outer").drop(userKeyAlias).coalesce(500)
    dataDF = dataDF.join(itemGeneFeatureDF, sampleDF(FeatureConstant.ITEM_KEY) === itemGeneFeatureDF(itemKeyAlias), "left_outer").drop(itemKeyAlias).coalesce(500)
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

  override def toString = s"UserGenePreferFeatureCalculator($featureName, $userField, $itemField, $userFieldPath, $itemFieldPath, $bizDate, $maxValue, $tableName)"
}
