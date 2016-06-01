package com.mgj.feature.impl

import com.mgj.feature.{FeatureCalculator, FeatureConstant}
import com.mgj.utils.HdfsUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by xiaonuo on 12/5/15.
  */
class ItemFeatureCalculator extends FeatureCalculator {

  override def compute(sampleDF: DataFrame, sc: SparkContext, sqlContext: HiveContext): DataFrame = {
    println("build feature: " + this.featureName)
    println("userField: " + userField)
    println("itemField: " + itemField)
    println("FeatureConstant.USER_KEY: " + FeatureConstant.USER_KEY)
    println("FeatureConstant.ITEM_KEY: " + FeatureConstant.ITEM_KEY)

    val featureNames = sampleDF.columns
      .filter(x => !x.equals(FeatureConstant.USER_KEY))
      .filter(x => !x.equals(FeatureConstant.ITEM_KEY))
      .filter(x => !x.equals(itemField)).toList

    sampleDF.printSchema()
    println("featureNames: " + featureNames)

    val feature = sampleDF.map(x => {
      val featureVals: List[String] = featureNames.map(name => x.getAs[String](name))
      var featureVal: String = "0"
      val score = x.getAs[String](itemField)
      if (score != null && score != None) {
        featureVal = (score.toDouble / maxValue.toDouble).toString
      }
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

    println(featureName + " DataFrame")
    result.show

    return result
  }

  override def getFeatureDF(sampleDF: DataFrame, sc: SparkContext, sqlContext: HiveContext): DataFrame = {
    val alias = FeatureConstant.ITEM_KEY + "_alias"
    val pattern = """(\d+)[\t\s](\d+)""".r
    val itemFeaturePath = HdfsUtil.getDirWithDate(sc, itemFieldPath, bizDate)
    val itemFeature = sc.textFile(itemFeaturePath).map(x => {
      val pattern(id, score) = x.trim
      Row(id.trim, score.trim)
    })
    val schema = StructType(StructField(alias, StringType, true) :: StructField(itemField, StringType, true) :: Nil)
    val itemFeatureDF = sqlContext.createDataFrame(itemFeature, schema)
    println(itemField + " DataFrame")
    itemFeatureDF.show

    val dataDF = sampleDF.join(itemFeatureDF, sampleDF(FeatureConstant.ITEM_KEY) === itemFeatureDF(alias), "left_outer").drop(alias).coalesce(500)
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

  override def toString = s"ItemFeatureCalculator($const, $featureName, $userField, $itemField, $userFieldPath, $itemFieldPath, $bizDate, $maxValue)"
}
