package com.mgj.feature.impl

import com.mgj.feature.{FeatureConstant, FeatureCalculator}
import com.mgj.utils.HdfsUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructType, DoubleType, StructField}

/**
  * Created by xiaonuo on 12/5/15.
  */
class UserShopPreferFeatureCalculator extends FeatureCalculator {

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
      val featureVal: String = score(x.getAs[String](userField), Math.round(x.getAs[String](itemField).toDouble).toString).toString
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

    val userFeaturePath = HdfsUtil.getDirWithDate(sc, userFieldPath, bizDate)
    val userShopPreferBase = sc.textFile(userFeaturePath).map(x => Row(x.split(" ")(0).trim, x.split(" ")(1).trim))
    val schema = StructType(StructField(userKeyAlias, StringType, true) :: StructField(userField, StringType, true) :: Nil)
    val userShopPreferDF = sqlContext.createDataFrame(userShopPreferBase, schema)
    println(userField + " DataFrame")
    userShopPreferDF.show

    val itemShopFeatureDF = sqlContext.sql("select cast(tradeitemid as string) as " + itemKeyAlias + ", cast(shopid as string) as " + itemField + " from v_dw_trd_tradeitem")
    println(itemField + " DataFrame")
    itemShopFeatureDF.show()

    var dataDF = sampleDF.join(userShopPreferDF, sampleDF(FeatureConstant.USER_KEY) === userShopPreferDF(userKeyAlias), "left_outer").drop(userKeyAlias).coalesce(500)
    dataDF = dataDF.join(itemShopFeatureDF, sampleDF(FeatureConstant.ITEM_KEY) === itemShopFeatureDF(itemKeyAlias), "left_outer").drop(itemKeyAlias).coalesce(500)
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

  override def toString = s"UserShopPreferFeatureCalculator($featureName, $userField, $itemField, $userFieldPath, $itemFieldPath, $bizDate)"
}
