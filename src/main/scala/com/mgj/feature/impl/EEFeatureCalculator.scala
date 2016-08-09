package com.mgj.feature.impl

import java.util

import com.mgj.feature.{FeatureType, FeatureCalculator, FeatureConstant}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 12/5/15.
  */
class EEFeatureCalculator extends FeatureCalculator {

  override var featureName: String = _
  override var userField: String = _
  override var itemField: String = _
  override var userFieldPath: String = _
  override var itemFieldPath: String = _
  override var bizDate: String = _
  override var tableName: String = _

  override def toString = s"EEFeatureCalculator($featureName, $userField, $itemField, $userFieldPath, $itemFieldPath, $bizDate, $tableName)"

  override def compute(sampleDF: DataFrame, sc: SparkContext, sqlContext: HiveContext): DataFrame = ???

  override def getFeatureDF(sampleDF: DataFrame, sc: SparkContext, sqlContext: HiveContext): DataFrame = ???

  override def getFeatureRDD(sc: SparkContext, sqlContext: HiveContext): Seq[(RDD[(String, List[String])], List[String], String)] = {
    val itemEEFeatureDF = sqlContext.sql(s"select cast(tid as string) as ${FeatureConstant.ITEM_KEY}, '0.2' as ${itemField} from online_ee_daily_selection where dt = '${bizDate}'status = 2 and tid is not null")
    val result = new util.ArrayList[(RDD[(String, List[String])], List[String], String)]()
    result.add(getFeature(sqlContext, itemEEFeatureDF, FeatureType.ITEM))
    return result.toList.toSeq
  }
}
