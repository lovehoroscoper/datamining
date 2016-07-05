package com.mgj.feature

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 7/5/16.
  */
class FeatureConstructor {

  def init(): Unit = {

  }

  def construct(sc: SparkContext, sqlContext: HiveContext, sampleDF: DataFrame, features: _*): Unit = {

    val sampleSchema = sampleDF.schema.map(x => x.name).toList

    val sampleItemRDD = sampleDF.map(x => {
      val itemKeyIndex = x.fieldIndex(FeatureConstant.ITEM_KEY)
      val userKeyIndex = x.fieldIndex(FeatureConstant.USER_KEY)
      val list = x.toSeq.map(x => x.toString).toList
      (x.get(itemKeyIndex).toString, list)
    })

    val sampleItemRDD = sampleDF.map(x =>)
  }

  private def buildSql(): Unit = {

  }
}
