package com.mgj.feature.impl

import java.text.SimpleDateFormat
import java.util

import com.mgj.feature.{FeatureType, FeatureCalculator, FeatureConstant}
import com.mgj.utils.HiveUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 12/5/15.
  */
class UserRealItemPreferFeatureCalculator extends FeatureCalculator {
  val N = 25

  override def compute(sampleDF: DataFrame, sc: SparkContext, sqlContext: HiveContext): DataFrame = {

    def score(userFeature: String, itemId: String, time: String, itemSim: Map[String, String]): Double = {
      if (userFeature == null || time == null || itemId == null) {
        return 0d
      }
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val userClickList = userFeature.split(",").map(x => (x.split(":", 2)(0), x.split(":", 2)(1))).filter(x => {
        sdf.parse(x._2).getTime < sdf.parse(time).getTime
      })
      val itemSet = userClickList.filter(x => itemSim.contains(x._1)).map(x => (itemSim.get(x._1).get, x._2)).toIterable
      var timeDiff = Long.MaxValue
      var score = 0d
      for (x <- itemSet) {
        val list = x._1.split(",").map(x => x.split(":")(0)).take(N).toList
        val t = list.indexOf(itemId) + 1
        if (t > 0) {
          timeDiff = sdf.parse(time).getTime - sdf.parse(x._2).getTime
          val temp = 1.0 / t * 1.0 / (1 + Math.log(1.0 * timeDiff / 1000 / 60 / 10 + 1))
          if (temp > score) {
            score = temp
          }
        }
      }
      return score
    }

    println("build feature: " + featureName)
    println("userField: " + userField)
    println("itemField: " + itemField)

    println("FeatureConstant.USER_KEY: " + FeatureConstant.USER_KEY)
    println("FeatureConstant.ITEM_KEY: " + FeatureConstant.ITEM_KEY)

    val featureNames = sampleDF.columns
      .filter(x => !x.equals(FeatureConstant.USER_KEY))
      .filter(x => !x.equals(FeatureConstant.ITEM_KEY))
      .filter(x => !x.equals(userField)).toList

    sampleDF.printSchema()
    println("featureNames: " + featureNames)

    val itemSim = sc.textFile(itemFieldPath).map(x => (x.split(" ")(0), x.split(" ")(1).split(",").take(N).mkString(","))).collect().toMap
    println("item sim:")
    itemSim.take(10).foreach(println)
    val feature = sampleDF.map(x => {
      val featureVals: List[String] = featureNames.map(name => x.getAs[String](name))
      val featureVal: String = score(x.getAs[String](userField), x.getAs[String](FeatureConstant.ITEM_KEY), x.getAs[String]("time"), itemSim).toString
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
    val alias = FeatureConstant.USER_KEY + "_alias"
    val clickSql = "select user_id, item_id, time from s_dg_user_base_log where pt = '" + bizDate + "' and action_type = 'click' and platform_type = 'app'"
    val userClickItem = sqlContext.sql(clickSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString)).map(x => (x._1, x)).groupByKey().map(x => {
      val clickList = x._2.map(x => (x._2 + ":" + x._3)).mkString(",")
      Row(x._1, clickList)
    })
    val schemaUser = StructType(StructField(alias, StringType, true) :: StructField(userField, StringType, true) :: Nil)
    val userClickItemDF = sqlContext.createDataFrame(userClickItem, schemaUser)
    println(userField + " DataFrame")
    userClickItemDF.show

    val dataDF = sampleDF.join(userClickItemDF, sampleDF(FeatureConstant.USER_KEY) === userClickItemDF(alias), "left_outer").drop(alias).coalesce(500)
    println("sample after join")
    dataDF.show

    return dataDF
  }

  override def getFeatureRDD(sc: SparkContext, sqlContext: HiveContext): Seq[(RDD[(String, List[String])], List[String], String)] = {
    val itemSim = sc.textFile(itemFieldPath).map(x => (x.split(" ")(0), x.split(" ")(1).split(",").take(N).mkString(","))).collect().toMap

    val clickSql = s"select user_id, item_id, time from s_dg_user_base_log where pt = '${bizDate}' and action_type = 'click' and platform_type = 'app'"
    val userClickItem = sqlContext.sql(clickSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString)).map(x => (x._1, x)).groupByKey().map(x => {
      val clickList = x._2.filter(t => itemSim.contains(t._2)).map(t => (itemSim.get(t._2).get + "#" + t._3)).mkString(";")
      Row(x._1, clickList)
    })
    val schemaUser = StructType(StructField(FeatureConstant.USER_KEY, StringType, true) :: StructField(userField, StringType, true) :: Nil)
    val userClickItemDF = sqlContext.createDataFrame(userClickItem, schemaUser)
    println(userField + " DataFrame")
    userClickItemDF.show

    val result = new util.ArrayList[(RDD[(String, List[String])], List[String], String)]()
    result.add(getFeature(sqlContext, userClickItemDF, FeatureType.USER))
    return result.toList.toSeq
  }

  override var featureName: String = _
  override var userField: String = _
  override var itemField: String = _
  override var userFieldPath: String = _
  override var itemFieldPath: String = _
  override var bizDate: String = _
  override var tableName: String = _

  override def toString = s"UserRealItemPreferFeatureCalculator($featureName, $userField, $itemField, $userFieldPath, $itemFieldPath, $bizDate, $tableName)"
}
