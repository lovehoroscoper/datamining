package com.mgj.utils

import com.mgj.feature.{FeatureType, FeatureConstant}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by xiaonuo on 6/27/16.
  */
object HiveUtil {

  def featureHdfsToHive(sc: SparkContext, sqlContext: HiveContext, featureName: String, filePath: String, tableName: String, featureType: String): DataFrame = {
    return featureHdfsToHive(sc, sqlContext, featureName, filePath, "", tableName)
  }

  def featureHdfsToHive(sc: SparkContext, sqlContext: HiveContext, featureName: String, filePath: String, bizdate: String, tableName: String, featureType: String, splitter: String = " "): DataFrame = {
    val hdfsPath = HdfsUtil.getDirWithDate(sc, filePath, bizdate)
    val featureBase = sc.textFile(hdfsPath).map(x => Row(x.split(splitter)(0).trim, x.split(splitter)(1).trim))
    val schema = featureType match {
      case FeatureType.USER => StructType(StructField(FeatureConstant.USER_KEY, StringType, true) :: StructField(featureName, StringType, true) :: Nil)
      case FeatureType.ITEM => StructType(StructField(FeatureConstant.ITEM_KEY, StringType, true) :: StructField(featureName, StringType, true) :: Nil)
      case _ => StructType(StructField(FeatureConstant.ITEM_KEY, StringType, true) :: StructField(featureName, StringType, true) :: Nil)
    }
    val featureDF = sqlContext.createDataFrame(featureBase, schema)
    featureDF.show
    val bizdateNew = getDate(bizdate)
    val fullTableName = s"${tableName}_${bizdateNew}"
    println(s"full table name:${fullTableName}")
    sqlContext.sql(s"drop table if exists ${fullTableName}")
    featureDF.write.saveAsTable(fullTableName)
    return featureDF
  }

  def checkTable(sqlContext: HiveContext, tableName: String): Boolean = {
    val tableSet = sqlContext.sql("show tables").select("tableName").map(x => x(0).toString).collect().toSet
    return tableSet.contains(tableName)
  }

  def checkTable(sqlContext: HiveContext, tableName: String, bizdate: String): Boolean = {
    val fullTableName = tableName + getDate(bizdate)
    val tableSet = sqlContext.sql("show tables").select("tableName").map(x => x(0).toString).collect().toSet
    return tableSet.contains(fullTableName)
  }

  def getDate(date: String): String = {
    val pattern = """(\d{4}).*(\d{2}).*(\d{2})""".r
    val dateMeta = pattern.findFirstIn(date)
    if (dateMeta != None) {
      val pattern(year, month, day) = dateMeta.get
      return year + month + day
    } else {
      return ""
    }
  }

}
