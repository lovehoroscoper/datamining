package com.mgj.feature

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by xiaonuo on 7/5/16.
  */
abstract class UdfTemplate {
  var name: String

  def buildFunction(): Function

  def register(sqlContext: HiveContext): Unit = {
    sqlContext.udf.register(this.name, buildFunction())
  }
}
