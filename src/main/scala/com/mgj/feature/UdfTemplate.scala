package com.mgj.feature

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by xiaonuo on 7/5/16.
  */
trait UdfTemplate extends Serializable {
  def register(sqlContext: HiveContext, name: String): Unit
}
