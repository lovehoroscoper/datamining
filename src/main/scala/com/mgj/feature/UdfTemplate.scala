package com.mgj.feature

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by xiaonuo on 7/5/16.
  */
abstract class UdfTemplate extends Serializable {
  //  def buildFunction(): Seq[String] => Double

  def register(sqlContext: HiveContext, name: String): Unit = {
    //    sqlContext.udf.register(name, this.buildFunction)
    //    sqlContext.udf.register(name, this.buildFunction)
  }
}
