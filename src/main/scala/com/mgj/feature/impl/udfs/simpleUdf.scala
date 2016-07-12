package com.mgj.feature.impl.udfs

import com.mgj.feature.UdfTemplate
import org.apache.spark.sql.hive.HiveContext
import org.springframework.stereotype.Service

/**
  * Created by xiaonuo on 7/5/16.
  */
@Service("simpleUdf")
class SimpleUdf extends UdfTemplate {
  override def buildFunction(): Seq[String] => Double = {
    val function = (input: Seq[String]) => input(0).toDouble
    return function
  }

  //  override def register(sqlContext: HiveContext, name: String): Unit = {
  //    val function = (input: String) => input.toDouble
  //    sqlContext.udf.register(name, this.buildFunction)
  //  }
}
