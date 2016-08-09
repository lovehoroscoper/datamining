package com.mgj.feature.impl.udfs

import com.mgj.feature.UdfTemplate
import org.apache.spark.sql.hive.HiveContext
import org.springframework.stereotype.Service

/**
  * Created by xiaonuo on 7/5/16.
  */
class SimpleUdf extends UdfTemplate {
  //  override def buildFunction(): Seq[String] => Double = {
  //    def udf(featureValue: String, scale: String): Double = {
  //      return featureValue.toDouble / scale.toDouble
  //    }
  //    val function = (input: Seq[String]) => udf(input(0), input(1))
  //    return function
  //  }

  override def register(sqlContext: HiveContext, name: String): Unit = {
    def udf(featureValue: String, scale: String): Double = {
      if (featureValue == null || featureValue.isEmpty || featureValue.equals("null")) {
        return 0d
      }
      return featureValue.toDouble / scale.toDouble
    }
    val function = (featureValue: String, scale: String) => udf(featureValue, scale)
    sqlContext.udf.register(name, function)
  }
}
