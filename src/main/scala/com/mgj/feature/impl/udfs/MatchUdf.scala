package com.mgj.feature.impl.udfs

import com.mgj.feature.UdfTemplate
import org.apache.spark.sql.hive.HiveContext
import org.springframework.stereotype.Service

/**
  * Created by xiaonuo on 7/5/16.
  */
class MatchUdf extends UdfTemplate {

  override def register(sqlContext: HiveContext, name: String): Unit = {
    def udf(userFeature: String, itemFeature: String): Double = {
      if (userFeature != null && itemFeature != null) {
        userFeature.split(",").foreach(x => {
          val kv = x.split(":")
          if (kv(0).equals(itemFeature)) {
            if (kv.size < 2) {
              throw new Exception(s"feature format error, userFeature ${userFeature}, itemFeature ${itemFeature}")
            }
            return kv(1).toDouble / 100000d
          }
        })
      }
      return 0d
    }
    val function = (userFeature: String, itemFeature: String) => udf(userFeature, itemFeature)
    sqlContext.udf.register(name, function)
  }

  //  override def buildFunction(): Seq[String] => Double = {
  //    def udf(userFeature: String, itemFeature: String, scale: String): Double = {
  //      if (userFeature != null && itemFeature != null) {
  //        userFeature.split(",").foreach(x => {
  //          val kv = x.split(":")
  //          if (kv(0).equals(itemFeature)) {
  //            return kv(1).toDouble / scale.toDouble
  //          }
  //        })
  //      }
  //      return 0d
  //    }
  //    val function = (input: Seq[String]) => udf(input(0), input(1), input(2))
  //    return function
  //  }
}
