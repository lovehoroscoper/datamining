package com.mgj.feature.impl.udfs

import com.mgj.feature.UdfTemplate
import org.apache.spark.sql.hive.HiveContext
import org.springframework.stereotype.Service

/**
  * Created by xiaonuo on 7/5/16.
  */
@Service("matchUdf")
class MatchUdf extends UdfTemplate {
  //  override def buildFunction(): (String, String) => Double = {
  //    val function = (userFeature: String, itemFeature: String) => {
  //      if (userFeature != null && itemFeature != null) {
  //        userFeature.split(",").foreach(x => {
  //          val kv = x.split(":")
  //          if (kv(0).equals(itemFeature)) {
  //            kv(1).toDouble / 100000d
  //          }
  //        })
  //      }
  //      0d
  //    }
  //    return function
  //  }

  override def register(sqlContext: HiveContext, name: String): Unit = {
    def udf(userFeature: String, itemFeature: String): Double = {
      if (userFeature != null && itemFeature != null) {
        userFeature.split(",").foreach(x => {
          val kv = x.split(":")
          if (kv(0).equals(itemFeature)) {
            return kv(1).toDouble / 100000d
          }
        })
      }
      return 0d
    }
    val function = (userFeature: String, itemFeature: String) => udf(userFeature, itemFeature)
    sqlContext.udf.register(name, function)
  }

}
