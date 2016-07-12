package com.mgj.feature.impl.udfs

import java.text.SimpleDateFormat

import com.mgj.feature.UdfTemplate
import org.apache.spark.sql.hive.HiveContext
import org.springframework.stereotype.Service

/**
  * Created by xiaonuo on 7/5/16.
  */
@Service("realSimUdf")
class RealSimUdf extends UdfTemplate {

  override def buildFunction(): Seq[String] => Double = {
    val N = 50
    def udf(userFeature: String, itemFeature: String, time: String): Double = {
      if (userFeature == null
        || itemFeature == null
        || time == null
        || userFeature.equals("null")
        || itemFeature.equals("null")
        || time.equals("null")) {
        return 0d
      }
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val userClickList = userFeature
        .split(",")
        .map(x => x.split("#", 2))
        .filter(x => x.size == 2).map(x => (x(0), x(1))).filter(x => {
        sdf.parse(x._2).getTime < sdf.parse(time).getTime
      })
      var timeDiff = Long.MaxValue
      var score = 0d
      for (x <- userClickList.toIterable) {
        val list = x._1.split(",").map(x => x.split(":")(0)).take(N).toList
        val t = list.indexOf(itemFeature) + 1
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

    val function = (input: Seq[String]) => udf(input(0), input(1), input(2))
    return function
  }

  //  override def register(sqlContext: HiveContext, name: String): Unit = {
  //    val N = 50
  //    def udf(userFeature: String, itemFeature: String, time: String): Double = {
  //      if (userFeature == null
  //        || itemFeature == null
  //        || time == null
  //        || userFeature.equals("null")
  //        || itemFeature.equals("null")
  //        || time.equals("null")) {
  //        return 0d
  //      }
  //      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //      val userClickList = userFeature
  //        .split(",")
  //        .map(x => x.split("#", 2))
  //        .filter(x => x.size == 2).map(x => (x(0), x(1))).filter(x => {
  //        sdf.parse(x._2).getTime < sdf.parse(time).getTime
  //      })
  //      var timeDiff = Long.MaxValue
  //      var score = 0d
  //      for (x <- userClickList.toIterable) {
  //        val list = x._1.split(",").map(x => x.split(":")(0)).take(N).toList
  //        val t = list.indexOf(itemFeature) + 1
  //        if (t > 0) {
  //          timeDiff = sdf.parse(time).getTime - sdf.parse(x._2).getTime
  //          val temp = 1.0 / t * 1.0 / (1 + Math.log(1.0 * timeDiff / 1000 / 60 / 10 + 1))
  //          if (temp > score) {
  //            score = temp
  //          }
  //        }
  //      }
  //      return score
  //    }
  //
  //    val function = (userFeature: String, itemFeature: String, time: String) => udf(userFeature, itemFeature, time)
  //
  //    sqlContext.udf.register(name, function)
  //  }
}
