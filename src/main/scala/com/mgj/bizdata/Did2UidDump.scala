package com.mgj.bizdata

import com.mgj.utils.PartitionUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xiaonuo on 3/7/16.
  */
object Did2UidDump {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("delete item cf score")
      .set("spark.sql.parquet.binaryAsString", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val outputPath = args(0)

    val maxPt = PartitionUtil.getMaxPt(sqlContext, "s_dg_did2uid")
    val did2uid = sqlContext.sql(s"select device_id, user_id from s_dg_did2uid where pt = '${maxPt}'")
      .rdd.filter(x => x.anyNull == false && x(1).toString.toLong > 0).map(x => (x(0).toString, x(1).toString))

    println("user count:" + did2uid.count)
    did2uid.map(x => x._1 + " " + x._2).saveAsTextFile(outputPath)
  }
}
