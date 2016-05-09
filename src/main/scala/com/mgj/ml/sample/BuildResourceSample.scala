package com.mgj.ml.sample

import com.mgj.utils.{SampleV2Util, SampleUtil}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 11/18/15.
  */
object BuildResourceSample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("build resource sample").
      set("spark.sql.parquet.binaryAsString", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val appIds = args(0).split(",")
    val table = args(1)
    println(s"appIds:${appIds}")
    println(s"table:${table}")

    // user_id, entity_id, expose_time, click_time, pos, label.
    val clickSampleDF = SampleV2Util.getClickSample(sqlContext, appIds: _*)
    clickSampleDF.show()

    clickSampleDF.registerTempTable(table + "_temp")

    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    sqlContext.sql("drop table if exists " + table)
    sqlContext.sql("create table " + table + " as select * from " + table + "_temp")
  }
}
