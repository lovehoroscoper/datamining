package com.mgj.ml.rank

import com.mgj.utils.SampleV2Util
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

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
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    val appIds = args(0).split(",")
    val table = args(1)
    val bizdate = args(2)
    println(s"appIds:${appIds}")
    println(s"table:${table}")
    println(s"bizdate:${bizdate}")

    // user_id, entity_id, expose_time, click_time, pos, label.
    val clickSampleDF = SampleV2Util.getClickSample(sqlContext, bizdate, true, appIds: _*)
    clickSampleDF.show()

    //    clickSampleDF.registerTempTable(table + "_temp")
    //    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    //    sqlContext.sql("drop table if exists " + table)
    //    sqlContext.sql("create table " + table + " as select * from " + table + "_temp")
    sqlContext.sql("drop table if exists " + table)
    clickSampleDF.write.saveAsTable(table)
  }
}
