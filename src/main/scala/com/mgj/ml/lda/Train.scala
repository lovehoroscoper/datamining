package com.mgj.ml.lda

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xiaonuo on 9/18/15.
 */
object Train {

  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf().
      setAppName("test").
      set("spark.sql.parquet.binaryAsString", "true");
    // Spark context.
    val sc: SparkContext = new SparkContext(conf);
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster");

    // User click log: user_id, item_id, visit_time.
    val userBaseLogApp = sqlContext.sql("select userid as user_id, tradeitemid as item_id, visit_time from user_click_log where visit_date >= '" + args(1) + "' and visit_date <= '" + args(2) + "'").repartition(100).rdd.filter(r => r.anyNull == false);


  }
}
