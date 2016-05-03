package com.mgj.useritemprefer.evaluate

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by xiaonuo on 9/6/15.
 */
object Evaluate {
  def main(args: Array[String]) = {
    val conf = new SparkConf().
      setAppName("evaluate user item prefer").
      set("spark.sql.parquet.binaryAsString", "true");
    // Spark context.
    val sc: SparkContext = new SparkContext(conf);
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc);

    val temp = sqlContext.sql("select * from s_dg_user_item_prefer_spark").map(x => (x(0), x(1)));

    val result = temp.map(x => (x._2.toString.split(",").size, x._2.toString.split(",").size, x._2.toString.split(",").size, 1)).reduce((a, b) => (Math.min(a._1, b._1), Math.max(a._2, b._2), a._3 + b._3, a._4 + b._4));

    println("Static result: ");
    println("min: " + result._1);
    println("max: " + result._2);
    println("avg: " + result._3 / result._4);
    println("total user: " + result._4);
  }
}
