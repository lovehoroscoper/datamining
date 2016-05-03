package com.mgj.cf.evaluate

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis

/**
 * Created by xiaonuo on 9/2/15.
 */
object Evaluate {

  def main(args: Array[String]) = {
    val conf = new SparkConf().
      setAppName("dump item cf score").
      set("spark.sql.parquet.binaryAsString", "true")
    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val cfSimDataFrame = sqlContext.sql("select * from s_dg_cf_sim_spark")

    // Dump to redis
    // itemx, itemy, item_cntx, item_cnty, cos_score, jaccard_score, jaccard_score_v2, categoryx, categoryy.

    val temp = cfSimDataFrame.map(x => (x(0), x(1))).groupBy(_._1).map(x => (x._2.size, 1))

    val result = temp.map(x => (x._1, x._1, x._1, x._2)).reduce((a, b) => (Math.min(a._1, b._1), Math.max(a._2, b._2), a._3 + b._3, a._4 + b._4))

    println("Static result: ")
    println("min: " + result._1)
    println("max: " + result._2)
    println("avg: " + result._3 / result._4)
    println("total items: " + result._4)
  }
}
