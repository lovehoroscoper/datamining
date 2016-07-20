package com.mgj.cf.dump

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis

/**
 * Created by xiaonuo on 10/10/15.
 */
object ItemCFDelete {

  // Dump.
  lazy val jedis = new Jedis("chenyang.cache.mogujie.org", 6379)

  lazy val pipline = {
    jedis.select(100)
    jedis.connect()
    jedis.pipelined()
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().
      setAppName("delete item cf score").
      set("spark.sql.parquet.binaryAsString", "true")
    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    val itemIds = sqlContext.sql("select tradeitemid from v_dw_trd_tradeitem").map(x => "digu_cf_item_" + x(0).toString())

    val itemIdsTotal = itemIds.collect()
    System.out.println("item size: " + itemIdsTotal.size)
    val interval = 1000
    var start = 0
    var end = start + interval
    while (start == 0 || end <= itemIdsTotal.size) {
      val t0 = System.currentTimeMillis()
      itemIdsTotal.slice(start, end).foreach((x: String) => pipline.del(List(x.toString): _*))
      pipline.sync()
      System.out.println("slice start: " + start + ", end: " + end + ", time cost: " + (System.currentTimeMillis() - t0))
      start = end + 1
      if (end == itemIdsTotal.size) {
        end = itemIdsTotal.size + 1
      } else if (end + interval > itemIdsTotal.size) {
        end = itemIdsTotal.size
      } else {
        end = end + interval
      }
    }
    System.out.println("END")
  }
}
