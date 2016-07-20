package com.mgj.useritemprefer.dump

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis

/**
 * Created by xiaonuo on 8/28/15.
 */
object UserItemPreferDump {

  def main(args: Array[String]) = {
    val conf = new SparkConf().
      setAppName("dump user item prefer").
      set("spark.sql.parquet.binaryAsString", "true");
    // Spark context.
    val sc: SparkContext = new SparkContext(conf);
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster");

    //    val userItemPreferDataFrame = sqlContext.sql("select * from s_dg_user_item_prefer_spark");
    val userItemPrefer = sc.textFile("hdfs://mgjcluster/user/digu/userItemPrefer");

    // Dump to redis
    // val df = new java.text.DecimalFormat("0.000E0");
    val dataToRedis = userItemPrefer.map(x => (x.split(" ", 2)(0).replace("(", "").replace(")", ""), x.split(" ", 2)(1).replace("(", "").replace(")", "")));
    //    val dataToRedis = userItemPreferDataFrame.map(x => (x(0), x(1)));
    dataToRedis.take(30).foreach({ case (key, value) =>
      println("digu_offline_" + key.toString + ":" + value.toString);
    });
    println("Data to redis...");

    dataToRedis.foreachPartition(iter => {
      lazy val jedis = new Jedis("chenyang.cache.mogujie.org", 6379);
      jedis.select(99);
      jedis.connect();

      iter.foreach {
        case (key, value) =>
          // jedis.set("digu_offline_" + key.toString, df.format(value.toString));
          jedis.set("digu_offline_" + key.toString, value.toString);
      }
    });

    println("Write to redis!");
    println("Sync Ok!");
  }
}
