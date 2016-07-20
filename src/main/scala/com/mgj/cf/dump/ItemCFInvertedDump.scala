package com.mgj.cf.dump

import java.util

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 10/12/15.
  */
object ItemCFInvertedDump {

  def main(args: Array[String]) = {
    val conf = new SparkConf().
      setAppName("dump item cf inverted score").
      set("spark.sql.parquet.binaryAsString", "true")
    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    //    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    val cfSimDataFrame = sqlContext.sql("select * from s_dg_cf_sim_spark")

    def sort(x: Iterable[(Any, Any, Double)], N: Int): String = {
      var list = List[(Any, Any, Double)]()
      for (e <- x) {
        list :+= e
      }
      list = list.sortWith((a, b) => a._3.compareTo(b._3) > 0)
      val max = list.reduce((a, b) => (a._1, a._2, Math.max(a._3, b._3)))._3
      val min = list.reduce((a, b) => (a._1, a._2, Math.min(a._3, b._3)))._3
      // Normalize.
      if (max == min) {
        list = list.map(x => (x._1, x._2, x._3 - min + 1e-5))
      } else {
        list = list.map(x => (x._1, x._2, (x._3 - min + 1e-5) / (max - min)))
      }
      val output: util.ArrayList[String] = new util.ArrayList[String]()
      for (e <- list.take(N)) {
        output.add(e._2.toString)
      }
      // Score multiply 1e4
      //      for (e <- list.take(N)) {
      //        output :+= Math.round(e._3 * 1e4).toString
      //      }
      return output.mkString(",")
    }

    // Dump to redis
    // itemx, itemy, item_cntx, item_cnty, cos_score, jaccard_score, jaccard_score_v2, categoryx, categoryy.

    // val cfSimData = cfSimDataFrame.map(x => (x(0), x(1), x(6).asInstanceOf[Double]))
    // val cfSimDataTrunc = cfSimData.map(x => (x._1.toString, x)).join(orderItemData.map(x => (x.toString, x))).map(x => (x._2._1._2.toString, x._2._1)).join(orderItemData.map(x => (x.toString, x))).map(x => x._2._1)

    val dataToRedis = cfSimDataFrame.map(x => (x(0), x(1), x(6).asInstanceOf[Double])).groupBy(_._1).map(x => (x._1, sort(x._2, 100)))
    //val dataToRedis = cfSimDataTrunc.groupBy(_._1).map(x => (x._1, sort(x._2, 20)))

    dataToRedis.take(30).foreach({ case (key, value) =>
      println(key.toString + ":" + value)
    })
    println("Data number:" + dataToRedis.count())
    println("Data to redis...")

    dataToRedis.map(x => (x._1.toString + " " + x._2)).saveAsTextFile("/user/digu/itemSimInverted")
    println("data save to hdfs")

    lazy val jedis = new Jedis("chenyang.cache.mogujie.org", 6379)

    lazy val pipline = {
      jedis.select(100)
      jedis.connect()
      jedis.pipelined()
    }

    val dataToRedisTotal = dataToRedis.collect()
    val interval = 2000
    var start = 0
    var end = start + interval

    while (start == 0 || end <= dataToRedisTotal.size) {
      val t0 = System.currentTimeMillis()
      dataToRedisTotal.slice(start, end).foreach {
        case (key, value) =>
          pipline.setex(key.toString, 60 * 60 * 24 * 2, value)
      }
      pipline.sync()
      System.out.println("slice start: " + start + ", end: " + end + ", time cost: " + (System.currentTimeMillis() - t0))
      start = end + 1
      if (end == dataToRedisTotal.size) {
        end = dataToRedisTotal.size + 1
      } else if (end + interval > dataToRedisTotal.size) {
        end = dataToRedisTotal.size
      } else {
        end = end + interval
      }
    }

    //    dataToRedis.foreachPartition(iter => {
    //      lazy val jedis = new Jedis("chenyang.cache.mogujie.org", 6379)
    //
    //      lazy val pipline = {
    //        jedis.select(100)
    //        jedis.connect()
    //        jedis.pipelined()
    //      }
    //
    //      val interval = 2000
    //      var start = 0
    //      var end = start + interval
    //
    //      while (start == 0 || end <= iter.size) {
    //        iter.slice(start, end).foreach {
    //          case (key, value) =>
    //            // jedis.set("digu_offline_" + key.toString, value.toString)
    //            pipline.setex(key.toString, 60 * 60 * 24 * 2, value)
    //        }
    //
    //        pipline.sync()
    //        start = end + 1
    //        if (end == iter.size) {
    //          end = iter.size + 1
    //        } else if (end + interval > iter.size) {
    //          end = iter.size
    //        } else {
    //          end = end + interval
    //        }
    //      }
    //    })

    println("Synchronize done")
  }
}
