package com.mgj.usershopprefer

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by xiaonuo on 12/14/15.
 */
object ShopAnalysis {
  def main(args: Array[String]): Unit = {
    // args[0]: User base log.
    val conf = new SparkConf().
      setAppName("calculate shop rank").
      set("spark.sql.parquet.binaryAsString", "true")
    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    val bizdateSub1 = "2015-12-13"
    val bizdateSub30 = "2015-11-12"
    val orderSql = "select user_id, shop_id, time, user_defined as price, pt from s_dg_user_base_log where pt >= '" + bizdateSub30 + "' and pt <= '" + bizdateSub1 + "' and action_type = 'order' and user_id is not null and shop_id is not null and time is not null"

    val userOrderLog = sqlContext.sql(orderSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(3).toString.toDouble, x(4).toString))

    val schema = StructType(
      StructField("shop_id", StringType, true)
        :: StructField("date", StringType, true)
        :: StructField("gmv", DoubleType, true)
        :: Nil)

    val shopOrder = userOrderLog.map(x => (x._2 + "_" + x._4, x)).groupByKey().map(x => {
      var sum: Double = 0d
      for (t <- x._2) {
        sum += t._3
      }
      Row(x._1.split("_")(0), x._1.split("_")(1), sum)
    })

    sqlContext.createDataFrame(shopOrder, schema).registerTempTable("s_dg_shop_analysis_temp")
    //    sqlContext.sql("drop table if exists s_dg_shop_analysis")
    sqlContext.sql("insert overwrite table s_dg_shop_analysis select * from s_dg_shop_analysis_temp")

    val shopGMV = sqlContext.sql("select t1.* from s_dg_shop_analysis t1 join( select shop_id, avg(gmv) as gmv from s_dg_shop_analysis group by shop_id sort by gmv desc limit 100) t2 on t1.shop_id = t2.shop_id")

    shopGMV.map(x => (x(0).toString, x(1).toString, x(2).toString.toDouble)).map(x => (x._1, x)).groupByKey().map(x => {
      x._2.toList.map(x => x._2 + " " + x._3).mkString(",")
    })

  }
}
