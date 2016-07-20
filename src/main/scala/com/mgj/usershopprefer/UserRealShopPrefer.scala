package com.mgj.usershopprefer

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by xiaonuo on 12/16/15.
 */
object UserRealShopPrefer {
  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("calculate user shop prefer")
      .set("spark.cores.max", "28")
    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    //    val userOrderLog = sqlContext.sql("select user_id, shop_id, time from s_dg_user_base_log where pt = '2015-11-26' and action_type = 'order' and platform_type = 'app'")
    //    val userClickLog = sqlContext.sql("select user_id, shop_id, time from s_dg_user_base_log where pt = '2015-11-26' and action_type = 'click' and platform_type = 'app'")
    //    val userFavorLog = sqlContext.sql("select user_id, shop_id, time from s_dg_user_base_log where pt = '2015-11-26' and action_type = 'favor'")

    //    val userOrderLog = sqlContext.sql("select user_id, shop_id, item_id from s_dg_user_base_log where pt = '2015-12-15' and action_type = 'order' and platform_type = 'app' and category_id in ('684','685','687','688','690','691','692','695','696','697','698','699','704','705','716','718','848','1299','1390','1437','2698','2699','4159','4160','686','689','693','694','700','701','706','710','711','712','713','714','715','717','719','720','846','847','849','852','1272','1436','1438','2695')").rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString))
    //
    //    val userClickLog = sqlContext.sql("select user_id, shop_id, time from s_dg_user_base_log where pt = '2015-11-26' and action_type = 'click' and platform_type = 'app'")
    //
    //    val userOrderLog = sqlContext.sql("select user_id, shop_id, time from s_dg_user_base_log where pt = '2015-12-15' and action_type = 'order' and platform_type = 'app'").rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString))
    //    val userShopPreferOrderSub = sc.textFile("hdfs://mgjcluster/user/digu/userShopPreferSub").map(x => (x.split(" ")(0), x.split(" ")(1).split(",").map(x => x.split(":")(0)).take(20))).map(x => x._2.map(y => x._1 + "_" + y)).flatMap(x => x).map(x => (x.split("_")(0), x.split("_")(1)))

    //    println(userOrderLog.count())
    //    println(userOrderLog.map(x => x._1).distinct().count())
    //    val r = userOrderLog.map(x => (x._1 + x._2, x)).join(userShopPreferOrderSub.map(x => (x._1 + x._2, x)))
    //    println(r.count)
    //    println(r.map(x => (x._2._1._1)).distinct().count)
    //
    //    val userShopPreferOrderSub10 = sc.textFile("hdfs://mgjcluster/user/digu/userShopPreferSub").map(x => (x.split(" ")(0), x.split(" ")(1).split(",").map(x => x.split(":")(0)).take(10))).map(x => x._2.map(y => x._1 + "_" + y)).flatMap(x => x).map(x => (x.split("_")(0), x.split("_")(1)))
    //    val r10 = userOrderLog.map(x => (x._1 + x._2, x)).join(userShopPreferOrderSub10.map(x => (x._1 + x._2, x)))
    //    println(r10.count)
    //    println(r10.map(x => (x._2._1._1)).distinct().count)
    //
    //    val test = userShopPreferOrderSub.map(x=> (x._1)).distinct().map(x=>(x,1)).join(userOrderLog.map(x =>x._1).distinct().map(x=>(x,1))).count()
  }
}
