package com.mgj.usershopprefer

import java.io.BufferedReader
import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Path, Files}
import java.util

import com.mgj.utils.ShopInfoUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by xiaonuo on 9/15/15.
  */
object ShopSelect {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("shop select").
      set("spark.sql.parquet.binaryAsString", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val bizdate = args(0)
    val bizdateSub = args(1)
    val FilePath = args(2)
    val simPath = args(3)
    val weight = args(4).toDouble
    //    val br: BufferedReader = Files.newBufferedReader(Paths.get(FilePath), StandardCharsets.UTF_8)
    //    val shopIdSet = new util.HashSet[String]()
    //    while (br.ready) {
    //      shopIdSet.add(br.readLine)
    //    }
    //    val shopId = sc.textFile(FilePath).collect()

    //            val bizdate = "2016-01-10"
    //            val bizdateSub = "2016-01-09"
    //            val simPath = "/user/digu/shopSimRank"
    //            val FilePath = "/user/digu/shopPL"

    val clickLogSql = "select user_id, shop_id, time from s_dg_user_base_log where pt >= '" + bizdateSub + "' and pt <= '" + bizdate + "' and action_type = 'click' and platform_type = 'app'"
    val orderLogSql = "select user_id, shop_id, time, user_defined as price from s_dg_user_base_log where pt >= '" + bizdateSub + "' and pt <= '" + bizdate + "' and action_type = 'order'"

    val clickLog = sqlContext.sql(clickLogSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString))
    val orderLog = sqlContext.sql(orderLogSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString))

    val shopSeedIdSet = sc.textFile(FilePath).collect().toSet
    val shopSimMap = new util.HashMap[String, Double]()
    sc.textFile(simPath).map(x => (x.split(" ")(0), x.split(" ")(1))).filter(x => shopSeedIdSet.contains(x._1)).collect().foreach(x => {
      x._2.split(",").foreach(x => {
        val key = x.split(":")(0)
        val value = x.split(":")(1).toDouble
        if (!shopSimMap.containsKey(key) || shopSimMap.containsKey(key) && shopSimMap.get(key) < value) {
          shopSimMap.put(key, value)
        }
      })
    })

    val userCntClick = clickLog.groupBy(_._1).map(x => (x._1, x._2.map(x => x._2).toList.size)).filter(x => x._2 <= 2000)
    val userCntOrder = orderLog.groupBy(_._1).map(x => (x._1, x._2.map(x => x._2).toList.size)).filter(x => x._2 <= 50)

    // user_id, item_id, visit_time, user_item_count, shop_id, shop_name.
    val clickLogFilter = clickLog.map(x => (x._1, x)).join(userCntClick).map(x => (x._2._1._1, x._2._1._2, x._2._1._3))
    val orderLogFilter = orderLog.map(x => (x._1, x)).join(userCntOrder).map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4))

    // per customer transaction
    // per product transaction
    val shopOrderIndex = orderLogFilter.groupBy(x => x._2).map(x => {
      val uv = x._2.map(x => x._1).toSet.size
      val pv = x._2.map(x => x._1).size
      val gmv = x._2.map(x => x._4.toDouble / 100).sum
      (x._1, gmv, uv, pv)
    })

    val shopClickIndex = clickLogFilter.groupBy(x => x._2).map(x => {
      val uv = x._2.map(x => x._1).toSet.size
      val pv = x._2.map(x => x._1).size
      (x._1, uv, pv)
    })

    val shopRatioIndex = shopClickIndex.map(x => (x._1, x)).leftOuterJoin(shopOrderIndex.map(x => (x._1, x))).map(x => {
      var orderUV = 0
      var orderPV = 0
      var orderGMV = 0d
      if (x._2._2 != null && x._2._2 != None) {
        orderUV = x._2._2.get._3
        orderPV = x._2._2.get._4
        orderGMV = x._2._2.get._2
      }

      val clickUV = x._2._1._2
      val clickPV = x._2._1._3
      var pct = 0d
      if (orderUV > 0) {
        pct = 1.0 * orderGMV / orderUV
      }
      var ppt = 0d
      if (orderPV > 0) {
        ppt = 1.0 * orderGMV / orderPV
      }

      (x._1, 1.0 * orderUV / clickUV, 1.0 * orderPV / clickPV, clickUV, clickPV, orderUV, orderPV, pct, ppt)
    }).filter(x => x._2 <= 1 && x._3 <= 1).cache()

    // click.
    val shopClickCycleIndex = clickLogFilter.groupBy(x => x._1 + x._2).map(x => {
      val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val userId = x._2.head._1
      val shopId = x._2.head._2
      var dateMax = df.parse("2000-01-01 00:00:00")
      var dateMin = new Date()

      for (e <- x._2) {
        val date = df.parse(e._3)
        if (date.after(dateMax)) {
          dateMax = date
        }
        if (date.before(dateMin)) {
          dateMin = date
        }
      }

      var revisit = 0
      if (Math.abs((dateMax.getTime() - dateMin.getTime()) / 1000 / 60 / 60 / 24) >= 1) {
        revisit = 1
      }
      (userId, shopId, revisit)
    }).map(x => (x._2, (x._2, x._3))).groupBy(x => x._1).map(x => (x._1, x._2.head._2._1, x._2.map(x => x._2._2).sum)).cache()

    val shopIdSet = sqlContext.sql("select shopid,count(*) as total, sum(category_level2_flag) as category_level2_top, sum(category_level1_flag) as category_level1_top from (select shopid,case when categoryname2='上装' then 1 else 0 end as category_level2_flag,case when categoryname1='女装' then 1 else 0 end as category_level1_flag from v_dw_trd_tradeitem) x group by shopid")
      .map(x => (x(0).toString, x(2).toString.toDouble / x(1).toString.toDouble, x(3).toString.toDouble / x(1).toString.toDouble)).filter(x => x._2 > 0.5 && x._3 > 0.9).map(x => x._1).distinct().collect().toSet
    val shopInfo = sqlContext.sql("select shopid, fans, add_new_7d_num from dw_trd_xshop_wide where visit_date = '" + bizdate + "'").rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, (x(1).toString, x(2).toString))).filter(x => shopIdSet.contains(x._1)).collect.toMap

    val shopIndex = shopRatioIndex.map(x => (x._1, x)).join(shopClickCycleIndex.map(x => (x._1, x))).map(x => {
      var simScore = 0d
      if (shopSimMap.containsKey(x._1)) {
        simScore = shopSimMap.get(x._1)
      }

      var favorScore = 0d
      var addNewScore = 0d
      if (shopInfo.contains(x._1)) {
        favorScore = shopInfo.get(x._1).get._1.toDouble
        addNewScore = shopInfo.get(x._1).get._2.toDouble
      }

      // uv_cvr pct revisit_uv order_uv favor_score add_new_score
      val generalScore = x._2._1._2 * Math.log(1 + x._2._1._8) * Math.log(1 + x._2._2._3) * Math.log(1 + x._2._1._6) * Math.log10(10 + favorScore) * Math.log(10 + addNewScore) + weight * simScore
      (x._2._1._1.toString, ShopInfoUtil.toUrl(x._2._1._1).toString, x._2._1._2.toString, x._2._1._3.toString, x._2._1._4.toString, x._2._1._5.toString, x._2._1._6.toString, x._2._1._7.toString, x._2._1._8.toString, x._2._1._9.toString, x._2._2._3.toString, simScore.toString, favorScore.toString, addNewScore.toString, generalScore)
    }).filter(x => shopIdSet.contains(x._1)).filter(x => x._9.toDouble >= 100d && x._10.toDouble >= 100d).sortBy(x => x._15, false).cache()

    println("total shop count: " + shopIndex.count)
    shopIndex.take(100).foreach(println)

    val schema =
      StructType(
        StructField("shop_id", StringType, true) ::
          StructField("shop_url", StringType, true) ::
          StructField("uv_cvr", StringType, true) ::
          StructField("pv_cvr", StringType, true) ::
          StructField("click_uv", StringType, true) ::
          StructField("click_pv", StringType, true) ::
          StructField("order_uv", StringType, true) ::
          StructField("order_pv", StringType, true) ::
          StructField("pct", StringType, true) ::
          StructField("ppt", StringType, true) ::
          StructField("revisit_uv", StringType, true) ::
          StructField("sim_score", StringType, true) ::
          StructField("favor_num", StringType, true) ::
          StructField("add_new", StringType, true) ::
          StructField("general_score", DoubleType, true) :: Nil)

    sqlContext.createDataFrame(shopIndex.map(x => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14, x._15)), schema).registerTempTable("s_dg_shop_index_temp")
    sqlContext.sql("insert overwrite table s_dg_shop_index select * from s_dg_shop_index_temp")
  }
}
