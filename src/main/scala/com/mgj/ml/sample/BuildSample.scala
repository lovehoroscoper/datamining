package com.mgj.ml.sample

import java.text.SimpleDateFormat
import java.util.Date
import com.mgj.utils.{PartitionUtil, SourceFilter}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.context.support.{AbstractApplicationContext, ClassPathXmlApplicationContext}

/**
  * Created by xiaonuo on 11/18/15.
  */
object BuildSample {

  var context: AbstractApplicationContext = null

  def init() = {
    context = new ClassPathXmlApplicationContext("dataMiningContext.xml")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("build sample").
      set("spark.sql.parquet.binaryAsString", "true")
    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    init()

    val bizDate = args(0)
    //    val bizDate = "2015-12-17"

    PartitionUtil.checkAppLog(sqlContext, bizDate, "click")
    PartitionUtil.checkAppLog(sqlContext, bizDate, "order")
    PartitionUtil.checkAppLog(sqlContext, bizDate, "expose")
    val exposeSql = "select user_id, item_id, shop_id, time, refer from s_dg_user_base_log where pt = '" + bizDate + "' and action_type = 'expose' and platform_type = 'app'"
    val clickSql = "select user_id, item_id, shop_id, time, refer from s_dg_user_base_log where pt = '" + bizDate + "' and action_type = 'click' and platform_type = 'app'"
    val orderSql = "select user_id, item_id, shop_id, time, refer from s_dg_user_base_log where pt = '" + bizDate + "' and action_type = 'order' and platform_type = 'app'"

    println("get order sql:")
    println("{" + orderSql + "}")
    println("get expose sql:")
    println("{" + exposeSql + "}")
    println("get click sql:")
    println("{" + clickSql + "}")

    val exposeLog = sqlContext.sql(exposeSql).rdd.filter(x => x.anyNull == false).filter(x => SourceFilter.isFrom("app_book", x(4).toString)).repartition(200).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString))
    val orderLog = sqlContext.sql(orderSql).rdd.filter(x => x.anyNull == false).filter(x => SourceFilter.isFrom("app_book", x(4).toString)).repartition(200).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString))
    val clickLog = sqlContext.sql(clickSql).rdd.filter(x => x.anyNull == false).filter(x => SourceFilter.isFrom("app_book", x(4).toString)).repartition(200).map(x => (x(0).toString, x(1).toString, x(2).toString, x(3).toString))

    def joinFunc(x: Option[(String, String, String, String)], y: Option[(String, String, String, String)], interval: Int): (String, String, String, String, String) = {
      var label = "0"
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      if (y != null && y != None) {
        val timeClick = sdf.parse(y.get._4).getTime
        if (x != null && x != None) {
          val timeExpose = sdf.parse(x.get._4).getTime
          if (Math.abs(timeClick - timeExpose) < 1000 * interval) {
            label = "1"
          }
        } else {
          label = "1"
        }
      }
      if (x != null && x != None) {
        (x.get._1, x.get._2, x.get._3, x.get._4, label)
      } else {
        (y.get._1, y.get._2, y.get._3, y.get._4, label)
      }
    }

    val clickSample = exposeLog.map(x => (x._1 + x._2, x)).fullOuterJoin(clickLog.map(x => (x._1 + x._2, x))).map(x => joinFunc(x._2._1, x._2._2, 120))
    val orderSample = clickLog.map(x => (x._1 + x._2, x)).fullOuterJoin(orderLog.map(x => (x._1 + x._2, x))).map(x => joinFunc(x._2._1, x._2._2, 60 * 60))

    val clickSamplePos = clickSample.filter(x => x._5 == "1")
    val clickPosCnt = clickSamplePos.count()
    val clickTotalCnt = clickSample.count()
    val clickSampleRatio = clickPosCnt.toDouble / clickTotalCnt
    val clickSampleNeg = clickSample.filter(x => x._5 == "0").sample(false, clickSampleRatio)
    val clickSampleFinal = clickSamplePos.union(clickSampleNeg)

    println("clickPosCnt: " + clickPosCnt)
    println("clickTotalCnt: " + clickTotalCnt)
    println("clickSampleRatio: " + clickSampleRatio)

    val orderSamplePos = orderSample.filter(x => x._5 == "1")
    val orderPosCnt = orderSamplePos.count()
    val orderTotalCnt = orderSample.count()
    val orderSampleRatio = orderPosCnt.toDouble / orderTotalCnt
    val orderSampleNeg = orderSample.filter(x => x._5 == "0").sample(false, orderSampleRatio)
    val orderSampleFinal = orderSamplePos.union(orderSampleNeg)

    println("orderPosCnt: " + orderPosCnt)
    println("orderTotalCnt: " + orderTotalCnt)
    println("orderSampleRatio: " + orderSampleRatio)

    val allSampleFinal = clickSampleFinal.union(orderSampleFinal).union(orderSamplePos).union(orderSamplePos).union(orderSamplePos).union(orderSamplePos)

    println("allSampleFinalCnt: " + allSampleFinal.count)

    val schema =
      StructType(
        StructField("user_id", StringType, true) ::
          StructField("item_id", StringType, true) ::
          StructField("shop_id", StringType, true) ::
          StructField("time", StringType, true) ::
          StructField("label", StringType, true) :: Nil)

    val clickSampleDF = sqlContext.createDataFrame(clickSampleFinal.map(x => Row(x._1, x._2, x._3, x._4, x._5)), schema)
    val orderSampleDF = sqlContext.createDataFrame(orderSampleFinal.map(x => Row(x._1, x._2, x._3, x._4, x._5)), schema)
    val allSampleDF = sqlContext.createDataFrame(allSampleFinal.map(x => Row(x._1, x._2, x._3, x._4, x._5)), schema)

    //    clickSampleDF.registerTempTable("s_dg_click_sample_temp")
    //    orderSampleDF.registerTempTable("s_dg_order_sample_temp")
    //    allSampleDF.registerTempTable("s_dg_all_sample_temp")
    //
    //    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    //    sqlContext.sql("drop table if exists s_dg_click_sample")
    //    sqlContext.sql("create table s_dg_click_sample as select * from s_dg_click_sample_temp")
    //
    //    sqlContext.sql("drop table if exists s_dg_order_sample")
    //    sqlContext.sql("create table s_dg_order_sample as select * from s_dg_order_sample_temp")
    //
    //    sqlContext.sql("drop table if exists s_dg_all_sample")
    //    sqlContext.sql("create table s_dg_all_sample as select * from s_dg_all_sample_temp")

    sqlContext.sql("drop table if exists s_dg_click_sample")
    sqlContext.sql("drop table if exists s_dg_order_sample")
    sqlContext.sql("drop table if exists s_dg_all_sample")
    clickSampleDF.write.saveAsTable("s_dg_click_sample")
    orderSampleDF.write.saveAsTable("s_dg_order_sample")
    allSampleDF.write.saveAsTable("s_dg_all_sample")

    println("THE END")

    sc.stop()
  }
}
