package com.mgj.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by xiaonuo on 3/3/16.
  */
object PartitionUtil {
  def checkAppLog(sqlContext: HiveContext, bizdate: String, actionType: String): Unit = {
    var flag = true
    while (flag) {
      val partitions = sqlContext.sql("show partitions s_dg_user_base_log").rdd.map(x => x(0).toString).map(x => (x.split("/")(0).split("=")(1), x.split("/")(1).split("=")(1), x.split("/")(2).split("=")(1))).filter(x => x._2.equals(actionType) && x._3.equals("app")).map(x => x._1).collect().toSet
      if (partitions.contains(bizdate)) {
        flag = false
      } else {
        Thread.sleep(30000)
        val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        val date = new Date()
        System.out.println(df.format(date) + ", partitions does not exists")
      }
    }
  }

  def getMaxPt(sqlContext: HiveContext, table: String): String = {
    val maxpt = sqlContext.sql("show partitions " + table).rdd.filter(x => x.anyNull == false).map(x => x(0).toString.split("=")(1)).sortBy(x => x, false).take(1).apply(0)
    return maxpt
  }
}
