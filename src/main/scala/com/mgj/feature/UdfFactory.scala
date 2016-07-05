package com.mgj.feature

import java.util
import javax.annotation.PostConstruct

import org.apache.spark.sql.hive.HiveContext
import org.springframework.stereotype.Service

/**
  * Created by xiaonuo on 7/5/16.
  */
class UdfFactory {
  private var udfs: util.HashMap[String, UdfTemplate] = _

  def setUdfs(calculators: util.HashMap[String, UdfTemplate]): Unit = {
    this.udfs = calculators
  }

  def getUdf(name: String): UdfTemplate = {
    return udfs.get(name)
  }

  def containsUdf(name: String): Boolean = {
    return udfs.containsKey(name)
  }

  def init(sqlContext: HiveContext): Unit = {
    for (e <- udfs.keySet()) {
      udfs.get(e).register(sqlContext)
    }
  }
}
