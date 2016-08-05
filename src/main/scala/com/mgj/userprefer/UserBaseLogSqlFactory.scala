package com.mgj.userprefer

import java.io.InputStream

import org.apache.commons.io.IOUtils

/**
  * Created by xiaonuo on 8/3/16.
  */
object UserBaseLogSqlFactory {
  val BIZDATE = "BIZDATE"
  val BIZDATESUBA = "BIZDATESUBA"
  val BIZDATESUBB = "BIZDATESUBB"
  val ENTITY = "ENTITY"

  private def getTemplate(sqlFilePath: String): String = {
    var inputStream: InputStream = UserBaseLogSqlFactory.getClass.getResourceAsStream(sqlFilePath)
    if (inputStream == null) {
      inputStream = ClassLoader.getSystemClassLoader.getResourceAsStream(sqlFilePath)
    }
    val sqlTemplate: String = IOUtils.toString(inputStream, "UTF-8")
    return sqlTemplate
  }

  private def getFeatureSql(sqlFilePath: String, bizdateSubA: String, bizdateSubB: String, entity: String): String = {
    val sqlTemplate = getTemplate(sqlFilePath)
    val sql: String = sqlTemplate
      .replace(BIZDATESUBA, bizdateSubA)
      .replace(BIZDATESUBB, bizdateSubB)
      .replace(ENTITY, entity)
    println(sql)
    return sql
  }

  private def getSampleSql(sqlFilePath: String, bizdate: String, entity: String): String = {
    val sqlTemplate = getTemplate(sqlFilePath)
    val sql: String = sqlTemplate
      .replace(BIZDATE, bizdate)
      .replace(ENTITY, entity)
    println(sql)
    return sql
  }

  def getClickFeatureSql(bizdateSubA: String, bizdateSubB: String, entity: String, sqlFilePath: String = "/sql/userprefer/get_click_log.sql"): String = {
    return getFeatureSql(sqlFilePath, bizdateSubA, bizdateSubB, entity)
  }

  def getAddCartFeatureSql(bizdateSubA: String, bizdateSubB: String, entity: String, sqlFilePath: String = "/sql/userprefer/get_add_cart_log.sql"): String = {
    return getFeatureSql(sqlFilePath, bizdateSubA, bizdateSubB, entity)
  }

  def getOrderFeatureSql(bizdateSubA: String, bizdateSubB: String, entity: String, sqlFilePath: String = "/sql/userprefer/get_order_log.sql"): String = {
    return getFeatureSql(sqlFilePath, bizdateSubA, bizdateSubB, entity)
  }

  def getFavorFeatureSql(bizdateSubA: String, bizdateSubB: String, entity: String, sqlFilePath: String = "/sql/userprefer/get_favor_log.sql"): String = {
    return getFeatureSql(sqlFilePath, bizdateSubA, bizdateSubB, entity)
  }

  def getClickSampleSql(bizdate: String, entity: String, sqlFilePath: String = "/sql/userprefer/get_click_sample_log.sql"): String = {
    return getSampleSql(sqlFilePath, bizdate, entity)
  }

  def getOrderSampleSql(bizdate: String, entity: String, sqlFilePath: String = "/sql/userprefer/get_order_sample_log.sql"): String = {
    return getSampleSql(sqlFilePath, bizdate, entity)
  }
}
