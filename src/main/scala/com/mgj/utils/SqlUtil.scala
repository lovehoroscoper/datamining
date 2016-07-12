package com.mgj.utils

import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.Validate

/**
  * Created by xiaonuo on 4/12/16.
  */
object SqlUtil {
  val DATE_CONST = "BIZDATE"
  val RESOURCE_CONST = "RESOURCE"
  val DATE_SUB7_CONST = "BIZDATE_SUB7"

  def getResourceSql(sqlFilePath: String, resourceCodes: String*): String = {
    var inputStream: InputStream = SqlUtil.getClass.getResourceAsStream(sqlFilePath)
    if (inputStream == null) {
      inputStream = ClassLoader.getSystemClassLoader.getResourceAsStream(sqlFilePath)
    }

    val sqlTemplate: String = IOUtils.toString(inputStream, "UTF-8")

    val code = resourceCodes.map(x => "'" + x + "'").mkString(",")
    val sql: String = sqlTemplate.replace(RESOURCE_CONST, code)

    return sql
  }

  def getSampleSql(sqlFilePath: String): String = {
    var inputStream: InputStream = SqlUtil.getClass.getResourceAsStream(sqlFilePath)
    if (inputStream == null) {
      inputStream = ClassLoader.getSystemClassLoader.getResourceAsStream(sqlFilePath)
    }

    val sqlTemplate: String = IOUtils.toString(inputStream, "UTF-8")

    val sql: String = sqlTemplate.replace(DATE_CONST, getDate(-1))

    return sql
  }

  def getSampleSql(sqlFilePath: String, bizdate: String): String = {
    var inputStream: InputStream = SqlUtil.getClass.getResourceAsStream(sqlFilePath)
    if (inputStream == null) {
      inputStream = ClassLoader.getSystemClassLoader.getResourceAsStream(sqlFilePath)
    }

    val sqlTemplate: String = IOUtils.toString(inputStream, "UTF-8")

    val sql: String = sqlTemplate.replace(DATE_CONST, bizdate)

    return sql
  }

  def getBlackUserSql(sqlFilePath: String = "/sql/get_black_user.sql"): String = {
    var inputStream: InputStream = SqlUtil.getClass.getResourceAsStream(sqlFilePath)
    if (inputStream == null) {
      inputStream = ClassLoader.getSystemClassLoader.getResourceAsStream(sqlFilePath)
    }

    val sqlTemplate: String = IOUtils.toString(inputStream, "UTF-8")

    val sql: String = sqlTemplate.replace(DATE_SUB7_CONST, getDate(-8))

    return sql
  }

  def getBlackDeviceSql(sqlFilePath: String = "/sql/get_black_device.sql"): String = {
    var inputStream: InputStream = SqlUtil.getClass.getResourceAsStream(sqlFilePath)
    if (inputStream == null) {
      inputStream = ClassLoader.getSystemClassLoader.getResourceAsStream(sqlFilePath)
    }

    val sqlTemplate: String = IOUtils.toString(inputStream, "UTF-8")

    val sql: String = sqlTemplate.replace(DATE_SUB7_CONST, getDate(-8))

    return sql
  }

  def getDate(N: Int): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, N)
    return sdf.format(calendar.getTime)
  }
}
