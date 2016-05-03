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
  val YESTERDAY_CONST = "YESTERDAY"

  def getSql(sqlFilePath: String): String = {
    var inputStream: InputStream = SqlUtil.getClass.getResourceAsStream(sqlFilePath)
    if (inputStream == null) {
      inputStream = ClassLoader.getSystemClassLoader.getResourceAsStream(sqlFilePath)
    }

    val sqlTemplate: String = IOUtils.toString(inputStream, "UTF-8")

    val sql: String = sqlTemplate.replace(YESTERDAY_CONST, getDate(1))

    return sql
  }

  private def getDate(N: Int): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -N)
    return sdf.format(calendar.getTime)
  }
}
