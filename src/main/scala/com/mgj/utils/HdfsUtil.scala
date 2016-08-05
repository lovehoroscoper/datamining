package com.mgj.utils

import org.apache.spark.SparkContext

import scala.util.matching.Regex

/**
  * Created by xiaonuo on 5/2/16.
  */
object HdfsUtil {
  def isExists(sc: SparkContext, path: String): Boolean = {
    val conf = sc.hadoopConfiguration
    conf.set("fs.default.name", "hdfs://mgjcluster")
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    if (path.isEmpty) {
      return false
    }
    val exists = fs.exists(new org.apache.hadoop.fs.Path(path))
    return exists
  }

  def getDirWithDate(sc: SparkContext, pathPrefix: String, date: String): String = {
    val path = s"${pathPrefix}/${date}"
    if (isExists(sc, path)) {
      return path
    } else {
      val pattern = """(\d{4}).*(\d{2}).*(\d{2})""".r
      val dateMeta = pattern.findFirstIn(date)
      if (dateMeta != None) {
        val pattern(year, month, day) = dateMeta.get
        val newPath = s"${pathPrefix}/${year}${month}${day}"
        if (isExists(sc, newPath)) {
          return newPath
        } else {
          return s"${pathPrefix}"
        }
      } else {
        return s"${pathPrefix}"
      }
    }
  }
}
