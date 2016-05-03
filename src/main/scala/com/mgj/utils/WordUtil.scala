package com.mgj.utils

import java.io.BufferedReader
import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files}
import java.util

import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 4/1/16.
  */
object WordUtil {

  val dict = new util.HashSet[String]()

  def isDigitalOrLetter(s: String): Boolean = {
    for (c <- s.toCharArray) {
      if (!(Character.isDigit(c) || Character.isLetter(c))) {
        return false
      }
    }
    return true
  }

  // filter chinese, alphabet and digital
  def wordsFilter(s: String): Boolean = {
    for (c <- s.toCharArray) {
      val sc = Character.UnicodeScript.of(c)
      if (!(sc == Character.UnicodeScript.HAN || (c >= 65 && c <= 90 || c >= 97 && c <= 122))) {
        return false
      }
    }
    return true
  }

  def loadDict(dictPath: String): Unit = {
    val path = Paths.get(dictPath)
    val br: BufferedReader = Files.newBufferedReader(path, StandardCharsets.UTF_8)

    val s: Long = System.currentTimeMillis
    var count: Int = 0
    while (br.ready) {
      val line: String = br.readLine
      dict.add(line)
      count += 1
    }

    println(s"words cnt:${count}")
    println(s"time cost:${System.currentTimeMillis - s}")
    br.close
  }

  def reverse(src: String): String = {
    val sb = new StringBuilder(src)
    return sb.reverse.toString()
  }

  //  def checkWord(src: String): Boolean = {
  //    if (src.size > 2 && dict.contains(src.substring(0, src.length - 1))) {
  //
  //    }
  //  }

  // 3.1、词必须在语料中出现过，以排除语料中因为句尾和句首链接而形成的错误词
  // 3.2、校验是否是复合词,是复合词则不算新词
  // 3.3、从原有词典中搜索所有以当前词首字结尾的词，用以排除类似“装新款，衣外套”等词
  // 3.4、从原有词典中搜索所有以当前词尾字开头的词，用以排除类似"长款针"等词
}
