package com.mgj.utils

import java.nio.file.Paths

import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.{JiebaSegmenter, WordDictionary}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 4/1/16.
  */
object WordSegUtil {

  val segmenter: JiebaSegmenter = new JiebaSegmenter()

  def loadDict(dictPath: String): Unit = {
    val dict = WordDictionary.getInstance()
    val path = Paths.get(dictPath)
    dict.loadUserDict(path)
  }

  def process(str: String): List[String] = {
    return segmenter.process(str, SegMode.SEARCH).toList.map(x => x.word).filter(x => WordUtil.wordsFilter(x) == true)
  }
}
