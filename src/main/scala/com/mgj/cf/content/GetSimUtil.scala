package com.mgj.cf.content

import com.mgj.utils.KMUtil

import scala.collection.mutable

/**
  * Created by xiaonuo on 4/23/16.
  */
object GetSimUtil {
  def getSimScore(wordSimMap: Map[(String, String), Double], wordTag: Map[String, String], wordIdf: Map[String, Double], queryX: List[String], queryY: List[String]): Double = {
    val weight = mutable.Map[(Int, Int), Double]()
    val weightIdf = mutable.Map[Int, Double]()
    val set = "产品类型-复合,产品类型修饰词,产品-品牌,产品-型号,空白".split(",").toSet
    for (i <- 0 to queryX.size - 1; j <- 0 to queryY.size - 1) {
      val x = queryX.apply(i)
      val y = queryY.apply(j)
      val xset = x.toCharArray.toSet
      val yset = y.toCharArray.toSet
      val intersection = (xset & yset).size
      val union = (xset | yset).size
      val idfx = wordIdf.get(x).getOrElse(0d)
      val idfy = wordIdf.get(y).getOrElse(0d)
      val sim = wordSimMap.get((x, y)).getOrElse(0d)
      //      val simScore = (1.0 * intersection / union + sim)
      val simScore = 1.0 * intersection / union
      val tagx = wordTag.get(x).getOrElse("空白")
      val tagy = wordTag.get(y).getOrElse("空白")
      if (set.contains(tagx) && set.contains(tagy)) {
        weight.put((i, j), simScore)
        weightIdf.put(i, idfx)
        weightIdf.put(j, idfy)
      }
    }
    val x = weight.map(x => x._1._1).toSet.size
    val y = weight.map(x => x._1._2).toSet.size
    return if (weight.isEmpty) 0d else KMUtil.findMaxWeightSumCos(weight, weightIdf) / Math.sqrt(1 + x) / Math.sqrt(1 + y)
  }
}
