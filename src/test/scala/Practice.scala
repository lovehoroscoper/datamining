
import java.nio.file.{Paths, Path}
import java.text.SimpleDateFormat
import java.util
import java.util.regex.{Pattern, Matcher}

import com.google.gson.Gson
import com.huaban.analysis.jieba.JiebaSegmenter
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.WordDictionary
import com.mgj.feature.impl.UserRealItemPreferFeatureCalculator
import com.mgj.utils.{SampleUtil, SqlUtil, KMUtil, WordSegUtil}

import scala.collection.mutable

/**
  * Created by xiaonuo on 8/20/15.
  */

import java.util.{Calendar, Date}
import scala.collection.JavaConversions._

class Practice {

  def main(args: Array[String]) {
    //    val userFeature = "1:2016-04-11 12:10:10,2:2016-04-11 12:10:10,3:2016-04-11 13:10:10"
    //    val itemId = "4"
    //    val time = "2016-04-11 12:10:20"
    //    val itemSim = Map("2" -> "0:100,4:20,9:30")
    //
    //    println(Seq[(Long, Double)]().isEmpty)
    //    val gson = new Gson()
    //    val listA = List(1, 2, 3, 4, 5)
    //    val listB = List(6l, 7l, 8l, 9l, 10l)
    //    val list = new util.ArrayList[Int]()
    //    listA.foreach(x => list.add(x))
    //    listB.foreach(x => list.add(x.toInt))
    //    println(gson.toJson(list))
    //    val weight = mutable.Map[(Int, Int), Double]()
    //    weight.put((0, 0), 3d)
    //    weight.put((0, 1), 0d)
    //    weight.put((0, 2), 6d)
    //    weight.put((1, 0), 6d)
    //    weight.put((1, 1), 4d)
    //    weight.put((1, 2), 5d)
    //    weight.put((2, 0), 7d)
    //    weight.put((2, 1), 5d)
    //    weight.put((2, 2), 3d)
    //    println(weight.maxBy(x => x._2))
    //    val weight = mutable.Map((2, 1) -> 0.38003460939413936, (9, 6) -> 0.44394200060877975, (1, 0) -> 0.232229689682258, (5, 6) -> 0.611108591192656, (2, 3) -> 0.781110594405582, (7, 3) -> 0.525271335475628, (6, 5) -> 0.6138040907838621, (2, 5) -> 0.0, (5, 5) -> 0.0, (2, 4) -> 0.47475763094654044, (7, 0) -> 0.28661071670774246, (6, 2) -> 14.135220822471428, (7, 4) -> 1.4962384779952609, (3, 2) -> 0.0, (2, 0) -> 0.3954202807435232, (8, 4) -> 0.0, (5, 0) -> 0.32856076285944197, (8, 3) -> 1.0116197908925693, (8, 5) -> 2.306701771502605, (1, 5) -> 0.0, (2, 6) -> 0.0, (6, 3) -> 1.6792649594617686, (4, 2) -> 0.0, (5, 3) -> 0.6407812566675898, (6, 1) -> 0.2113998677636273, (8, 1) -> 0.19303977001079697, (5, 2) -> 0.0, (2, 2) -> 0.0, (6, 4) -> 0.0, (1, 3) -> 0.8861510518058282, (7, 6) -> 0.40738413790814443, (4, 6) -> 0.5094948592936884, (1, 1) -> 0.28207724614742175, (0, 2) -> 0.0, (3, 5) -> 0.0, (9, 2) -> 0.619421704178229, (3, 3) -> 0.5473820490119199, (8, 0) -> 0.1338275443698154, (4, 0) -> 0.26876933277149584, (3, 1) -> 0.3183156922850662, (0, 6) -> 0.2322296896822626, (0, 1) -> 0.27475950029909024, (0, 3) -> 0.3156689815062706, (3, 6) -> 0.0, (4, 5) -> 0.0, (7, 5) -> 0.0, (0, 5) -> 0.0, (1, 4) -> 0.3380960040434874, (4, 3) -> 0.5374221745636376, (9, 1) -> 0.25687346668442246, (5, 1) -> 0.30729727886500907, (4, 1) -> 0.30275649991000037, (6, 6) -> 0.9225709548474068, (7, 2) -> 0.0, (7, 1) -> 0.23945637661130403, (4, 4) -> 0.25593561642796925, (9, 5) -> 15.31673617395077, (8, 2) -> 0.9208301439605265, (0, 0) -> 0.4928618721162713, (1, 2) -> 0.922570954847405, (5, 4) -> 0.39286677227546407, (9, 4) -> 0.2165275056746543, (9, 0) -> 0.18336605919475368, (9, 3) -> 1.3857667539042406, (8, 6) -> 0.30057188386654526, (3, 4) -> 0.4650968080566947, (6, 0) -> 0.18973286612192558, (0, 4) -> 0.3309873489609752, (1, 6) -> 5.611511463176577, (3, 0) -> 0.3630260326804827)
    //    println(KMUtil.findMaxWeightSum(weight))
    //    WordSegUtil.loadDict("/Users/xiaonuo/Documents/project/test.txt")
    //    val test = WordSegUtil.process("冬装新款韩版松紧腰休闲百搭皮裤假两件显瘦打底裤")
    //    test.foreach(x => println(x))

    val pathPrefix = "test"
    val date = "2016-04-21"
    val pattern = """(\d{4}).*(\d{2}).*(\d{2})""".r
    val dateMeta = pattern.findFirstIn(date)
    if (dateMeta != None) {
      val pattern(year, month, day) = dateMeta.get
      println(s"${pathPrefix}/${year}${month}${day}")
    } else {
      println(s"${pathPrefix}")
    }
  }
}
