package com.mgj.ml.louvain

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xiaonuo on 12/22/15.
  */
object Grouping {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("calculate item graph")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("es.read.metadata", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    val inputDir = args(0)
    println("input dir: " + inputDir)

    val param = sc.textFile(inputDir + "/param").map(x => (x.split(":")(0), x.split(":")(1))).collect().toMap
    var modularity = sc.textFile(inputDir + "/level_0_vertices/").map(x => List(x.split(" ")(0), x.split(" ")(1)))
    for (i <- 1 to param.get("levelCounter").get.toInt - 1) {
      val temp = sc.textFile(inputDir + "/level_" + i + "_vertices/").map(x => (x.split(" ")(0), x.split(" ")(1)))
      modularity = modularity.map(x => (x(i), x)).join(temp).map(x => x._2._1 :+ x._2._2)
    }

    for (i <- 0 to param.get("levelCounter").get.toInt - 1) {
      println("level " + i + " count: " + modularity.map(x => x(i)).distinct().count())
    }

    //    val mapList = new util.HashMap[Int, Map[String, Int]]()
    //    for (i <- 1 to param.get("levelCounter").get.toInt - 1) {
    //      mapList.put(i, modularity.map(x => (x(i), 1)).reduceByKey((x, y) => x + y).collect().toMap)
    //    }

    val cluster = modularity.map(x => {
      var level = x(1)
      //      val clusterLevelOneCnt = mapList.get(1).get(x(1)).get
      //      if (clusterLevelOneCnt < 250) {
      //        for (i <- 2 to param.get("levelCounter").get.toInt - 1) {
      //          val clusterLevelCnt = mapList.get(i).get(x(i)).get
      //          if (clusterLevelCnt < 500) {
      //            level = x(i)
      //          }
      //        }
      //      }
      (x(0), x(1))
    }).cache()

    val clusterFilter = cluster.groupBy(x => x._2).map(x => (x._2, x._2.size)).filter(x => x._2 >= 20).map(x => x._1).flatMap(x => x)
    val outputDir = args(1)
    println("output dir: " + outputDir)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)

    clusterFilter.map(x => x._1 + " " + x._2).saveAsTextFile(outputDir + "/data/" + sdf.format(calendar.getTime))
    clusterFilter.map(x => (x._2, x._1)).reduceByKey((x, y) => (x + " " + y)).map(x => x._1 + ":" + x._2).saveAsTextFile(outputDir + "/index/" + sdf.format(calendar.getTime))
    clusterFilter.map(x => (x._2, x._1)).reduceByKey((x, y) => (x + " " + y)).map(x => x._1 + ":" + x._2).take(10).foreach(println)

    //    cluster.map(x => (x._2, 1)).reduceByKey((x, y) => x + y).filter(x => x._2 > 100).count
    //    modularity.map(x => (x(1), 1)).reduceByKey((x, y) => x + y).filter(x => x._2 > 100).count
    //    sc.textFile("/user/digu/itemGroupWithTitle/index").map(x => (x,x.split(":")(1).split(" ").size)).sortBy(x => x._2,false)
  }
}
