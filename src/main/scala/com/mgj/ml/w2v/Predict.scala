package com.mgj.ml.w2v

import java.util
import org.apache.spark.sql.hive.HiveContext

import scala.collection.JavaConversions._
import breeze.linalg._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by xiaonuo on 12/10/15.
 */
object Predict {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("cluster predict")
      .set("spark.cores.max", "28")

    // spark context
    val sc: SparkContext = new SparkContext(conf)

    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val modelStr = args(0)
    val clusterStr = args(1)
    val resultStr = args(2)
    val entityType = args(3)
    val isScored = args(4).toBoolean
    val isSameCategory = args(5).toBoolean

    //    val modelStr = "/user/digu/itemWord2VecModel"
    //    val clusterStr = "/user/digu/itemCluster"
    //    val resultStr = "/user/digu/temp"
    //    val entityType = "item_id"
    //    val isScored = false
    //    val isSameCategory = false
    var schema = "tradeitemid"
    if (entityType.equals("shop_id")) {
      schema = "shopid"
    }
    val shopInfo = sqlContext.sql("select distinct " + schema + ", cid from v_dw_trd_tradeitem").rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString)).groupByKey().map(x => (x._1, x._2.toSet))

    val cluster = sc.textFile(clusterStr).map(x => (x.split(" ")(0), x.split(" ")(1))).cache
    val model = sc.textFile(modelStr).map(x => (x.split(":")(0), x.split(":")(1))).cache

    val entity = cluster.join(model.join(shopInfo).map(x => (x._1, x._2))).map(x => (x._2._1, (x._1, DenseVector(x._2._2._1.split(" ").map(_.toDouble)), x._2._2._2))).map(x => (x._1, (x._2._1, x._2._2 / x._2._2.norm, x._2._3))).repartition(500).cache
    val count = entity.map(x => (x._1, 1)).reduceByKey((a, b) => a + b)
    println("entity count: " + entity.count)
    println("entity count max: " + count.max)
    println("entity count min: " + count.min)

    def getSim(clusterId: String, iterable: Iterable[(String, DenseVector[Double], Set[String])], N: Int): List[String] = {
      val data = iterable.toList
      val list: util.ArrayList[(String, String, Double)] = new util.ArrayList[(String, String, Double)]()
      for (x <- data) {
        for (y <- data) {
          val setInter = x._3 & y._3
          val setUnion = x._3 | y._3
          if (!x._1.equals(y._1) && (1.0 * setInter.size / setUnion.size >= 0.5 || !isSameCategory)) {
            list.add(x._1, y._1, x._2.dot(y._2))
          }
        }
      }
      if (isScored) {
        return list.groupBy(x => x._1).map(x => (x._1, x._2.sortWith((a, b) => a._3 > b._3).map(x => x._2 + ":" + x._3.toDouble).take(N))).map(x => x._1 + " " + x._2.mkString(" ")).toList
      } else {
        return list.groupBy(x => x._1).map(x => (x._1, x._2.sortWith((a, b) => a._3 > b._3).map(x => x._2).take(N))).map(x => x._1 + " " + x._2.mkString(" ")).toList
      }
    }

    val result = entity.groupByKey().map(x => getSim(x._1, x._2, 20)).filter(x => x.size > 0).flatMap(x => x).repartition(500)
    result.take(10).foreach(println)
    result.saveAsTextFile(resultStr)
  }
}
