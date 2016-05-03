package com.mgj.usershopprefer

import java.util
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._

/**
 * Created by xiaonuo on 12/14/15.
 */
object GuaranteeDelivery {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("calculate user shop prefer")
      .set("spark.cores.max", "28")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)
    val input = args(4)
    val output = args(5)

    val userShopPrefer = sc.textFile(input).map(x => (x.split(" ")(0), x.split(" ")(1).split(",").toList.map(x => {
      (x.split(":")(0), x.split(":")(1).toDouble / 100000)
    }).toMap)).sample(false, 0.001).cache
    val userNum = userShopPrefer.count

    val iterMax = args(0).toInt
    val beta = args(1).toDouble
    val pitNum = args(2).toInt
    val mu = userNum / args(3).toDouble

    //    val iterMax = 10
    //    val beta = 1
    //    val pitNum = 1000
    //    val mu = userNum/1000.0
    println("iterMax:" + iterMax)
    println("beta:" + beta)
    println("pitNum:" + pitNum)
    println("mu:" + mu)

    // (F, FLimit, lambda, lambdaSup)
    var shopDelivery = userShopPrefer.map(x => x._2.keySet).flatMap(x => x).distinct().map(x => (x, (0d, 0d, 0d, 0d))).cache

    val shopNum = shopDelivery.count
    println("user count: " + userNum)
    println("shop count: " + shopNum)
    println("shop delivery")

    shopDelivery = shopDelivery.map(x => {
      if (x._1.equals("111319")) {
        (x._1, (0d, 5.0d / (shopNum + 5), 0d, 0d))
      } else {
        (x._1, (0d, 1.0d / (shopNum + 5), 0d, 0d))
      }
    })
    shopDelivery.take(10).foreach(println)

    val t: util.ArrayList[Double] = new util.ArrayList[Double]()
    t.add(0)
    t.add(0)

    // calculate z
    // shopid, (cij, FLimit, lambda, lambdaSup)
    val userShopPreferFlat = userShopPrefer.map(x => {
      val map: util.HashMap[String, Double] = new util.HashMap[String, Double]()
      for (entry <- x._2) {
        map.put(x._1 + "_" + entry._1, entry._2)
      }
      map.toMap
    }).flatMap(x => x)

    for (i <- 1 to iterMax) {

      println("iteration: " + i)

      val z = userShopPreferFlat.map(x => (x._1.split("_")(1), x)).join(shopDelivery).map(x => (x._2._1._1, (x._2._1._2, x._2._2._2, x._2._2._3, x._2._2._4))).map(x => (x._1, (Math.exp((x._2._1 + x._2._3) / beta), x._2._2, x._2._3, x._2._4)))

      // calculate a
      val a = z.map(x => (x._1.split("_")(0), (x._1.split("_")(1), x._2))).groupByKey().map(x => {
        var norm = 0d
        val map: util.HashMap[String, (Double, Double, Double, Double)] = new util.HashMap[String, (Double, Double, Double, Double)]()
        for (t <- x._2) {
          norm += t._2._1
        }

        for (t <- x._2) {
          map.put(t._1, (t._2._1 / norm, t._2._2, t._2._3, t._2._4))
        }
        map.toMap
      })

      // calculate flow
      val F = a.flatMap(x => x).reduceByKey((x, y) => (x._1 + y._1, x._2, x._3, x._4))
      val sumF = F.map(x => x._2._1).reduce(_ + _)
      val FNorm = F.map(x => (x._1, (x._2._1 / sumF, x._2._2, x._2._3, x._2._4)))

      val err = FNorm.map(x => Math.abs(x._2._1 - x._2._2)).reduce(_ + _)
      println("error: " + err)

      val tk = (1 + Math.sqrt(4 * t.get(i) * t.get(i) + 1)) / 2
      t.add(tk)

      shopDelivery = FNorm.map(x => {
        var lambda = x._2._3
        val F = x._2._1
        val FLimit = x._2._2
        var lambdaSup = lambda - mu * (F - FLimit)
        if (lambdaSup <= 0d) {
          lambdaSup = 0d
        }

        val lambdaSupPre = x._2._4
        lambda = lambdaSup + t.get(i - 1) / t.get(i + 1) * (lambdaSup - lambdaSupPre)

        (x._1, (F, FLimit, lambda, lambdaSup))
      })
    }

    shopDelivery.take(10)
    shopDelivery.map(x => x._1 + ":" + x._2._1 + " " + x._2._2 + " " + x._2._3 + " " + x._2._4).saveAsTextFile(output)
  }

}
