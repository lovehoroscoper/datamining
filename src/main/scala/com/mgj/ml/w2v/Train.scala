package com.mgj.ml.w2v

import java.text.SimpleDateFormat
import java.util

import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
 * Created by xiaonuo on 12/8/15.
 */
object Train {
  def main(args: Array[String]): Unit = {

    val modelFile = args(0)
    val clusterFile = args(1)
    val start = args(2)
    val end = args(3)
    val vectorSize = args(4).toInt
    val minCount = args(5).toInt
    val clusterNum = args(6).toInt
    val numPartitions = args(7).toInt
    val entityType = args(8)
    val bufferSize = args(9)

    val conf = new SparkConf()
      .setAppName("cluster train")
      .set("spark.core.connection.ack.wait.timeout", "640")
      .set("spark.akka.frameSize", "256")
      .set("spark.cores.max", "32")
      .set("spark.kryoserializer.buffer", bufferSize)

    // spark context
    val sc: SparkContext = new SparkContext(conf)

    // hive context
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val dataSql = "select user_id, " + entityType + ", time from s_dg_user_base_log where pt >= '" + start + "' and pt <= '" + end + "' and action_type = 'click' and platform_type = 'app'"
    println("dataSql:" + dataSql)

    def getSeq(iterable: Iterable[Row], N: Int): Seq[String] = {
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val sorted = iterable.toSeq.sortWith((a, b) => (sdf.parse(a(2).toString).getTime < sdf.parse(a(2).toString).getTime)).map(x => x(1).toString)
      val temp: util.ArrayList[String] = new util.ArrayList[String]()
      var w = "-1"
      sorted.foreach(x => {
        if (!w.equals(x)) {
          w = x
          temp.add(w)
        }
      })

      val interval = temp.size - N
      if (interval % 2 == 0) {
        return temp.slice((temp.size - N) / 2, temp.size - (temp.size - N) / 2)
      } else {
        return temp.slice((temp.size - N) / 2, temp.size - (temp.size - N) / 2 - 1)
      }
    }

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val interval = (sdf.parse(end).getTime - sdf.parse(start).getTime) / 1000 / 60 / 60 / 24 + 1
    val data = sqlContext.sql(dataSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x)).groupByKey().map(x => getSeq(x._2, interval.toInt * 50)).repartition(600).cache()
    val count = data.map(x => (x.size, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val max = data.map(x => x.size).max()
    val min = data.map(x => x.size).min()
    println("data total count: " + count._2)
    println("data average length: " + 1.0 * count._1 / count._2)
    println("data max length: " + max)
    println("data min length: " + min)
    println("start training")

    val w2v = new Word2Vec()
    w2v.setNumPartitions(numPartitions)
    w2v.setMinCount(minCount)
    w2v.setVectorSize(vectorSize)

    val model = w2v.fit(data).getVectors
    println("model:")
    model.take(10).map(x => x._1 + ":" + x._2.mkString(" ")).foreach(println)

    println("save model to: " + modelFile)
    sc.makeRDD(model.map(x => x._1 + ":" + x._2.mkString(" ")).toArray).saveAsTextFile(modelFile)

    val modelRDD = sc.textFile(modelFile).map(x => (x.split(":")(0), x.split(":")(1).split(" ").map(_.toDouble)))
    val itemList = modelRDD.map(x => x._1)
    val vector = modelRDD.filter(x => x._2.size > 0).map(x => Vectors.dense(x._2)).cache

    println("start clustering")
    val kMeans = new KMeans()
    kMeans.setEpsilon(1e-6)
    //    kMeans.setInitializationMode("k-means||")
    kMeans.setInitializationMode("random")
    kMeans.setK(clusterNum)
    val clusters = kMeans.run(vector)
    val clusterId = clusters.predict(vector)

    println("save cluster to: " + clusterFile)
    itemList.zip(clusterId).map(t => t._1 + " " + t._2.toString).saveAsTextFile(clusterFile)
  }
}
