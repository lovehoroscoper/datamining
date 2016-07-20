package com.mgj.usergeneperfer

import java.text.SimpleDateFormat
import java.util.{Date, HashMap}

import com.mgj.utils.PartitionUtil
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaonuo on 11/26/15.
  */
object UserGenePrefer {
  val SPLITTER = "$"

  def main(args: Array[String]): Unit = {

    // args[0]: User base log.
    val conf = new SparkConf()
      .setAppName("calculate user gene prefer")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
 		sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 		sqlContext.setConf("fs.defaultFS","hdfs://mgjcluster")

    val bizdate = args(0)
    val bizdateSub1 = args(1)
    val bizdateSub30 = args(2)

    val geneMapDir = args(3)
    val geneMap = sc.textFile(geneMapDir).map(x => (x.split(" ")(0), x.split(" ")(1))).collect().toMap

    sqlContext.udf.register("to_gene", (itemId: String) => {
      if (geneMap.contains(itemId)) {
        geneMap.get(itemId).get
      } else {
        "-1"
      }
    })

    PartitionUtil.checkAppLog(sqlContext, bizdate, "click")
    PartitionUtil.checkAppLog(sqlContext, bizdate, "order")
    val sampleOrderSql = "select user_id, to_gene(item_id), time from s_dg_user_base_log where pt = '" + bizdate + "' and action_type = 'order' and platform_type = 'app'"
    val sampleSql = "select user_id, to_gene(item_id), time from s_dg_user_base_log where pt = '" + bizdate + "' and action_type = 'click' and platform_type = 'app'"
    val clickSql = "select user_id, to_gene(item_id), time from s_dg_user_base_log where pt >= '" + bizdateSub30 + "' and pt <= '" + bizdateSub1 + "' and action_type = 'click' and platform_type = 'app'"
    val orderSql = "select user_id, to_gene(item_id), time from s_dg_user_base_log where pt >= '" + bizdateSub30 + "' and pt <= '" + bizdateSub1 + "' and action_type = 'order'"
    val addCartSql = "select user_id, to_gene(item_id), time from s_dg_user_base_log where pt >= '" + bizdateSub30 + "' and pt <= '" + bizdateSub1 + "' and action_type = 'add_cart'"
    val favorSql = "select user_id, to_gene(item_id), time from s_dg_user_base_log where pt >= '" + bizdateSub30 + "' and pt <= '" + bizdateSub1 + "' and action_type = 'favor'"

    println("get sample order sql:")
    println("{" + sampleOrderSql + "}")
    println("get sample sql:")
    println("{" + sampleSql + "}")
    println("get click log sql:")
    println("{" + clickSql + "}")
    println("get order log sql:")
    println("{" + orderSql + "}")
    println("get add cart log sql:")
    println("{" + addCartSql + "}")
    println("get favor log sql:")
    println("{" + favorSql + "}")

    //    val bizdateSub1 = "2015-11-25"
    //    val sampleOrderSql = "select user_id, shop_id, time from s_dg_user_base_log where pt = '2015-11-26' and action_type = 'order' and platform_type = 'app' limit 10000"
    //    val sampleSql = "select user_id, shop_id, time from s_dg_user_base_log where pt = '2015-11-26' and action_type = 'click' and platform_type = 'app' limit 10000"
    //    val clickSql = "select user_id, shop_id, time from s_dg_user_base_log where pt >= '2015-11-23' and pt <= '2015-11-25' and action_type = 'click' and platform_type = 'app' limit 10000"
    //    val orderSql = "select user_id, shop_id, time from s_dg_user_base_log where pt >= '2015-11-23' and pt <= '2015-11-25' and action_type = 'order' limit 10000"
    //    val addCartSql = "select user_id, shop_id, time from s_dg_user_base_log where pt >= '2015-11-23' and pt <= '2015-11-25' and action_type = 'add_cart' limit 10000"
    //    val favorSql = "select user_id, shop_id, time from s_dg_user_base_log where pt >= '2015-11-23' and pt <= '2015-11-25' and action_type = 'favor' limit 10000"

    val userBaseLogClick = sqlContext.sql(clickSql)
    val userBaseLogOrder = sqlContext.sql(orderSql)
    val userBaseLogAddCart = sqlContext.sql(addCartSql)
    val userBaseLogFavor = sqlContext.sql(favorSql)
    val userBaseLogSample = sqlContext.sql(sampleSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString))
    val userBaseLogSampleOrder = sqlContext.sql(sampleOrderSql).rdd.filter(x => x.anyNull == false).map(x => (x(0).toString, x(1).toString, x(2).toString))

    def getFeature(iterable: Iterable[(String, String, String)]): Array[Double] = {
      val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val dateCurrent = new SimpleDateFormat("yyyy-MM-dd").parse(bizdateSub1)
      val feature: HashMap[Integer, Double] = new HashMap[Integer, Double]()
      for (log <- iterable) {
        val date = df.parse(log._3)
        val diff: Double = Math.ceil(1.0 * (dateCurrent.getTime - date.getTime) / (60 * 60 * 1000 * 24))
        if (!feature.containsKey(diff.toInt)) {
          feature.put(diff.toInt, 0d)
        }
        feature.put(diff.toInt, feature.get(diff.toInt) + 1d)
      }
      val featureArray: Array[Double] = new Array[Double](30)
      for (i <- 0 to 29) {
        if (feature.containsKey(i)) {
          featureArray(i) = feature.get(i)
        } else {
          featureArray(i) = 0
        }
      }
      return featureArray
    }

    def buildSample(sampleLog: RDD[(String, String, String)], feature: RDD[(String, String, Vector)]): RDD[(Vector, Double)] = {
      val sample = feature.map(x => (x._1 + x._2, x._3)).leftOuterJoin(sampleLog.map(x => (x._1 + x._2, 1d))).map(x => {
        if (x._2._2 == None) {
          (x._2._1, 0d)
        } else {
          (x._2._1, 1d)
        }
      })

      return sample
    }

    def buildFeature(userBaseLogClick: DataFrame, userBaseLogOrder: DataFrame): RDD[(String, String, Vector)] = {
      val clickRawFeature = userBaseLogClick.rdd.filter(x => x.anyNull == false && x(1).toString != "-1").map(x => (x(0).toString, x(1).toString, x(2).toString)).groupBy(x => x._1 + "_" + x._2).map(x => (x._1, getFeature(x._2)))

      val orderRawFeature = userBaseLogOrder.rdd.filter(x => x.anyNull == false && x(1).toString != "-1").map(x => (x(0).toString, x(1).toString, x(2).toString)).groupBy(x => x._1 + "_" + x._2).map(x => (x._1, getFeature(x._2)))

      val addCartRawFeature = userBaseLogAddCart.rdd.filter(x => x.anyNull == false && x(1).toString != "-1").map(x => (x(0).toString, x(1).toString, x(2).toString)).groupBy(x => x._1 + "_" + x._2).map(x => (x._1, getFeature(x._2)))

      val favorRawFeature = userBaseLogFavor.rdd.filter(x => x.anyNull == false && x(1).toString != "-1").map(x => (x(0).toString, x(1).toString, x(2).toString)).groupBy(x => x._1 + "_" + x._2).map(x => (x._1, getFeature(x._2)))

      def joinFeature(arrayLeft: Option[Array[Double]], arrayRight: Option[Array[Double]], offSet: Int, N: Int): Array[Double] = {
        val featureArray: Array[Double] = new Array[Double](N)
        if (arrayLeft != None) {
          val array = arrayLeft.get
          val size = array.size - 1
          for (x <- 0 to size) {
            featureArray.update(x, array.apply(x))
          }
        }

        if (arrayRight != None) {
          val array = arrayRight.get
          val size = array.size - 1
          for (x <- 0 to size) {
            featureArray.update(x + offSet, array.apply(x))
          }
        }

        return featureArray
      }

      val allFeaturePartA = clickRawFeature.fullOuterJoin(orderRawFeature).map(x => (x._1, joinFeature(x._2._1, x._2._2, 30, 60)))
      val allFeaturePartB = addCartRawFeature.fullOuterJoin(favorRawFeature).map(x => (x._1, joinFeature(x._2._1, x._2._2, 30, 60)))
      val allFeature = allFeaturePartA.fullOuterJoin(allFeaturePartB).map(x => (x._1.split("_")(0), x._1.split("_")(1), Vectors.dense(joinFeature(x._2._1, x._2._2, 60, 120))))
      return allFeature
    }

    val allFeature = buildFeature(userBaseLogClick, userBaseLogOrder)

    val sample = buildSample(userBaseLogSample, allFeature)
    val sampleOrder = buildSample(userBaseLogSampleOrder, allFeature)

    val ratioCount = sample.map(x => (x._2.toDouble, 1d)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    val ratio = ratioCount._1 / ratioCount._2

    val ratioCountOrder = sampleOrder.map(x => (x._2.toDouble, 1d)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    val ratioOrder = ratioCountOrder._1 / ratioCountOrder._2

    println("total sample count: " + ratioCount._2)
    println("total positive sample count: " + ratioCount._1)
    println("positive negtive sample ratio: " + ratio)

    println("total sample order count: " + ratioCountOrder._2)
    println("total positive sample order count: " + ratioCountOrder._1)
    println("positive negtive sample order ratio: " + ratioOrder)

    val posSample = sample.filter(x => x._2 > 0.5)
    val negSample = sample.filter(x => x._2 < 0.5).sample(false, ratio)

    val sampleFinal = posSample.union(negSample).map(x => Row(x._1.toString, x._2))
    println("sample count after sampling: " + sampleFinal.count)

    val posSampleOrder = sampleOrder.filter(x => x._2 > 0.5)
    val negSampleOrder = sampleOrder.filter(x => x._2 < 0.5).sample(false, ratio)

    val sampleOrderFinal = posSampleOrder.union(negSampleOrder).map(x => Row(x._1.toString, x._2))
    println("sample order count after sampling: " + sampleOrderFinal.count)

    val schema =
      StructType(
        StructField("feature", StringType, true)
          :: StructField("label", DoubleType, true)
          :: Nil)

    val sampleDF: DataFrame = sqlContext.createDataFrame(sampleFinal, schema)
    //    sampleDF.registerTempTable("s_dg_user_gene_prefer_sample_temp")
    //    sqlContext.sql("drop table if exists s_dg_user_gene_prefer_sample")
    //    sqlContext.sql("create table s_dg_user_gene_prefer_sample as select * from s_dg_user_gene_prefer_sample_temp")
    sqlContext.sql("drop table if exists s_dg_user_gene_prefer_sample")
    sampleDF.write.saveAsTable("s_dg_user_gene_prefer_sample")

    val sampleDFOrder: DataFrame = sqlContext.createDataFrame(sampleOrderFinal, schema)
    //    sampleDFOrder.registerTempTable("s_dg_user_gene_prefer_order_sample_temp")
    //    sqlContext.sql("drop table if exists s_dg_user_gene_prefer_order_sample")
    //    sqlContext.sql("create table s_dg_user_gene_prefer_order_sample as select * from s_dg_user_gene_prefer_order_sample_temp")
    sqlContext.sql("drop table if exists s_dg_user_gene_prefer_order_sample")
    sampleDFOrder.write.saveAsTable("s_dg_user_gene_prefer_order_sample")
  }
}
