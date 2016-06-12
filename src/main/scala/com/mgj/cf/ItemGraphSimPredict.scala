package com.mgj.cf

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.mgj.utils.LRLearner
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._

/**
  * Created by xiaonuo on 1/30/16.
  */
object ItemGraphSimPredict {
  val SPLITTER = "#"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("calculate item cf score")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("spark.cores.max", "32")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // User click log: user_id, item_id, visit_time.
    val itemSimResultPath = args(0)
    val itemSimModel = args(1)

    val model = sc.objectFile[LogisticRegressionModel](itemSimModel).first()
    println("model")
    println(model)
    println(model.coefficients)

    sqlContext.udf.register("to_vector", (vector: String) => (Vectors.parse(vector)))
    sqlContext.udf.register("to_double", (label: String) => (label.toDouble))
    val sampleFeature = sqlContext.sql("select itemx, itemy, to_vector(feature) as feature from s_dg_item_sim_feature")
    val isSameCategory = udf { (vector: Vector) => if (vector.apply(6) == 1.0) true else false }
    val sampleFeatureSameCategory = sampleFeature.filter(isSameCategory(sampleFeature("feature")))
    sampleFeatureSameCategory.show()
    val learner = new LRLearner()
    val result = learner.predict(model, sampleFeatureSameCategory, "itemx", "itemy")

    def sort(x: Iterable[(String, String, Double)], N: Int): String = {
      val list = x.toList.sortWith((a, b) => a._3.compareTo(b._3) > 0).take(N).map(x => x._2 + ":" + Math.round(x._3 * 100000)).mkString(",")
      return list
    }

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_MONTH, -1)

    result.map(x => (x(0), x(1), x(2).toDouble)).groupBy(_._1).map(x => x._1 + " " + sort(x._2, 50)).saveAsTextFile(itemSimResultPath + "/" + sdf.format(calendar.getTime))

    //    val resultDiff = learner.predict(model, sampleFeature, "itemx", "itemy")

    val schema =
      StructType(
        StructField("itemx", StringType, true) ::
          StructField("itemy", StringType, true) ::
          StructField("sim_score", StringType, true) ::
          Nil)

    //    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    val resultDF = sqlContext.createDataFrame(result.map(x => (x(0), x(1), x(2).toString)).map(x => Row(x._1, x._2, x._3)), schema)
    result.unpersist(blocking = false)
    //    resultDF.registerTempTable("s_dg_item_sim_merge_temp")
    //    sqlContext.sql("drop table if exists s_dg_item_sim_merge")
    //    sqlContext.sql("create table s_dg_item_sim_merge as select * from s_dg_item_sim_merge_temp")
    sqlContext.sql("drop table if exists s_dg_item_sim_merge")
    resultDF.write.saveAsTable("s_dg_item_sim_merge")
  }
}
