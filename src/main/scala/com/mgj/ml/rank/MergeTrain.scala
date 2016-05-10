package com.mgj.ml.rank

import com.mgj.utils.LRLearnerV2
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaonuo on 12/21/15.
  */
object MergeTrain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("offline trainning")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("es.read.metadata", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new HiveContext(sc)

    val features = args(0)

    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    val dataDF = sqlContext.sql(s"select ${features},label from test_sample").cache()

    val learner: LRLearnerV2 = new LRLearnerV2()
    learner.run(sc, dataDF)

    sc.stop()
  }
}
