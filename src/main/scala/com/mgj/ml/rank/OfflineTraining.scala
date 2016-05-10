package com.mgj.ml.rank

import com.mgj.feature.FeatureCalculatorFactory
import com.mgj.utils.{LRLearnerV2, SampleV2Util}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, Column, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
  * Created by xiaonuo on 5/10/16.
  */
object OfflineTraining {
  val context: ApplicationContext = new ClassPathXmlApplicationContext("dataMiningContext.xml")
  val featureCalculatorFactory: FeatureCalculatorFactory = context.getBean(classOf[FeatureCalculatorFactory])

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("offline training").
      set("spark.sql.parquet.binaryAsString", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val appIds = args(0)
    val sampleTable = args(1)
    val featureTable = args(2)
    val bizdate = args(3)
    val features = args(4)
    val stage = args(5)

    println(s"appIds:${appIds}")
    println(s"sampleTable:${sampleTable}")
    println(s"bizdate:${bizdate}")
    println(s"features:${features}")
    println(s"stage:${stage}")

    val stageSet = stage.split(",").toSet

    if (stageSet.contains("build_sample")) {
      val clickSampleDF = SampleV2Util.getClickSample(sqlContext, bizdate, appIds.split(","): _*)
      clickSampleDF.show()
      val orderSampleDF = SampleV2Util.getOrderSample(sqlContext, bizdate, appIds.split(","): _*)
      orderSampleDF.show

      var allSampleDF = clickSampleDF
      for (i <- 1 to 5) {
        allSampleDF = allSampleDF.unionAll(orderSampleDF)
      }

      allSampleDF.registerTempTable(s"${sampleTable}_temp")
      sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
      sqlContext.sql(s"drop table if exists ${sampleTable}")
      sqlContext.sql(s"create table ${sampleTable} as select * from ${sampleTable}_temp")
    }

    if (stageSet.contains("adapt_features")) {
      var dataDF: DataFrame = sqlContext.sql("select * from " + sampleTable).cache()
      dataDF.show()

      for (feature <- features.split(",")) {
        if (featureCalculatorFactory.containsCalculator(feature)) {
          val calculator = featureCalculatorFactory.getCalculator(feature)
          calculator.setBizDate(bizdate)
          println(calculator)
          dataDF = calculator.getFeatureDF(dataDF, sc, sqlContext)
          dataDF = calculator.compute(dataDF, sc, sqlContext).cache()
        } else {
          println(s"feature calculator ${feature} dose not exists")
        }
      }

      val columns = features + ",label"
      dataDF = dataDF.select(columns.split(",").map(x => new Column(x)): _*)
      dataDF.show()

      val dataDFRDD = dataDF.rdd.filter(x => x.anyNull == false).map(x => {
        val list: List[Double] = columns.split(",").toList.map(name => x.getAs[String](name).toDouble)
        Row(list: _*)
      })
      val schema = StructType(columns.split(",").map(x => StructField(x, DoubleType, true)))
      dataDF = sqlContext.createDataFrame(dataDFRDD, schema)

      sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
      dataDF.registerTempTable(s"${featureTable}_temp")
      sqlContext.sql(s"drop table if exists ${featureTable}")
      sqlContext.sql(s"create table ${featureTable} as select * from ${featureTable}_temp")
    }

    if (stageSet.contains("train")) {
      sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
      val dataDF = sqlContext.sql(s"select ${features},label from ${featureTable}").cache()
      val learner: LRLearnerV2 = new LRLearnerV2()
      learner.run(sc, dataDF)
    }
  }
}
