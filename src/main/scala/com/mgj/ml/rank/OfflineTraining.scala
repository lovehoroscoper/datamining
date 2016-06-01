package com.mgj.ml.rank

import com.mgj.feature.FeatureCalculatorFactory
import com.mgj.utils.{TimeUtil, LRLearnerV2, SampleV2Util}
import com.mogujie.algo.model.client.main.ModelClientFactory
import org.apache.spark.sql.types.{StructType, DoubleType, StructField}
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

    val code = args(0)
    val sampleTable = args(1)
    val featureTable = args(2)
    val bizdate = args(3)
    val features = args(4)
    val stage = args(5)
    val N = args(6).toInt

    println(s"code:${code}")
    println(s"sampleTable:${sampleTable}")
    println(s"bizdate:${bizdate}")
    println(s"features:${features}")
    println(s"stage:${stage}")
    println(s"N:${N}")

    val stageSet = stage.split(",").toSet
    val modelClient = ModelClientFactory.getClient
    println(s"${modelClient.getModel("DIGU_MODEL")}")

    TimeUtil.start
    execute()
    TimeUtil.end
    println(s"execute time cost:${TimeUtil.timeCost}")

    def execute(): Unit = {
      if (stageSet.contains("build_sample")) {
        val clickSampleDF = SampleV2Util.getClickSample(sqlContext, bizdate, code.split(","): _*)
        clickSampleDF.show()
        val orderSampleDF = SampleV2Util.getOrderSample(sqlContext, bizdate, code.split(","): _*)
        orderSampleDF.show
        var allSampleDF = clickSampleDF
        for (i <- 1 to N) {
          allSampleDF = allSampleDF.unionAll(orderSampleDF)
        }

        allSampleDF.registerTempTable(s"${sampleTable}_temp")
        sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
        sqlContext.sql(s"drop table if exists ${sampleTable}")
        sqlContext.sql(s"create table ${sampleTable} as select * from ${sampleTable}_temp")
        clickSampleDF.unpersist(blocking = false)
        orderSampleDF.unpersist(blocking = false)
        allSampleDF.unpersist(blocking = false)
      }

      if (stageSet.contains("adapt_features")) {
        var dataDF: DataFrame = sqlContext.sql("select * from " + sampleTable).cache()
        dataDF.show()

        for (feature <- features.split(",")) {
          if (featureCalculatorFactory.containsCalculator(feature)) {
            val calculator = featureCalculatorFactory.getCalculator(feature)
            calculator.setBizDate(bizdate)
            println(calculator)
            dataDF = calculator.getFeatureDF(dataDF, sc, sqlContext).cache()
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
        dataDF.unpersist(blocking = false)
      }

      if (stageSet.contains("train")) {
        sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
        val dataDF = sqlContext.sql(s"select ${features},label from ${featureTable}").cache()
        val learner: LRLearnerV2 = new LRLearnerV2()
        val model = learner.run(sc, dataDF)

        //        val featureNameMap = Map()
        //        val featureColumns = dataDF.columns.filter(!_.equals("label"))
        //        val featureWeights = featureColumns.zip(model.coefficients.toArray.toList).toMap
      }
    }
  }
}
