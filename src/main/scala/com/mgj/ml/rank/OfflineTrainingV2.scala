package com.mgj.ml.rank

import com.mgj.feature.{UdfFactory, FeatureConstructor, FeatureCalculatorFactory}
import com.mgj.utils.{LRLearnerV2, SampleV2Util, TimeUtil}
import com.mogujie.algo.model.client.main.ModelClientFactory
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
  * Created by xiaonuo on 5/10/16.
  */
object OfflineTrainingV2 {
  val context: ApplicationContext = new ClassPathXmlApplicationContext("dataMiningContext.xml")
  val featureCalculatorFactory: FeatureCalculatorFactory = context.getBean(classOf[FeatureCalculatorFactory])
  val udfFactory: UdfFactory = context.getBean(classOf[UdfFactory])

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("offline training")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.maxResultSize", "2g")

    conf.registerKryoClasses(Array(classOf[Tuple2[String, List[String]]]))

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlContext.setConf("fs.defaultFS", "hdfs://mgjcluster")

    val code = args(0)
    val sampleTable = args(1)
    val featureTable = args(2)
    val bizdate = args(3)
    val features = args(4)
    val stage = args(5)
    val N = args(6).toInt
    val modelName = args(7)

    println(s"code:${code}")
    println(s"sampleTable:${sampleTable}")
    println(s"bizdate:${bizdate}")
    println(s"features:${features}")
    println(s"stage:${stage}")
    println(s"N:${N}")
    println(s"modelName:${modelName}")

    val stageSet = stage.split(",").toSet
    val modelClient = ModelClientFactory.getClient
    modelClient.init()
    println(s"${modelClient.getModel(modelName)}")

    TimeUtil.start
    execute()
    TimeUtil.end
    println(s"execute time cost:${TimeUtil.timeCost}")

    def execute(): Unit = {
      if (stageSet.contains("build_sample")) {
        val clickSampleDF = SampleV2Util.getClickSample(sqlContext, bizdate, true, code.split(","): _*)
        val orderSampleDF = SampleV2Util.getOrderSample(sqlContext, bizdate, true, code.split(","): _*)
        var allSampleDF = clickSampleDF
        for (i <- 1 to N) {
          allSampleDF = allSampleDF.unionAll(orderSampleDF)
        }

        sqlContext.sql(s"drop table if exists ${sampleTable}")
        allSampleDF.write.saveAsTable(s"${sampleTable}")
        allSampleDF.unpersist(blocking = false)
      }

      if (stageSet.contains("adapt_features")) {
        var dataDF: DataFrame = sqlContext.sql("select * from " + sampleTable).cache()
        FeatureConstructor.init(sqlContext, udfFactory)
        dataDF = FeatureConstructor.construct(sc, sqlContext, dataDF, featureCalculatorFactory, bizdate, features.split(","): _*)
        dataDF.show()
        sqlContext.sql(s"drop table if exists ${featureTable}")
        dataDF.write.saveAsTable(s"${featureTable}")
      }

      if (stageSet.contains("train")) {
        val dataDF = sqlContext.sql(s"select ${features} from ${featureTable}").cache()
        val learner: LRLearnerV2 = new LRLearnerV2()
        val model = learner.run(sc, dataDF)

        val featureColumns = dataDF.columns.filter(x => !x.equals("label") && !x.equals("pos")).map(x => FeatureNameMapper.getName(x))
        val featureWeights = featureColumns.zip(model.coefficients.toArray.toList).toMap
        println("feature weights")
        //        for (e <- featureWeights) {
        //          println(s"feature weight:${e}")
        //          modelClient.setWeight(modelName, e._1, e._2)
        //        }
        //        modelClient.synModel(modelName)
      }
    }
  }
}
