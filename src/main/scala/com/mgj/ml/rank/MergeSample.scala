package com.mgj.ml.rank

import com.mgj.feature.FeatureCalculatorFactory
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
  * Created by xiaonuo on 12/17/15.
  */
object MergeSample {
  val context: ApplicationContext = new ClassPathXmlApplicationContext("dataMiningContext.xml")
  val featureCalculatorFactory: FeatureCalculatorFactory = context.getBean(classOf[FeatureCalculatorFactory])

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("offline trainning")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("es.read.metadata", "true")

    // Spark context.
    val sc: SparkContext = new SparkContext(conf)

    // Hive context.
    val sqlContext: HiveContext = new HiveContext(sc)

    val bizDate = args(0)
    val features = args(1)
    val sample = args(2)

    //    val bizDate = "2015-12-20"
    var dataDF: DataFrame = sqlContext.sql("select * from " + sample).cache()
    dataDF.show()

    //    val itemCtrFeatureCalculator = new ItemCtrFeatureCalculator()
    //    itemCtrFeatureCalculator.setBizDate(bizDate)
    //    itemCtrFeatureCalculator.setFeatureName("item_ctr")
    //    itemCtrFeatureCalculator.setItemField("itemCtr")
    //    itemCtrFeatureCalculator.setItemFieldPath("/user/digu/LTR_FEATURE/old_ctr_score_sub")
    //    itemCtrFeatureCalculator.setUserFieldPath("")
    //    itemCtrFeatureCalculator.setUserField("")
    //    val dataDFTemp = itemCtrFeatureCalculator.getFeatureDF(dataDF, sc, sqlContext)
    //    itemCtrFeatureCalculator.compute(dataDFTemp, sc, sqlContext)

    for (feature <- features.split(",")) {
      val calculator = featureCalculatorFactory.getCalculator(feature)
      calculator.setBizDate(bizDate)
      println(calculator)
      dataDF = calculator.getFeatureDF(dataDF, sc, sqlContext)
      dataDF = calculator.compute(dataDF, sc, sqlContext).cache()
    }

    val columns = features + ",pos,label"
    dataDF = dataDF.select(columns.split(",").map(x => new Column(x)): _*)
    dataDF.show()

    val dataDFRDD = dataDF.rdd.filter(x => x.anyNull == false).map(x => {
      val list: List[Double] = columns.split(",").toList.map(name => x.getAs[String](name).toDouble)
      Row(list: _*)
    })
    val schema = StructType(columns.split(",").map(x => StructField(x, DoubleType, true)))
    dataDF = sqlContext.createDataFrame(dataDFRDD, schema)

    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
    dataDF.registerTempTable("test_sample_temp")
    sqlContext.sql("drop table if exists test_sample")
    sqlContext.sql("create table test_sample as select * from test_sample_temp")

    sc.stop()
  }
}
