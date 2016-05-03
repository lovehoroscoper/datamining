package com.mgj.ml.lr

import org.apache.spark.sql.Column
import com.mgj.feature.{FeatureConstant, FeatureCalculatorFactory}
import com.mgj.utils.LRLearnerV2
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
 * Created by xiaonuo on 12/4/15.
 */
object Train {

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

    var dataDF: DataFrame = sqlContext.sql("select * from s_dg_lr_sample")
    dataDF.show()

    val valFeild = dataDF.columns.filter(x => !x.equals(FeatureConstant.USER_KEY) && !x.equals(FeatureConstant.ITEM_KEY)).toList
    val dataDFRDD = dataDF.rdd.filter(x => x.anyNull == false).map(x => {
      val list: List[Double] = valFeild.map(name => x.getAs[String](name).toDouble)
      val uVal: String = x.getAs[String](FeatureConstant.USER_KEY).toString
      val iVal: String = x.getAs[String](FeatureConstant.ITEM_KEY).toString
      Row(uVal :: iVal :: list: _*)
    })
    val uField = StructField(FeatureConstant.USER_KEY, StringType, true)
    val iField = StructField(FeatureConstant.ITEM_KEY, StringType, true)
    val field = StructType(uField :: iField :: valFeild.map(x => StructField(x, DoubleType, true)))
    dataDF = sqlContext.createDataFrame(dataDFRDD, field)

    for (feature <- TrainConstant.USER_FIELDS.split(",")) {
      val calculator = featureCalculatorFactory.getCalculator(feature)
      println(calculator)
      dataDF = calculator.getFeatureDF(dataDF, sc, sqlContext)
      dataDF = calculator.compute(dataDF, sc, sqlContext)
    }

    dataDF = dataDF.select(TrainConstant.TRAIN_SCHEMA.split(",").map(x => new Column(x)): _*)
    dataDF.show()

    val learner: LRLearnerV2 = new LRLearnerV2()
    learner.run(sc, dataDF)
  }
}
