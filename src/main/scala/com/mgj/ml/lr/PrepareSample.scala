package com.mgj.ml.lr

//import com.mgj.common.{AlgoOfflineConstants, AlgoDataFactory, AlgoDFFactory}
//import com.mgj.utils.AlgoDateUtil
//import com.mogujie.algo.dc.client.AlgoDcClient
//import com.mogujie.algo.feature.factory.AlgoFeatureFactory
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.{SparkContext, SparkConf}
//import org.springframework.context.ApplicationContext
//import org.springframework.context.support.ClassPathXmlApplicationContext

/**
 * Created by xiaonuo on 12/2/15.
 */
object PrepareSample {

  //  val ctxPath = "dataMiningContext.xml"
  //  val appCtx: ApplicationContext = new ClassPathXmlApplicationContext(ctxPath)
  //  val algoDcClient: AlgoDcClient = appCtx.getBean(classOf[AlgoDcClient])
  //  val algoDFFactory: AlgoDFFactory = appCtx.getBean(classOf[AlgoDFFactory])
  //  val algoSampleFactory: AlgoDataFactory = appCtx.getBean(classOf[AlgoDataFactory])
  //  val algoFeatureFactory: AlgoFeatureFactory = appCtx.getBean(classOf[AlgoFeatureFactory])
  //
  //  def main(args: Array[String]): Unit = {
  //    val conf = new SparkConf()
  //      .setAppName("offline trainning")
  //      .set("spark.sql.parquet.binaryAsString", "true")
  //      .set("es.read.metadata", "true")
  //
  //    // Spark context.
  //    val sc: SparkContext = new SparkContext(conf)
  //
  //    // Hive context.
  //    val sqlContext: HiveContext = new HiveContext(sc)
  //
  //    val hiveTableName = "fengjian_book_sample_with_position"
  //    val sampleSql =
  //      "select userid, tradeitemid, cast(clicked as double) as clicked, cast(revisited as string) as revisited, " +
  //        "cast(is_ad as string) as is_ad, cast(position_ctr as string) as position_ctr " +
  //        "from fengjian_app_book_sample_with_position_2"
  //
  //    val userCol = "userid"
  //    val itemCol = "tradeitemid"
  //    val labelCol = "clicked"
  //    //    val labelCol ="label"
  //    //    val hiveTableName = "s_dg_click_sample"
  //    //    val sampleSql = "select cast(userid as bigint) as userid, cast(tradeitemid as bigint) as tradeitemid, cast(label as double) as label from s_dg_click_sample"
  //    val itemFeild: List[String] = TrainConstant.ITEM_FIELDS.split(",").toList
  //    var dataDF = algoSampleFactory.getDataDF(sc, hiveTableName, sampleSql, userCol, itemCol, labelCol, itemFeild)
  //    dataDF.show
  //
  //    // sample
  //    val posetiveSampleDF = dataDF.filter(s"${AlgoOfflineConstants.LABEL} > 0.5")
  //    val postiveCnt = posetiveSampleDF.count
  //    val totalCnt = dataDF.count
  //    val positiveRatio = postiveCnt.toDouble / totalCnt
  //    val nagetiveSampleDF = dataDF.filter(s"${AlgoOfflineConstants.LABEL} < 0.5").sample(false, positiveRatio)
  //    dataDF = posetiveSampleDF.unionAll(nagetiveSampleDF)
  //    println("dataDF schema after union")
  //    dataDF.show()
  //
  //    val primaryFeild: List[String] = List[String](AlgoOfflineConstants.UID, AlgoOfflineConstants.IID, AlgoOfflineConstants.LABEL)
  //    val schema: String = (primaryFeild ::: itemFeild).mkString(",")
  //
  //    dataDF = sqlContext.createDataFrame(dataDF.rdd.filter(x => x.anyNull == false)
  //      .map(x => Row(x.toSeq.map(x => x.toString): _*)), StructType(dataDF.schema.map(x => StructField(x.name, StringType, true))))
  //
  //    dataDF.repartition(500).registerTempTable("s_dg_lr_sample_temp")
  //    sqlContext.sql("set hive.metastore.warehouse.dir=/user/digu/warehouse")
  //    sqlContext.sql("drop table if exists s_dg_lr_sample")
  //    sqlContext.sql("create table s_dg_lr_sample as select " + schema + " from s_dg_lr_sample_temp")
  //    println("THE END")
  //  }
}
