package com.mgj.userprefer

import com.mgj.feature.FeatureType
import com.mgj.utils.{HiveUtil, HdfsUtil, PartitionUtil}
import org.apache.commons.lang3.Validate
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 8/3/16.
  */
object UserPrefer {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("calculate user prefer")
      .set("spark.cores.max", "28")

    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlContext.setConf("fs.defaultFS", "hdfs://mgjcluster")

    VariableFactory.init(args)

    PartitionUtil.checkAppLog(sqlContext, VariableFactory.bizdate, "click")
    PartitionUtil.checkAppLog(sqlContext, VariableFactory.bizdate, "order")

    if (HdfsUtil.isExists(sc, VariableFactory.entityMapPath)) {
      val entityMap = sc.textFile(VariableFactory.entityMapPath).map(x => (x.split(" ")(0), x.split(" ")(1))).collect().toMap
      sqlContext.udf.register("to_entity", (itemId: String) =>
        if (entityMap.contains(itemId)) {
          entityMap.get(itemId).get
        } else {
          "-1"
        }
      )
      Validate.notBlank(VariableFactory.entityFeatureName)
      Validate.notBlank(VariableFactory.entityTableName)
      HiveUtil.featureHdfsToHive(sc, sqlContext, VariableFactory.entityFeatureName, VariableFactory.entityMapPath, VariableFactory.sdf.format(VariableFactory.calendar.getTime), VariableFactory.entityTableName, FeatureType.ITEM)
    }

    val feature = VariableFactory.userPreferProcessor.buildFeature(sc, sqlContext, VariableFactory.bizdateSubA, VariableFactory.bizdateSubB, VariableFactory.entity, VariableFactory.featureTypeList: _*)

    var i = 0
    for (sampleType <- VariableFactory.sampleTypeList) {
      val sample = VariableFactory.userPreferProcessor.buildSampleV2(sc, sqlContext, feature, VariableFactory.bizdate, VariableFactory.entity, sampleType)
      sqlContext.sql(s"drop table if exists ${VariableFactory.sampleList.apply(i)}")
      sample.write.saveAsTable(VariableFactory.sampleList.apply(i))
      sample.unpersist(blocking = false)
      i += 1
    }
  }
}
