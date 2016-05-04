
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by xiaonuo on 15/8/10.
  */
class Test extends FlatSpec with Matchers with LocalSparkContext {

  "practice" should "train" in {
    //    val test = new Practice()
    //    test.main(Array())
    //    val test = new SmoothTest()
    //    test.main(Array())
    val sqlContext = new SQLContext(sc)
    val left = sc.parallelize(Seq((100.0, 100.0, 100.0), (1000.0, 100.0, 100.0), (100.0, 100.0, 1000.0))).map(x => Row(x._1, x._2, x._3))
    left.take(10).foreach(println)
  }

  //  "FeatureBuilder test" should "ok" in {
  //    val sqlContext = new SQLContext(sc);
  //    val left = sc.parallelize(Seq((100.0, 100.0, 100.0), (1000.0, 100.0, 100.0), (100.0, 100.0, 1000.0))).map(x => Row(x._1, x._2, x._3))
  //    val schemaL = StructType(Seq(StructField("item_gmv_sum_0day_app", DoubleType, true),
  //      StructField("item_gmv_sum_0day_app_search", DoubleType, true),
  //      StructField("item_gmv_count_0day_app_search", DoubleType, true)))
  //    val dfA = sqlContext.createDataFrame(left, schemaL)
  //    //dfA.show()
  //    dfA.selectExpr("item_gmv_sum_0day_app_search / 100 as item_gmv_sum_0day_app_search").show()
  //    val test = new FeatureBuilder("test", "test")
  //    test.gmvDiv100(dfA).show()
  //  }
}