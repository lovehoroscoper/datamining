
import com.mgj.ml.maxentropy.{MESample, MaxEntropy}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by xiaonuo on 15/8/10.
  */
class Test extends FlatSpec with Matchers with LocalSparkContext {
  //class Test extends FlatSpec with Matchers {
  "main" should "test" in {
    //    val test = new Practice()
    //    test.main(Array())
    //    val test = new SmoothTest()
    //    test.main(Array())
    //    val test = new DBNTest()
    //    test.main(Array())
    //    val sqlContext = new SQLContext(sc)
    //    val data = sc.parallelize(List(1, 2, 3))
    //    data.take(10).foreach(println)
    val sampleTrain = sc.textFile("src/test/resources/train").map(x => {
      val labelStr = x.split(" ", 2)(0)
      val featureStr = x.split(" ", 2)(1)
      val label = labelStr.substring(labelStr.length - 1).toInt
      val feature = Vectors.dense(featureStr.split(" ").map(x => x.substring(x.length - 1).toDouble))
      new MESample(label, feature)
    })

    val maxEntropy = new MaxEntropy(sampleTrain)
    maxEntropy.train()
    val sampleTest = sc.textFile("src/test/resources/test").map(x => {
      val labelStr = x.split(" ", 2)(0)
      val featureStr = x.split(" ", 2)(1)
      val label = labelStr.substring(labelStr.length - 1).toInt
      val feature = Vectors.dense(featureStr.split(" ").map(x => x.substring(x.length - 1).toDouble))
      new MESample(label, feature)
    }).map(x => (maxEntropy.predict(x.getFeature()), x.getLabel()))
    sampleTest.collect.foreach(println)
    val ratio = sampleTest.map(x => (if (x._1._1 == x._2) 1 else 0, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    println(1.0 * ratio._1 / ratio._2)
    val ratioTrain = sampleTrain.map(x => (maxEntropy.predict(x.getFeature()), x.getLabel())).map(x => (if (x._1._1 == x._2) 1 else 0, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    println(1.0 * ratioTrain._1 / ratioTrain._2)
  }
}