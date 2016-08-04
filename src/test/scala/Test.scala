
import com.mgj.feature.UdfTemplate
import com.mgj.ml.maxentropy.{MESample, MaxEntropy}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._
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
    //    val sampleTrain = sc.textFile("src/test/resources/train").map(x => {
    //      val labelStr = x.split(" ", 2)(0)
    //      val featureStr = x.split(" ", 2)(1)
    //      val label = labelStr.substring(labelStr.length - 1).toInt
    //      val feature = Vectors.dense(featureStr.split(" ").map(x => x.substring(x.length - 1).toDouble))
    //      new MESample(label, feature)
    //    })
    //
    //    val maxEntropy = new MaxEntropy(sampleTrain)
    //    maxEntropy.train()
    //    val sampleTest = sc.textFile("src/test/resources/test").map(x => {
    //      val labelStr = x.split(" ", 2)(0)
    //      val featureStr = x.split(" ", 2)(1)
    //      val label = labelStr.substring(labelStr.length - 1).toInt
    //      val feature = Vectors.dense(featureStr.split(" ").map(x => x.substring(x.length - 1).toDouble))
    //      new MESample(label, feature)
    //    }).map(x => (maxEntropy.predict(x.getFeature()), x.getLabel()))
    //    sampleTest.collect.foreach(println)
    //    val ratio = sampleTest.map(x => (if (x._1._1 == x._2) 1 else 0, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    //    println(1.0 * ratio._1 / ratio._2)
    //    val ratioTrain = sampleTrain.map(x => (maxEntropy.predict(x.getFeature()), x.getLabel())).map(x => (if (x._1._1 == x._2) 1 else 0, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    //    println(1.0 * ratioTrain._1 / ratioTrain._2)

    //    val catRelation = sc.textFile("/user/bizdata/categoryScore.in").map(x => (x.split("\t")(1), x.split("\t")(3), x.split("\t")(4)))
    //    val query2category = sc.textFile("/user/bizdata/query2category").map(x => (x.split("\t")(0), x.split("\t")(1), "-1"))
    //    val allRelation = catRelation.union(query2category)
    //    val result = allRelation.filter(x => !x._1.equals("keyword")).groupBy(x => x._1).map(x => {
    //      var maxScore = x._2.map(t => t._3.toLong).max
    //      if (maxScore == -1l) {
    //        maxScore = 60000000l
    //      }
    //      val result = x._2.toList.map(t => {
    //        val score = t._3.toLong
    //        if (score == -1l) {
    //          (t._1, t._2, maxScore)
    //        } else {
    //          (t._1, t._2, score)
    //        }
    //      })
    //      result
    //    }).flatMap(x => x).groupBy(x => x._1 + x._2).map(x => {
    //      val score = x._2.toList.map(t => t._3).max
    //      val list = x._2.toList.head
    //      (list._1, list._2, score)
    //    })

    //    sc.parallelize(catRelation.map(x => "type\t"+x._1+"\tid\t"+x._2+"\t"+x._3+"\tsort").take(1)).union(result.map(x => "0\t"+x._1+"\t0\t"+x._2+"\t"+x._3+"\t0")).saveAsTextFile("/user/bizdata/categoryRelation")

    //    val result = allRelation.filter(x => !x._1.equals("keyword")).groupBy(x => x._1).map(x => {
    //      val maxScore = x._2.map(t => t._3.toLong).max
    //      val result = x._2.toList.map(t => {
    //        val score = t._3.toLong
    //        if (score == -1l) {
    //          (t._1, t._2, maxScore)
    //        } else {
    //          (t._1, t._2, score)
    //        }
    //      })
    //      result
    //    }).flatMap(x => x).groupBy(x => x._1 + x._2).map(x => {
    //      val score = x._2.toList.map(t => t._3).max
    //      val list = x._2.toList.head
    //      (list._1, list._2, x._2.size)
    //    }).filter(x => x._3 > 1)

    //    val bizdate = "2015/02/12"
    //    val pattern = """(\d{4}).*(\d{2}).*(\d{2})""".r
    //    val dateMeta = pattern.findFirstIn(bizdate)
    //    val bizdateNew = if (dateMeta != None) {
    //      val pattern(year, month, day) = dateMeta.get
    //      print(year + month + day)
    //    } else {
    //      print("error")
    //    }


    case class User()

    trait Repository {
      def save(user: User)
    }

    trait DefaultRepository extends Repository {
      def save(user: User) = {
        println("save user")
      }
    }

    trait NewRepository extends Repository {
      def save(user: User) = {
        println("save new user")
      }
    }

    trait UserService {
      self: Repository =>
      def create(user: User): Unit = {
        save(user)
      }
    }

    (new UserService with NewRepository).create(new User)

  }
}