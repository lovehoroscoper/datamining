import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Suite, BeforeAndAfterAll}

/**
  * Created by xiaonuo on 5/4/16.
  */
trait LocalSparkContext extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  val conf = new SparkConf(false)

  override def beforeAll() {
    _sc = new SparkContext("local[4]", "test", conf)
    super.beforeAll()
  }

  override def afterAll() {
    // LocalSparkContext.stop(_sc)
    if (sc != null) {
      sc.stop()
    }

    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")

    _sc = null
    super.afterAll()
  }
}

