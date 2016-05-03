
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by xiaonuo on 15/8/10.
  */
class Test extends FlatSpec with Matchers {

  "practice" should "train" in {
    val test = new Practice()
    test.main(Array())
    //    val test = new SmoothTest()
    //    test.main(Array())
  }
}
