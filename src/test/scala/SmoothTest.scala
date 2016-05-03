/**
  * Created by xiaonuo on 2/15/16.
  */

import org.apache.commons.math3.special.Gamma
import java.util
import scala.util.control.Breaks._

class SmoothTest {
  def main(args: Array[String]) {
    val N: Int = 1000
    val T: Int = 100000
    val err: Double = 1e-10
    var alpha: Double = 1
    var beta: Double = 1
    val click: util.List[Double] = new util.ArrayList[Double]()
    val expose: util.List[Double] = new util.ArrayList[Double]()
    var i: Int = 0
    while (i < N) {
      click.add(10d * (1d + Math.random) / 2)
      expose.add(1000d * (1d + Math.random) / 2)
      i += 1
    }

    //    System.out.println(util.Arrays.toString(click.toArray))
    //    System.out.println(util.Arrays.toString(expose.toArray))

    var k: Int = 0
    breakable {
      while (k < T) {
        var a: Double = 0
        var b: Double = 0
        var c: Double = 0
        var i: Int = 0
        while (i < N) {
          a += (Gamma.digamma(click.get(i) + alpha) - Gamma.digamma(alpha))
          b += (Gamma.digamma(expose.get(i) - click.get(i) + beta) - Gamma.digamma(beta))
          c += (Gamma.digamma(expose.get(i) + alpha + beta) - Gamma.digamma(alpha + beta))
          i += 1
        }
        val x = a / c
        val y = b / c
        if (Math.abs(x - 1) < err && Math.abs(y - 1) < err) {
          break
        }
        alpha *= a / c
        beta *= b / c
        k += 1
        println(s"alpha:${alpha}")
        println(s"beta:${beta}")
      }
    }
    println(k)

    System.out.println(alpha)
    System.out.println(beta)
    System.out.println(alpha / (alpha + beta))
    i = 0
    while (i < N) {
      //      System.out.println("click:" + click.get(i) + ", expose:" + expose.get(i) + ", real:" + click.get(i) / expose.get(i) + ", smooth:" + (click.get(i) + alpha) / (expose.get(i) + alpha + beta))
      i += 1
    }
  }
}
