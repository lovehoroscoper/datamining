package com.mgj.utils

import org.apache.commons.math3.special.Gamma
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._

/**
  * Created by xiaonuo on 2/15/16.
  */
object BayesSmoothUtil {

  /*
  @param input: (click, expose)
  @param output: (alpha, beta)
  @usage (click + alpha) / (expose + alpha + beta)
   */
  def getSmoothParam(input: RDD[(Double, Double)]): (Double, Double) = {
    //    val N: Int = 100
    val T: Int = 1000
    val err: Double = 1e-10
    var alpha: Double = 1
    var beta: Double = 1

    var k: Int = 0
    breakable {
      while (k < T) {
        val temp = input.map(x => {
          (Gamma.digamma(x._1 + alpha) - Gamma.digamma(alpha), Gamma.digamma(x._2 - x._1 + beta) - Gamma.digamma(beta), Gamma.digamma(x._2 + alpha + beta) - Gamma.digamma(alpha + beta))
        }).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))

        val x = temp._1 / temp._3
        val y = temp._2 / temp._3
        if (Math.abs(x - 1) < err && Math.abs(y - 1) < err) {
          break
        }
        alpha *= x
        beta *= y
        k += 1
      }
    }
    return (alpha, beta)
  }
}
