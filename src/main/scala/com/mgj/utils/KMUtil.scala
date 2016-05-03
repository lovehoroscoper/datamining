package com.mgj.utils

import java.util
import scala.util.control.Breaks._

/**
  * Created by xiaonuo on 4/5/16.
  */
object KMUtil {
  // mark x and mark y
  private val mx = new util.ArrayList[Boolean]()
  private val my = new util.ArrayList[Boolean]()

  // label x and label y
  private val lx = new util.ArrayList[Long]()
  private val ly = new util.ArrayList[Long]()
  private val slack = new util.ArrayList[Long]()

  // match flag
  private val mf = new util.ArrayList[Int]()

  // graph weight
  private val weight = scala.collection.mutable.Map[(Int, Int), Long]()

  private var size = 0

  private val const = 1e10

  private def init(weight: scala.collection.mutable.Map[(Int, Int), Double]): Unit = {
    this.weight.clear()
    this.mx.clear()
    this.my.clear()
    this.lx.clear()
    this.ly.clear()
    this.mf.clear()
    //    size = Math.max(weight.keys.map(x => x._1).toSet.size, weight.keys.map(x => x._2).toSet.size)
    size = Math.max(weight.keys.map(x => x._1).max + 1, weight.keys.map(x => x._2).max + 1)
    for (i <- 0 to size - 1; j <- 0 to size - 1) {
      this.weight.put((i, j), Math.round(const * weight.get((i, j)).getOrElse(0d)))
    }

    for (i <- 0 to size - 1) {
      lx.add(Long.MinValue)
      ly.add(0l)
      slack.add(Long.MaxValue)
      mx.add(false)
      my.add(false)
      mf.add(-1)
      lx.set(i, this.weight.filter(x => x._1._1 == i).map(x => x._2).max)
    }
  }

  private def resetFlag(): Unit = {
    for (i <- 0 to size - 1) {
      mx.set(i, false)
      my.set(i, false)
    }
  }

  private def resetSlack(): Unit = {
    for (i <- 0 to size - 1) {
      slack.set(i, Long.MaxValue)
    }
  }

  private def dfs(u: Int): Boolean = {
    mx.set(u, true)
    for (v <- 0 to size - 1) {
      if (!my.get(v)) {
        val t = lx.get(u) + ly.get(v) - weight.get((u, v)).get
        if (t == 0l) {
          my.set(v, true)
          if (mf.get(v) == -1 || dfs(mf.get(v))) {
            mf.set(v, u)
            return true
          }
        } else if (slack.get(v) > t) {
          slack.set(v, t)
        }
      }
    }
    return false
  }

  def findMaxWeightSum(weight: scala.collection.mutable.Map[(Int, Int), Double], weightIdf: scala.collection.mutable.Map[(Int, Int), Double]): Double = {
    init(weight)

    for (u <- 0 to size - 1) {
      resetSlack()
      breakable {
        while (true) {
          resetFlag()
          if (dfs(u)) {
            break()
          }

          var dmin = Long.MaxValue
          for (i <- 0 to size - 1) {
            if (!my.get(i) && slack.get(i) < dmin) {
              dmin = slack.get(i)
            }
          }
          for (i <- 0 to size - 1) {
            if (mx.get(i)) {
              lx.set(i, lx.get(i) - dmin)
            }
          }
          for (i <- 0 to size - 1) {
            if (my.get(i)) {
              ly.set(i, ly.get(i) + dmin)
            } else {
              slack.set(i, slack.get(i) - dmin)
            }
          }
        }
      }
    }

    var sum = 0d
    var sumA = 0d
    var sumB = 0d
    for (i <- 0 to size - 1) {
      sum += this.weight.get((mf.get(i), i)).get * weightIdf.get((mf.get(i), i)).getOrElse(0d)
    }

    return 1.0 * sum / const
  }

  def findMaxWeightSumCos(weight: scala.collection.mutable.Map[(Int, Int), Double], weightIdf: scala.collection.mutable.Map[Int, Double]): Double = {
    init(weight)

    for (u <- 0 to size - 1) {
      resetSlack()
      breakable {
        while (true) {
          resetFlag()
          if (dfs(u)) {
            break()
          }

          var dmin = Long.MaxValue
          for (i <- 0 to size - 1) {
            if (!my.get(i) && slack.get(i) < dmin) {
              dmin = slack.get(i)
            }
          }
          for (i <- 0 to size - 1) {
            if (mx.get(i)) {
              lx.set(i, lx.get(i) - dmin)
            }
          }
          for (i <- 0 to size - 1) {
            if (my.get(i)) {
              ly.set(i, ly.get(i) + dmin)
            } else {
              slack.set(i, slack.get(i) - dmin)
            }
          }
        }
      }
    }

    var sum = 0d
    var sumA = 0d
    var sumB = 0d
    for (i <- 0 to size - 1) {
      sum += this.weight.get((mf.get(i), i)).get * weightIdf.get(mf.get(i)).getOrElse(0d) * weightIdf.get(i).getOrElse(0d)
      sumA += weightIdf.get(mf.get(i)).getOrElse(0d) * weightIdf.get(mf.get(i)).getOrElse(0d)
      sumB += weightIdf.get(i).getOrElse(0d) * weightIdf.get(i).getOrElse(0d)
    }

    return 1.0 * sum / Math.sqrt(sumA * sumB) / const
  }
}