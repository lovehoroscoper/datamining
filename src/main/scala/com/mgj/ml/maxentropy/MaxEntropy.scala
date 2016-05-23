package com.mgj.ml.maxentropy

import java.util

import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.Statistics

/**
  * Created by xiaonuo on 5/21/16.
  */
class MaxEntropy(MESampleRDDInput: RDD[MESample]) extends Serializable {
  private var ITERATIONS: Int = 10
  private var EPSILON: Double = 1e-3d
  private var ETA: Double = 1e-8d
  private var ERROR: Double = 1e-3d
  private var sampleSize: Long = 0
  private var minLabel: Int = 0
  private var maxLabel: Int = 0
  private var empiricalExpects = Map[Long, Double]()
  private var weight = new util.HashMap[Long, Double]()
  private val MESampleRDD = MESampleRDDInput
  private var functions = Map[MEFunction, Long]()
  private var features = Map[Vector, Long]()

  def setIteration(iterations: Int): Unit = {
    this.ITERATIONS = iterations
  }

  def setEpsilon(epsilon: Double): Unit = {
    this.EPSILON = epsilon
  }

  def setEta(eta: Double): Unit = {
    this.ETA = eta
  }

  def setError(error: Double): Unit = {
    this.ERROR = error
  }

  def getFunctions(): Map[MEFunction, Long] = {
    return functions
  }

  def getWeight(): util.HashMap[Long, Double] = {
    return weight
  }

  def setWeight(weight: String): Unit = {
    val gson = new Gson()
    this.weight = gson.fromJson(weight, classOf[util.HashMap[Long, Double]])
  }

  def setFunctions(function: String): Unit = {
    val gson = new Gson()
    this.functions = gson.fromJson(function, classOf[Map[MEFunction, Long]])
  }

  private def init(): Unit = {
    this.maxLabel = MESampleRDD.map(x => x.getLabel()).max()
    println(s"max label:${maxLabel}")
    this.minLabel = MESampleRDD.map(x => x.getLabel()).min()
    println(s"min label:${minLabel}")
    this.features = MESampleRDD.map(x => x.getFeature()).distinct().zipWithIndex().collect().toMap
    println(s"distinct feature size:${features.size}")
    this.functions = Statistics.colStats(MESampleRDD.map(x => x.getFeature())).max.toArray.zipWithIndex.map(x => {
      for (label <- minLabel to maxLabel;
           value <- 0 to x._1.toInt) yield {
        (new MEFunction(x._2, value, label))
      }
    }).flatMap(x => x).zipWithIndex.map(x => (x._1, x._2.toLong)).toMap
    println(s"function size:${functions.size}")
    this.sampleSize = MESampleRDD.count()
    println(s"sample size:${sampleSize}")
    this.empiricalExpects = MESampleRDD.map(x => {
      this.functions.toList.map(t => (t._2, t._1.apply(x.getFeature(), x.getLabel()).toDouble))
    }).flatMap(x => x).reduceByKey((a, b) => a + b).map(x => (x._1, x._2 / sampleSize)).collect().toMap
    this.functions.foreach(x => this.weight.put(x._2, 0d))
  }

  def train(): Unit = {
    init()
    var errorPre = 0d
    for (k <- 1 to ITERATIONS) {
      // todo stop condition
      val deltaMap = this.functions.map(x => {
        val delta = getDelta(empiricalExpects.get(x._2).get, x._1)
        weight.put(x._2, delta + weight.get(x._2))
        (x._2, delta)
      })
      val error = deltaMap.map(x => x._2 * x._2).sum
      if (Math.abs(error - errorPre) < ERROR) {
        return
      } else {
        errorPre = error
      }
      println(s"iteration:${k},weight:${weight}")
      println(s"error:${error}")
    }
  }

  def predict(feature: Vector): (Int, Double) = {
    var prob = 0d
    var label = minLabel
    var sum = 0d
    for (l <- minLabel to maxLabel) {
      val temp = this.functions.map(x => Math.exp(x._1.apply(feature, l) * weight.get(x._2))).sum
      sum += temp
      if (prob < temp) {
        prob = temp
        label = l
      }
    }
    return (label, prob / sum)
  }

  // user iis to get delta
  private def getDelta(empirical: Double, function: MEFunction): Double = {
    var delta: Double = 0d
    var fNewton: Double = 0d
    var dfNewton: Double = 0d
    val probYX: Map[(Long, Long), Double] = getProbYX()
    var iter: Int = 0

    while (iter < 50) {
      val p = MESampleRDD.map(x => {
        val feature = x.getFeature()
        val index = this.features.get(feature).get
        var probSum = 0d
        var probDeltaSum = 0d
        for (label <- minLabel to maxLabel) {
          val functionSharp = getFunctionSharp(feature, label)
          val prob = probYX.get((index, label)).get * function.apply(feature, label) * Math.exp(delta * functionSharp)
          probSum += prob
          probDeltaSum += prob * functionSharp
        }
        (probSum, probDeltaSum)
      }).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      fNewton = empirical - p._1 / this.sampleSize
      dfNewton = -p._2 / this.sampleSize
      if (Math.abs(fNewton) < ETA) {
        return delta
      }
      val ratio: Double = fNewton / dfNewton
      delta -= ratio

      if (Math.abs(ratio) < EPSILON) {
        return delta
      }
      iter += 1
    }
    return delta
  }

  private def getFunctionSharp(feature: Vector, label: Int): Int = {
    return this.functions.map(x => x._1.apply(feature, label)).sum
  }

  private def getProbYX(): Map[(Long, Long), Double] = {
    var probYX = this.features.map(x => {
      for (y <- minLabel to maxLabel) yield {
        ((x._2, y.toLong), Math.exp(this.functions.map(t => t._1.apply(x._1, y) * this.weight.get(t._2)).sum))
      }
    }).flatMap(x => x).toMap

    val probX = probYX.groupBy(x => x._1._1).map(x => (x._1, x._2.map(x => x._2).sum))
    probYX = probYX.map(x => (x._1, x._2 / probX.get(x._1._1).get))
    return probYX
  }

}
