package com.mgj.ml.maxentropy

import org.apache.spark.mllib.linalg.Vector

/**
  * Created by xiaonuo on 5/21/16.
  */
class MESample(l: Int, f: Vector) {

  private val label = l
  private val feature = f

  def getFeature(): Vector = {
    return this.feature
  }

  def getLabel(): Int = {
    return this.label
  }

  override def toString = s"MESample($label, $feature)"
}
