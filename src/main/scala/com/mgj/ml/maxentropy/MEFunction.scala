package com.mgj.ml.maxentropy

import org.apache.spark.mllib.linalg.Vector

/**
  * Created by xiaonuo on 5/21/16.
  */
class MEFunction(indexInput: Int, valueInput: Int, labelInput: Int) extends Serializable {
  //  private val index: Int = indexInput
  //  private val value: Int = valueInput
  //  private val label: Int = labelInput

  val index: Int = indexInput
  val value: Int = valueInput
  val label: Int = labelInput

  def apply(feature: Vector, label: Int): Int = {
    if (feature.apply(index) == value && label == this.label) {
      return 1
    } else {
      return 0
    }
  }
}
