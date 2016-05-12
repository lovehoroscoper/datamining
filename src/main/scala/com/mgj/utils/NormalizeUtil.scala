package com.mgj.utils

/**
  * Created by xiaonuo on 4/25/16.
  */
object NormalizeUtil {
  def minMaxScaler(min: Double, max: Double, value: Double, minValue: Double): Double = {
    val interval = if (max == min) 1d else max - min
    return if (value == min) minValue else (value - min) / interval
  }

  def minMaxLogScaler(min: Double, max: Double, value: Double, minValue: Double): Double = {
    val interval = if (max == min) 1d else Math.log(1 + max) - Math.log(1 + min)
    return if (value == min) minValue else (Math.log(1 + value) - Math.log(1 + min)) / interval
  }
}
