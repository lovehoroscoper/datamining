package com.mgj.utils

/**
  * Created by xiaonuo on 5/18/16.
  */
object TimeUtil {
  var startTime = -1L
  var endTime = -1L

  def start(): Unit = {
    this.startTime = System.currentTimeMillis()
  }

  def end(): Unit = {
    this.endTime = System.currentTimeMillis()
  }

  def timeMillisCost(): Long = {
    return this.endTime - this.startTime
  }

  def timeCost(): Double = {
    return 1.0 * (this.endTime - this.startTime) / 1000 / 60
  }
}


