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

  def timeCost(): Long = {
    return this.endTime - this.startTime
  }
}


