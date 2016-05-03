package com.mgj.utils

/**
 * Created by xiaonuo on 11/18/15.
 */
object MkRowKeyUtil {
  var uniqKey: Int = 0

  def buildKey(time: Long): Long = {
    uniqKey += 1
    uniqKey %= (1 << 19)
    return (time / 1000 << 19) | uniqKey
  }
}
