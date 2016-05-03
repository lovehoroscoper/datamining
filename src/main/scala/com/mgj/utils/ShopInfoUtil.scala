package com.mgj.utils

/**
  * Created by xiaonuo on 1/12/16.
  */
object ShopInfoUtil {
  def toUrl(id: String): String = {
    return "shop.mogujie.com/" + SampleUtil.encode(id)
  }
}
