package com.mgj.feature.impl.udfs

import com.mgj.feature.UdfTemplate
import org.springframework.stereotype.Service

/**
  * Created by xiaonuo on 7/5/16.
  */
@Service("matchUdf")
class MatchUdf extends UdfTemplate {
  override def buildFunction(): (String, String) => Double = {
    val function = (userFeature: String, itemFeature: String) => {
      if (userFeature != null && itemFeature != null) {
        userFeature.split(",").foreach(x => {
          val kv = x.split(":")
          if (kv(0).equals(itemFeature)) {
            kv(1).toDouble / 100000d
          }
        })
      }
      0d
    }
    return function
  }
}
