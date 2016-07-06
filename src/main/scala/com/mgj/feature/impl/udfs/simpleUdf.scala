package com.mgj.feature.impl.udfs

import com.mgj.feature.UdfTemplate
import org.springframework.stereotype.Service

/**
  * Created by xiaonuo on 7/5/16.
  */
@Service("simpleUdf")
class SimpleUdf extends UdfTemplate {
  override def buildFunction(): (String) => Double = {
    val function = (input: String) => input.toDouble
    return function
  }
}
