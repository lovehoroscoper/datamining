package com.mgj.feature

import java.util

/**
 * Created by xiaonuo on 12/5/15.
 */
class FeatureCalculatorFactory {
  private var calculators: util.HashMap[String, FeatureCalculator] = _

  def setCalculators(calculators: util.HashMap[String, FeatureCalculator]): Unit = {
    this.calculators = calculators
  }

  def getCalculator(name: String): FeatureCalculator = {
    return calculators.get(name)
  }

}
