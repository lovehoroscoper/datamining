package com.mgj.feature

import java.util

/**
  * Created by xiaonuo on 12/5/15.
  */
class FeatureCalculatorFactory {
  private var calculators: util.HashMap[String, FeatureCalculator] = _

  private var computers: util.HashMap[String, String] = _

  def setCalculators(calculators: util.HashMap[String, FeatureCalculator]): Unit = {
    this.calculators = calculators
  }

  def getCalculator(name: String): FeatureCalculator = {
    return calculators.get(name)
  }

  def containsCalculator(name: String): Boolean = {
    return calculators.containsKey(name)
  }

  def setComputers(calculators: util.HashMap[String, String]): Unit = {
    this.computers = calculators
  }

  def getComputer(name: String): String = {
    return computers.get(name)
  }

  def containsComputer(name: String): Boolean = {
    return computers.containsKey(name)
  }
}
