package com.mgj.userprefer

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mgj.utils.LRLearner
import org.apache.commons.lang3.Validate

/**
  * Created by xiaonuo on 8/11/16.
  */
object VariableFactory {

  var bizdate: String = null
  var bizdateSubA: String = null
  var bizdateSubB: String = null
  var featureTypeList: Array[String] = null
  var sampleTypeList: Array[String] = null
  var entity: String = null
  var entityFeatureName: String = null
  var entityTableName: String = null
  var entityMapPath: String = null
  var entitySimPath: String = null
  var sampleList: Array[String] = null
  var modelList: Array[String] = null
  var predictBizdate: String = null
  var predictBizdateSub: String = null
  var predictResultList: Array[String] = null
  var predictTableList: Array[String] = null
  var featureNameList: Array[String] = null
  var successTag: String = null

  val userPreferProcessor = new UserPreferProcessor()
  val learner = new LRLearner()
  val sdf = new SimpleDateFormat("yyyyMMdd")
  val calendar = Calendar.getInstance()

  def init(args: Array[String]): Unit = {
    Validate.isTrue(args.size == 18, "input param error, param size must be 18.")

    bizdate = args(0)
    bizdateSubA = args(1)
    bizdateSubB = args(2)
    featureTypeList = args(3).split(",")
    sampleTypeList = args(4).split(",")
    entity = args(5)
    entityFeatureName = args(6)
    entityTableName = args(7)
    entityMapPath = args(8)
    entitySimPath = args(9)
    sampleList = args(10).split(",")
    modelList = args(11).split(",")
    predictBizdate = args(12)
    predictBizdateSub = args(13)
    predictResultList = args(14).split(",")
    predictTableList = args(15).split(",")
    featureNameList = args(16).split(",")
    successTag = args(17)

    Validate.isTrue(sampleList.size == sampleTypeList.size, "sample list size and sample type size must be the same.")
    Validate.isTrue(sampleList.size == modelList.size, "sample list size and model list size must be the same.")
    Validate.isTrue(predictResultList.size == predictTableList.size, "predict result list size and predict table list size must be the same.")
    Validate.isTrue(predictResultList.size == featureNameList.size, "predict result list size and feature name list size must be the same.")

    println(s"bizdate:${bizdate}")
    println(s"bizdateSubA:${bizdateSubA}")
    println(s"bizdateSubB:${bizdateSubB}")
    println(s"featureTypeList:${featureTypeList.toList}")
    println(s"sampleTypeList:${sampleTypeList.toList}")
    println(s"entity:${entity}")
    println(s"entityFeatureName:${entityFeatureName}")
    println(s"entityTableName:${entityTableName}")
    println(s"entityMapPath:${entityMapPath}")
    println(s"entitySimPath:${entitySimPath}")
    println(s"sampleList:${sampleList.toList}")
    println(s"modelList:${modelList.toList}")
    println(s"predictBizdate:${predictBizdate}")
    println(s"predictBizdateSub:${predictBizdateSub}")
    println(s"predictResultList:${predictResultList.toList}")
    println(s"predictTableList:${predictTableList.toList}")
    println(s"featureNameList:${featureNameList.toList}")
    println(s"successTag:${successTag}")
  }
}
