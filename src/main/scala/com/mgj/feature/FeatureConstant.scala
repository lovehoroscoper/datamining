package com.mgj.feature

/**
 * Created by xiaonuo on 12/6/15.
 */
object FeatureConstant extends java.io.Serializable {
  var USER_KEY: String = "user_id"  //AlgoOfflineConstants.UID
  var ITEM_KEY: String = "item_id"  //AlgoOfflineConstants.IID
  var LABEL_KEY: String = "label"
  var FEATURE_KEY: String = "features"
  val RAW_PREDICTION = "raw_prediction"
  val PREDICTION = "prediction"
  val PROBABILITY = "probability"
}
