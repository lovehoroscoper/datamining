package com.mgj.ml.sample

/**
 * Created by xiaonuo on 11/21/15.
 */

object SampleConstant extends java.io.Serializable {
  final val source: String = "app_book"
  final val ownerSchema: String = "userid"
  final val entitySchema: String = "tradeitemid"
  final val timeSchema: String = "visit_time"
  final val sourceSchema: String = "url"
  final val labelSchema: String = "label"
  final val posTableName: String = "s_dg_user_click_log_spark"
  final val allTableName: String = "s_dg_user_expose_log_spark where url like '%book%' or url like '%search%' or url like '%fastfashionchannel%' or url like '%freemarket%'"
}
