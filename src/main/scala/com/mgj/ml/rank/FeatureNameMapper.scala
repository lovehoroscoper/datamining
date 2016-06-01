package com.mgj.ml.rank

/**
  * Created by xiaonuo on 6/1/16.
  */
object FeatureNameMapper {
  val map = Map[String, String](
    "user_category_prefer" -> "userGBPrefer",
    "user_category_prefer_order" -> "userGBPreferOrder",
    "user_gene_prefer" -> "userGenePrefer",
    "user_gene_prefer_order" -> "userGenePreferOrder",
    "user_shop_prefer" -> "userShopPrefer",
    "user_shop_prefer_order" -> "userShopPreferOrder",
    "user_real_item_prefer" -> "userClick",
    "item_ctr" -> "ctr_score_new",
    "item_search_ctr" -> "ctr_cvr_score",
    "user_item_prefer" -> "userItemPrefer"
  )

  def getName(name: String): String = {
    return map.get(name).get
  }
}
