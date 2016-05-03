package com.mgj.utils

import java.util.HashMap
import java.util.regex.Pattern

/**
 * Created by xiaonuo on 11/18/15.
 */
object SourceFilter extends java.io.Serializable {
  var patterns: HashMap[String, String] = null;

  patterns = new HashMap[String, String]();
  patterns.put("app_book", "(mgj://wall/book)|(mgj://fastfashionchannel)|(mgj://freemarket)|(mgj://market)|(mgj://catewall)|(www.mogujie.com/book)");
  patterns.put("app_search", "mgj://search");

  def isFrom(code: String, url: String): Boolean = {
    return Pattern.compile(patterns.get(code)).matcher(url).find();
  }

  def isFrom(codeList: List[String], url: String): Boolean = {
    for (s <- codeList) {
      if (isFrom(s, url)) {
        return true
      }
    }
    return false
  }
}
