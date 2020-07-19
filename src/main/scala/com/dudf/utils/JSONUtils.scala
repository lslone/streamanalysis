package com.dudf.utils

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.StringUtils

object JSONUtils {
  /**
   * 判断是否为json字符串
   *
   * @param content
   * @return
   */
  def isJSONString(content: String) {
    if (StringUtils.isEmpty(content)) {
      return false;
    }
    if (!content.startsWith("{") || !content.endsWith("}")) {
      return false;
    }
    try {
      JSON.parseObject(content)
      return true;
    } catch{
      case ex: Exception => {
        return false;
      }
    }

  }
}
