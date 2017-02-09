package com.yuhe.mgame.utils

import math.BigDecimal

object Currency {
  /**
   * 汇率转换，这里后面再加上逻辑，需要从数据库或者缓存中读取汇率
   */
  def transformCurrency(currency:String, cashNum:BigDecimal) = {
    cashNum
  }
}