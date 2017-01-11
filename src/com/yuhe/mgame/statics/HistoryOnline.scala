package com.yuhe.mgame.statics

import org.apache.spark._
import org.apache.commons.lang.time.DateFormatUtils
/**
 * 统计历史在线信息
 */
class HistoryOnline extends Serializable with StaticsTrait{
  
   def statics(sc:SparkContext, platformID:String) = {
     val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
     
   }
}