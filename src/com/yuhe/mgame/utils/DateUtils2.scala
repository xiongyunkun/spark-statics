package com.yuhe.mgame.utils

import java.util.Date
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.commons.lang.time.DateUtils

object DateUtils2 {
  val DateFormat = "yyyy-MM-dd"
  
  /**
   * 获得dateStr相隔overdays的日期
   */
  def getOverDate(dateStr:String, overDays:Int) = {
    var resultDay = ""
    val parsePatterns = Array(DateUtils2.DateFormat)
    try{
      var date = DateUtils.parseDate(dateStr, parsePatterns)
      date = DateUtils.addDays(date, overDays)
		  resultDay = DateFormatUtils.format(date, DateUtils2.DateFormat)
    }catch{
      case ex:Exception =>
        ex.printStackTrace()
    }
    resultDay
  }
  
  def main(args: Array[String]) {
    println(getOverDate("2016-12-20", -1))
  }
}