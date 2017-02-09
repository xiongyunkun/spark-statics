package com.yuhe.mgame.db

import collection.mutable.Map
import collection.mutable.ArrayBuffer
/**
 * 玩家日充值统计表
 */
object UserPayDayStaticsDB {
  def insert(platformID:String, payResults:Map[Long, Map[String, String]]) = {
    val cols = Array("Uid", "Date", "Urs", "Name", "HostID", "Currency", "TotalCashNum" , "TotalNum", "TotalGold")
    val strResults = ArrayBuffer[String]()
    for((_, result) <- payResults){
      val valueList = ArrayBuffer[String]()
      for(col <- cols){
        val value = result.getOrElse(col, "")
        valueList += "'" + value + "'"
      }
      strResults += valueList.mkString(",")
    }
    var sql = "insert into " + platformID + "_statics.tblUserPayDayStatics( " + cols.mkString(",") + ") values("
    sql += strResults.mkString("),(")
    sql += ") on duplicate key update TotalCashNum = values(TotalCashNum), TotalNum = values(TotalNum),TotalGold = values(TotalGold)"
//    println(sql)
    DBManager.insert(sql)
  }
}