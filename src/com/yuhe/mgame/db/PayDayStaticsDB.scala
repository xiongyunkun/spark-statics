package com.yuhe.mgame.db

import math.BigDecimal
import collection.mutable.Map
import collection.mutable.ArrayBuffer

object PayDayStaticsDB {
  def insert(platformID:String, hostID:Int, date:String, currency:String, cashNum:BigDecimal, totalCashNum:BigDecimal,
      goldResults:Map[String, Long], payResults:Map[String, Int]) = {
    val payCols = Array("PayGold", "PayNum", "PayUserNum" , "FirstPayUserNum", 
		  "GoldConsume", "GoldProduce", "CreditGoldProduce", "CreditGoldConsume")
		val goldCols = Array("TotalPayGold", "TotalGoldProduce", "TotalGoldConsume", 
		    "TotalCreditGoldProduce", "TotalCreditGoldConsume")
		val strResults = ArrayBuffer[String]()
		val updateResults = ArrayBuffer[String]()
		for(col <- payCols){
		  val value = payResults.getOrElse(col, 0)
		  strResults += "'" + value + "'"
		  updateResults += col + "=values(" + col + ")" 
		}
    for(col <- goldCols){
      val value = goldResults.getOrElse(col, 0)
      strResults += "'" + value + "'"
		  updateResults += col + "=values(" + col + ")" 
    }
    //updateResults要加上totalCashNum,totalPayGold,cashNum
    updateResults += "CashNum=values(CashNum), TotalCashNum=values(TotalCashNum)"
    var sql = "insert into " + platformID + "_statics.tblPayDayStatics(HostID,Date,Currency,CashNum,TotalCashNum," 
    sql += payCols.mkString(",") + "," + goldCols.mkString(",") + ") values('"
		sql += hostID + "','" + date + "','" + currency + "','" + cashNum + "','" + totalCashNum + "',"
    sql += strResults.mkString(",") + ") on duplicate key update " + updateResults.mkString(", ")
//    println(sql)
    DBManager.insert(sql)
  }
}