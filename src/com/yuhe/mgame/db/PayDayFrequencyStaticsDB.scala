package com.yuhe.mgame.db

import collection.mutable.Map
import collection.mutable.ArrayBuffer

/**
 * 日充值频率统计表
 */
object PayDayFrequencyStaticsDB {

  def insert(platformID: String, hostID: Int, date: String, payResult: Map[String, Int]) = {
    val cols = Array("PayUserNum", "Pay1Num",
      "Pay2Num", "Pay3Num", "Pay4Num", "Pay5Num", "Pay6Num", "Pay11Num", "Pay20Num")
    val strResults = ArrayBuffer[String]()
    val updateResults = ArrayBuffer[String]()
    for (col <- cols) {
      val value = payResult.getOrElse(col, "0")
      strResults += "'" + value + "'"
      updateResults += col + "=values(" + col + ")"
    }
    var sql = "insert into " + platformID + "_statics.tblPayDayFrequencyStatics(HostID, Date, " + cols.mkString(",")
    sql += ") values('" + hostID + "','" + date + "'," + strResults.mkString(",") + ") on duplicate key update " + updateResults.mkString(", ")
//    println(sql)
    DBManager.insert(sql)
  }
}