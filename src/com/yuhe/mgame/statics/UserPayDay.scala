package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.UserPayDayStaticsDB
import com.yuhe.mgame.db.PayDayFrequencyStaticsDB
import collection.mutable.Map
import scala.util.control._
import math.BigDecimal
import com.yuhe.mgame.utils.Currency

object UserPayDay extends Serializable with StaticsTrait {

  def statics(platformID: String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val payMap = loadPayOrderFromDB(platformID, today)
    for ((hostID, payList) <- payMap) {
      //这里因为字段里面有很多属性类型，int,float,string
      val resultMap = Map[Long, Map[String, String]]()
      val frequencyMap = Map[Long, Int]() //记录每个玩家的充值次数
      for (info <- payList) {
        val uid = info._1
        if (!resultMap.contains(uid)) {
          val userInfo = Map(
            "HostID" -> hostID.toString,
            "Uid" -> uid.toString,
            "Urs" -> info._2,
            "Name" -> info._3,
            "Date" -> today,
            "Currency" -> info._4,
            "TotalCashNum" -> info._5.toString,
            "TotalNum" -> "1",
            "TotalGold" -> info._6.toString)
          resultMap(uid) = userInfo
        } else {
          val cashNum = Currency.transformCurrency(info._4, info._5) //汇率转换
          var totalCashNum = BigDecimal(resultMap(uid)("TotalCashNum")) + info._5 
          var totalNum = resultMap(uid)("TotalNum").toInt + 1
          var totalGold = resultMap(uid)("TotalGold").toInt + info._6
          resultMap(uid)("TotalCashNum") = totalCashNum.toString
          resultMap(uid)("TotalNum") = totalNum.toString
          resultMap(uid)("TotalGold") = totalGold.toString
        }
        frequencyMap(uid) = frequencyMap.getOrElse(uid, 0) + 1
      }
      //记录数据库
      UserPayDayStaticsDB.insert(platformID, resultMap)
      //充值频率表
      if (frequencyMap.size > 0) {
        val frequencyResult = staticsFrequency(platformID, frequencyMap)
        PayDayFrequencyStaticsDB.insert(platformID, hostID, today, frequencyResult)
      }
    }
  }

  /**
   * 从tblPayOrder表中获得当天的充值记录信息
   */
  def loadPayOrderFromDB(platformID: String, date: String) = {
    val tblName = platformID + "_statics.tblPayOrder"
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val payRes = DBManager.query(tblName, options)
    payRes.select("HostID", "Uid", "Urs", "Name", "Currency", "CashNum", "Gold").rdd.map(row => {
      val hostID = row.getInt(0)
      val uid = row.getLong(1)
      val urs = row.getString(2)
      val name = row.getString(3)
      val currency = row.getString(4)
      val cashNum = row.getDecimal(5)
      val gold = row.getInt(6)
      (hostID, (uid, urs, name, currency, cashNum, gold))
    }).groupByKey.collectAsMap
  }
  /**
   * 根据每个玩家的充值次数计算充值区间
   */
  def staticsFrequency(platformID:String, frequencyMap: Map[Long, Int]) = {
    val zoneMap = Map(
      "Pay1Num" -> Array(1, 1),
      "Pay2Num" -> Array(2, 2),
      "Pay3Num" -> Array(3, 3),
      "Pay4Num" -> Array(4, 4),
      "Pay5Num" -> Array(5, 5),
      "Pay6Num" -> Array(6, 10),
      "Pay11Num" -> Array(11, 20),
      "Pay20Num" -> Array(21))
    val results = Map[String, Int]()
    val loop = new Breaks
    var totalNum = 0
    for ((uid, num) <- frequencyMap) {
      loop.breakable {
        for ((zoneID, valueList) <- zoneMap) {
          val size = valueList.size
          if (size > 1 && num >= valueList(0)) {
            results(zoneID) = results.getOrElse(zoneID, 0) + 1
            loop.break
          }else if(size == 2 && num >= valueList(0) && num < valueList(1)){
            results(zoneID) = results.getOrElse(zoneID, 0) + 1
            loop.break
          }
        }
      }
      totalNum += 1
    }
    results("PayUserNum") = totalNum
    results
  }
}