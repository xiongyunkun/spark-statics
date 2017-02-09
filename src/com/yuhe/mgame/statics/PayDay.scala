package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.PayDayStaticsDB
import collection.mutable.Map
import com.yuhe.mgame.utils.Currency
import math.BigDecimal
import com.yuhe.mgame.utils.DateUtils2
/**
 * 日充值统计
 */
object PayDay extends Serializable with StaticsTrait {
  
  def statics(platformID: String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val userPayMap = loadUserPayDayFromDB(platformID, today)
    val goldMap = loadGoldFromDB(platformID, today)
    val yesterday = DateUtils2.getOverDate(today, -1)
    val yesterdayPayMap = loadPayDayStaticsFromDB(platformID, yesterday)
    for((hostID, payList) <- userPayMap){
      var cashNum = BigDecimal(0) //充值金额
      var totalCashNum = BigDecimal(0) //总充值金额
      var currency = "USD" //默认是美金
      val payResults = Map(
    		"PayGold" -> 0, //充值钻石数量
    		"PayNum" -> 0, //充值次数
    		"PayUserNum" -> 0, //充值人数
    		"FirstPayUserNum" -> 0, //首冲人数
    		"GoldConsume" -> 0, //非绑钻消耗数
    		"GoldProduce" -> 0, //非绑钻产出数
    		"CreditGoldProduce" -> 0, //绑钻产出数
    		"CreditGoldConsume" -> 0 //绑钻消耗数
      )
      val goldResults = Map(
        "TotalPayGold" -> 0L, //总充值钻石  
        "TotalGoldProduce" -> 0L, //总非绑钻石产出数
    		"TotalGoldConsume" -> 0L, //总非绑钻消耗数
    		"TotalCreditGoldProduce" -> 0L, //总绑钻产出数
    		"TotalCreditGoldConsume" -> 0L //总绑钻消耗数 
      )
      for(info <- payList){
        currency = info._1
        val userCashNum = info._2
        val totalNum = info._3
        val totalGold = info._4
        val currencyCashNum = Currency.transformCurrency(currency, userCashNum)
        cashNum = cashNum + currencyCashNum
        totalCashNum = totalCashNum + currencyCashNum
        payResults("PayGold") = payResults("PayGold") + totalGold
        payResults("PayNum") = payResults("PayNum") + totalNum
        payResults("PayUserNum") = payResults("PayUserNum") + 1
        goldResults("TotalPayGold") = goldResults("TotalPayGold") + totalGold
      }
      if(goldMap.contains(hostID)){
        val goldList = goldMap(hostID)
        for(info <- goldList){
          val goldType = info._1
          val staticsType = info._2
          val gold = info._3
          if(staticsType == 1){ //消耗
            if(goldType == 1){// 非绑钻
              goldResults("TotalGoldConsume") = goldResults("TotalGoldConsume") + gold
              payResults("GoldConsume") = payResults("GoldConsume") + gold
            }else{ //绑钻
              goldResults("TotalCreditGoldConsume") = goldResults("TotalCreditGoldConsume") + gold
              payResults("CreditGoldConsume") = payResults("CreditGoldConsume") + gold
            }
          }else if(staticsType == 2){ //产出
            if(goldType == 1){ //非绑钻
               goldResults("TotalGoldProduce") = goldResults("TotalGoldProduce") + gold
               payResults("GoldProduce") = payResults("GoldProduce") + gold
            }else{ //绑钻
              goldResults("TotalCreditGoldProduce") = goldResults("TotalCreditGoldProduce") + gold
              payResults("CreditGoldProduce") = payResults("CreditGoldProduce") + gold
            }
          }
        }
      }
      //还得加上昨天的数据才能计算总计
      if(yesterdayPayMap.contains(hostID)){
        val yesterdayList = yesterdayPayMap(hostID)
        for(info <- yesterdayList){
          totalCashNum = totalCashNum + info._1
          goldResults("TotalPayGold") = goldResults("TotalPayGold") + info._2
          goldResults("TotalGoldProduce") = goldResults("TotalGoldProduce") + info._3
          goldResults("TotalGoldConsume") = goldResults("TotalGoldConsume") + info._4
          goldResults("TotalCreditGoldProduce") = goldResults("TotalCreditGoldProduce") + info._5
          goldResults("TotalCreditGoldConsume") = goldResults("TotalCreditGoldConsume") + info._6
        }
      }
      //记录数据库
      PayDayStaticsDB.insert(platformID, hostID, today, currency, cashNum, totalCashNum, goldResults, payResults)
    }
  }
  
  /**
   * 从tblUserPayDayStatics获得当天各个玩家的充值数据
   */
  def loadUserPayDayFromDB(platformID:String, date:String) = {
    val tblName = platformID + "_statics.tblUserPayDayStatics"
    val timeOption = "Date = '" + date + "'"
    val options = Array(timeOption)
    val payRes = DBManager.query(tblName, options)
    payRes.select("HostID", "Currency", "TotalCashNum", "TotalNum", "TotalGold").rdd.map(row => {
      val hostID = row.getInt(0)
      val currency = row.getString(1)
      val totalCashNum = row.getDecimal(2)
      val totalNum = row.getInt(3)
      val totalGold = row.getInt(4)
      (hostID, (currency, totalCashNum, totalNum, totalGold))
    }).groupByKey.collectAsMap
  }
  /**
   * 从tblGold获得当天钻石的消费产出情况
   */
  def loadGoldFromDB(platformID:String, date:String) = {
    val tblName = platformID + "_statics.tblGold"
    val timeOption = "Date = '" + date + "'"
    val options = Array(timeOption)
    val goldRes = DBManager.query(tblName, options)
    goldRes.select("HostID", "GoldType", "StaticsType", "Value").rdd.map(row => {
      val hostID = row.getInt(0)
      val goldType = row.getInt(1)
      val staticsType = row.getInt(2)
      val gold = row.getInt(3)
      (hostID, (goldType, staticsType, gold))
    }).groupByKey.collectAsMap
  }
  /**
   * 从tblPayDayStatics获得某天的充值总计
   */
  def loadPayDayStaticsFromDB(platformID:String, date:String) = {
    val tblName = platformID + "_statics.tblPayDayStatics"
    val timeOption = "Date = '" + date + "'"
    val options = Array(timeOption)
    val payRes = DBManager.query(tblName, options)
    payRes.select("HostID", "TotalCashNum", "TotalPayGold", "TotalGoldProduce", "TotalGoldConsume",
        "TotalCreditGoldProduce", "TotalCreditGoldConsume").rdd.map(row => {
      val hostID = row.getInt(0)
      val totalCashNum = row.getDecimal(1)
      val totalPayGold = row.getLong(2)
      val totalGolProduce = row.getLong(3)
      val totalGoldConsume = row.getLong(4)
      val totalCreditGoldProduce = row.getLong(5)
      val TotalCreditGoldConsume = row.getLong(6)
      (hostID, (totalCashNum, totalPayGold, totalGolProduce, totalGoldConsume, totalCreditGoldProduce, TotalCreditGoldConsume))
    }).groupByKey.collectAsMap
  }
}