package com.yuhe.mgame.statics

import collection.mutable.ArrayBuffer
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.spark.rdd.RDD
import java.lang.Math
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.RetentionDB
import com.yuhe.mgame.utils.DateUtils2
import scala.collection.mutable.Map
/**
 * 统计登陆留存率，设备留存率，付费留存率
 */
object Retention extends Serializable with StaticsTrait{
  def statics(platformID:String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val (loginUids, loginIMEIs, hostIDs) = loadLoginInfoFromDB(platformID, today)
    staticsLoginRetention(platformID, today, loginUids, loginIMEIs, hostIDs)
    staticsPayUserRetention(platformID, today, loginUids, hostIDs)
  }
  /**
   * 统计登陆留存
   */
  def staticsLoginRetention(platformID:String, today:String, loginUids: RDD[(Long, Int)],
        loginIMEIs: RDD[(String, Int)], hostIDs: RDD[Int]) = {
    val (todayRegUids, todayRegIMEIs) = loadRegInfoFromDB(platformID, today)
    val day1 = DateUtils2.getOverDate(today, -1)
    val day2 = DateUtils2.getOverDate(today, -2)
    val day3 = DateUtils2.getOverDate(today, -3)
    val day4 = DateUtils2.getOverDate(today, -4)
    val day5 = DateUtils2.getOverDate(today, -5)
    val day6 = DateUtils2.getOverDate(today, -6)
    val day7 = DateUtils2.getOverDate(today, -7)
    val day10 = DateUtils2.getOverDate(today, -10)
    val day13 = DateUtils2.getOverDate(today, -13)
    val day15 = DateUtils2.getOverDate(today, -15)
    val day29 = DateUtils2.getOverDate(today, -29)
    val day30 = DateUtils2.getOverDate(today, -30)
    val (day1RegUids, day1RegIMEIs) = loadRegInfoFromDB(platformID, day1)
    val (day2RegUids, day2RegIMEIs) = loadRegInfoFromDB(platformID, day2)
    val (day3RegUids, day3RegIMEIs) = loadRegInfoFromDB(platformID, day3)
    val (day4RegUids, day4RegIMEIs) = loadRegInfoFromDB(platformID, day4)
    val (day5RegUids, day5RegIMEIs) = loadRegInfoFromDB(platformID, day5)
    val (day6RegUids, day6RegIMEIs) = loadRegInfoFromDB(platformID, day6)
    val (day7RegUids, day7RegIMEIs) = loadRegInfoFromDB(platformID, day7)
    val (day10RegUids, day10RegIMEIs) = loadRegInfoFromDB(platformID, day10)
    val (day13RegUids, day13RegIMEIs) = loadRegInfoFromDB(platformID, day13)
    val (day15RegUids, day15RegIMEIs) = loadRegInfoFromDB(platformID, day15)
    val (day29RegUids, day29RegIMEIs) = loadRegInfoFromDB(platformID, day29)
    val (day30RegUids, day30RegIMEIs) = loadRegInfoFromDB(platformID, day30)
    
    val rate1 = staticsUidRetentionRate(loginUids, day1RegUids)
    val rate2 = staticsUidRetentionRate(loginUids, day2RegUids)
    val rate3 = staticsUidRetentionRate(loginUids, day3RegUids)
    val rate4 = staticsUidRetentionRate(loginUids, day4RegUids)
    val rate5 = staticsUidRetentionRate(loginUids, day5RegUids)
    val rate6 = staticsUidRetentionRate(loginUids, day6RegUids)
    val rate7 = staticsUidRetentionRate(loginUids, day7RegUids)
    val rate10 = staticsUidRetentionRate(loginUids, day10RegUids)
    val rate13 = staticsUidRetentionRate(loginUids, day13RegUids)
    val rate15 = staticsUidRetentionRate(loginUids, day15RegUids)
    val rate29 = staticsUidRetentionRate(loginUids, day29RegUids)
    val rate30 = staticsUidRetentionRate(loginUids, day30RegUids)
    val loginNums = getHostNum(loginUids)
    val regNums = getHostNum(todayRegUids)
    
    hostIDs.foreach(hostID => {
      val results = Map[String,String]("LoginNum"->loginNums.getOrElse(hostID, 0).toString, 
        "NewNum"->regNums.getOrElse(hostID, 0).toString,
        "1Days"-> rate1.getOrElse(hostID, 0).toString, "2Days"-> rate2.getOrElse(hostID, 0).toString, 
        "3Days"-> rate3.getOrElse(hostID, 0).toString, "4Days"->rate4.getOrElse(hostID, 0).toString, 
        "5Days"->rate5.getOrElse(hostID, 0).toString, "6Days"->rate6.getOrElse(hostID, 0).toString, 
        "7Days"->rate7.getOrElse(hostID, 0).toString, "10Days"->rate10.getOrElse(hostID, 0).toString,
        "13Days"->rate13.getOrElse(hostID, 0).toString, "15Days"->rate15.getOrElse(hostID, 0).toString,
        "29Days"->rate29.getOrElse(hostID, 0).toString, "30Days"->rate30.getOrElse(hostID, 0).toString)
      RetentionDB.insertLoginRetention(platformID, hostID.toString, today, results)
    })
    /*
     * 统计设备留存IMEI
     */
    val irate1 = staticsIMEIRetention(loginIMEIs, day1RegIMEIs)
    val irate2 = staticsIMEIRetention(loginIMEIs, day2RegIMEIs)
    val irate3 = staticsIMEIRetention(loginIMEIs, day3RegIMEIs)
    val irate4 = staticsIMEIRetention(loginIMEIs, day4RegIMEIs)
    val irate5 = staticsIMEIRetention(loginIMEIs, day5RegIMEIs)
    val irate6 = staticsIMEIRetention(loginIMEIs, day6RegIMEIs)
    val irate7 = staticsIMEIRetention(loginIMEIs, day7RegIMEIs)
    val irate10 = staticsIMEIRetention(loginIMEIs, day10RegIMEIs)
    val irate13 = staticsIMEIRetention(loginIMEIs, day13RegIMEIs)
    val irate15 = staticsIMEIRetention(loginIMEIs, day15RegIMEIs)
    val irate29 = staticsIMEIRetention(loginIMEIs, day29RegIMEIs)
    val irate30 = staticsIMEIRetention(loginIMEIs, day30RegIMEIs)
    val loginIMEINums = getHostIMEINum(loginIMEIs)
    val regIMEINums = getHostIMEINum(todayRegIMEIs)
    hostIDs.foreach(hostID => {
      val results = Map[String,String]("LoginNum"->loginNums.getOrElse(hostID, 0).toString, 
        "NewNum"->regNums.getOrElse(hostID, 0).toString,
        "1Days"-> rate1.getOrElse(hostID, 0).toString, "2Days"-> rate2.getOrElse(hostID, 0).toString, 
        "3Days"-> rate3.getOrElse(hostID, 0).toString, "4Days"->rate4.getOrElse(hostID, 0).toString, 
        "5Days"->rate5.getOrElse(hostID, 0).toString, "6Days"->rate6.getOrElse(hostID, 0).toString, 
        "7Days"->rate7.getOrElse(hostID, 0).toString, "10Days"->rate10.getOrElse(hostID, 0).toString,
        "13Days"->rate13.getOrElse(hostID, 0).toString, "15Days"->rate15.getOrElse(hostID, 0).toString,
        "29Days"->rate29.getOrElse(hostID, 0).toString, "30Days"->rate30.getOrElse(hostID, 0).toString)
      RetentionDB.insertIMEIRetention(platformID, hostID.toString, today, results)
    })
  }
  /**
   * 统计付费留存
   */
  def staticsPayUserRetention(platformID:String, today:String, loginUids: RDD[(Long, Int)],
      hostIDs: RDD[Int]) = {
    val todayPayUids = loadFirstPayUidFromDB(platformID, today)
    val day1 = DateUtils2.getOverDate(today, -1)
    val day2 = DateUtils2.getOverDate(today, -2)
    val day3 = DateUtils2.getOverDate(today, -3)
    val day4 = DateUtils2.getOverDate(today, -4)
    val day5 = DateUtils2.getOverDate(today, -5)
    val day6 = DateUtils2.getOverDate(today, -6)
    val day7 = DateUtils2.getOverDate(today, -7)
    val day10 = DateUtils2.getOverDate(today, -10)
    val day13 = DateUtils2.getOverDate(today, -13)
    val day15 = DateUtils2.getOverDate(today, -15)
    val day29 = DateUtils2.getOverDate(today, -29)
    val day30 = DateUtils2.getOverDate(today, -30)
    val day1PayUids = loadFirstPayUidFromDB(platformID, day1)
    val day2PayUids = loadFirstPayUidFromDB(platformID, day2)
    val day3PayUids = loadFirstPayUidFromDB(platformID, day3)
    val day4PayUids= loadFirstPayUidFromDB(platformID, day4)
    val day5PayUids = loadFirstPayUidFromDB(platformID, day5)
    val day6PayUids = loadFirstPayUidFromDB(platformID, day6)
    val day7PayUids = loadFirstPayUidFromDB(platformID, day7)
    val day10PayUids = loadFirstPayUidFromDB(platformID, day10)
    val day13PayUids = loadFirstPayUidFromDB(platformID, day13)
    val day15PayUids = loadFirstPayUidFromDB(platformID, day15)
    val day29PayUids = loadFirstPayUidFromDB(platformID, day29)
    val day30PayUids = loadFirstPayUidFromDB(platformID, day30)
    
    val rate1 = staticsUidRetentionRate(loginUids, day1PayUids)
    val rate2 = staticsUidRetentionRate(loginUids, day2PayUids)
    val rate3 = staticsUidRetentionRate(loginUids, day3PayUids)
    val rate4 = staticsUidRetentionRate(loginUids, day4PayUids)
    val rate5 = staticsUidRetentionRate(loginUids, day5PayUids)
    val rate6 = staticsUidRetentionRate(loginUids, day6PayUids)
    val rate7 = staticsUidRetentionRate(loginUids, day7PayUids)
    val rate10 = staticsUidRetentionRate(loginUids, day10PayUids)
    val rate13 = staticsUidRetentionRate(loginUids, day13PayUids)
    val rate15 = staticsUidRetentionRate(loginUids, day15PayUids)
    val rate29 = staticsUidRetentionRate(loginUids, day29PayUids)
    val rate30 = staticsUidRetentionRate(loginUids, day30PayUids)
    val loginNums = getHostNum(loginUids)
    val payNums = getHostNum(todayPayUids)
    
    hostIDs.foreach(hostID => {
      val results = Map[String,String]("LoginNum"->loginNums.getOrElse(hostID, 0).toString, 
        "FirstPayUserNum"->payNums.getOrElse(hostID, 0).toString,
        "1Days"-> rate1.getOrElse(hostID, 0).toString, "2Days"-> rate2.getOrElse(hostID, 0).toString, 
        "3Days"-> rate3.getOrElse(hostID, 0).toString, "4Days"->rate4.getOrElse(hostID, 0).toString, 
        "5Days"->rate5.getOrElse(hostID, 0).toString, "6Days"->rate6.getOrElse(hostID, 0).toString, 
        "7Days"->rate7.getOrElse(hostID, 0).toString, "10Days"->rate10.getOrElse(hostID, 0).toString,
        "13Days"->rate13.getOrElse(hostID, 0).toString, "15Days"->rate15.getOrElse(hostID, 0).toString,
        "29Days"->rate29.getOrElse(hostID, 0).toString, "30Days"->rate30.getOrElse(hostID, 0).toString)
      RetentionDB.insertPayRetention(platformID, hostID.toString, today, results)
    })
  }

  
  /**
   * 从loginLog表中加载用户uid和IMEI设备号
   */
  def loadLoginInfoFromDB(platformID:String, date:String) = {
    val tblLogin = platformID + "_log.tblLoginLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val loginRes = DBManager.query(tblLogin, options)
    val loginUids = loginRes.rdd.map(row =>{
      val uid = row.getLong(2)
      val hostID = row.getInt(1)
      (uid, hostID)
    })
    val loginIMEIs = loginRes.rdd.map(row =>{
      val phoneInfo = row.getString(9)
      val imei = getIMEI(phoneInfo)
      val hostID = row.getInt(1)
      (imei, hostID)
    })
    //还要再合并退出日志中的uid和imei\
    val tblLogout = platformID + "_log.tblLogoutLog_" + date.replace("-", "")
    val logoutRes = DBManager.query(tblLogout, options)
    val logoutUids = logoutRes.rdd.map(row => {
      val uid = row.getLong(2)
      val hostID = row.getInt(1)
      (uid, hostID)
    })
    val logoutIMEIs = logoutRes.rdd.map(row => {
      val hostID = row.getInt(1)
      val phoneInfo = row.getString(16)
      val imei = getIMEI(phoneInfo)
      (imei, hostID)
    })
    val totalUids = loginUids.union(logoutUids)
    val distinctUids = totalUids.reduceByKey((x, y) => x)
    val totalIMEIs = loginIMEIs.union(logoutIMEIs)
    val distinctIMEIs = totalIMEIs.reduceByKey((x, y) => x)
    val distinctHostIDs = distinctUids.values.distinct
    (distinctUids, distinctIMEIs, distinctHostIDs)
  }
  
  /**
   * 从tblAddPlayerLog表中获得注册玩家的uid和IMEI
   */
  def loadRegInfoFromDB(platformID:String, date:String) = {
    val tblReg = platformID + "_log.tblAddPlayerLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val regRes = DBManager.query(tblReg, options)
    val regUids = regRes.rdd.map(row => {
      val uid = row.getLong(2)
      val hostID = row.getInt(1)
      (uid, hostID)
    })
    val regIMEIs = regRes.rdd.map(row =>{
      val hostID = row.getInt(1)
      val phoneInfo = row.getString(7)
      val imei = getIMEI(phoneInfo)
      (imei, hostID)
    })
    //同时还需要登陆过的才能记录下来，需要从登陆日志中去查询
    val tblLogin = platformID + "_log.tblLoginLog_" + date.replace("-", "")
    val loginRes = DBManager.query(tblLogin, options)
    val loginUids = loginRes.rdd.map(row => {
      val uid = row.getLong(2)
      val hostID = row.getInt(1)
      (uid, hostID)
    }).reduceByKey((x, y) => x)
    val loginIMEIs = loginRes.rdd.map(row => {
      val hostID = row.getInt(1)
      val phoneInfo = row.getString(9)
      val imei = getIMEI(phoneInfo)
      (imei, hostID)
    }).reduceByKey((x, y) => x)
    val uids = regUids.join(loginUids).map({case (x, (y,_)) => (x, y)})
    val distinctUids = uids.reduceByKey((x, y) => x)
    val imeis = regIMEIs.join(loginIMEIs).map({case (x, (y, _)) => (x, y)})
    val distinctIMEIs = imeis.reduceByKey((x, y) => x)
    (distinctUids, distinctIMEIs)
  }
  /**
   * 从tblUserPayStatics表中获得首充玩家uid和HostID的对应关系
   */
  def loadFirstPayUidFromDB(platformID:String, date:String) = {
    val tblUserPayStatics = platformID + "_statics.tblUserPayStatics"
    val timeOption = "FirstCashTime >= '" + date + " 00:00:00' and FirstCashTime <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val payRes = DBManager.query(tblUserPayStatics, options)
    val payUids = payRes.rdd.map(row => {
      val hostID = row.getInt(3)
      val uid = row.getLong(0)
      (uid, hostID)
    })
    payUids
  }
  /**
   * 从phoneInfo字符串中截取第七个获得IMEI
   */
  def getIMEI(phoneInfo: String) = {
    var imei = ""
    val strs = phoneInfo.split(";")
    if (strs.length >= 8)
      imei = strs(7)
    imei
  }
  /**
   * 计算uid留存率：登陆留存，付费留存
   */
  def staticsUidRetentionRate(loginUids: RDD[(Long, Int)], regUids: RDD[(Long, Int)]) = {
    val interNums = loginUids.join(regUids).map(x => (x._2._1, x._1 )).groupByKey().map(x => (x._1, x._2.size)).collectAsMap()
    val hostRegNums = regUids.map(x => (x._2, x._1)).groupByKey().map(x => (x._1, x._2.size)).collectAsMap()
    var rates:Map[Int, Double] = Map()
    for((k, v) <- hostRegNums){
      val num = interNums.getOrElse(k, 0)
      if(num != 0 && v != 0){
        val rate = Math.round(num * 10000 / v) / 100.0
        rates(k) = rate
      }
    }
    rates
  }
  /**
   * 统计设备留存率
   */
  def staticsIMEIRetention(loginIMEIs: RDD[(String, Int)], regIMEIs: RDD[(String, Int)]) = {
    val interNums = loginIMEIs.join(regIMEIs).map(x => (x._2._1, x._1 )).groupByKey().map(x => (x._1, x._2.size)).collectAsMap()
    val hostRegNums = regIMEIs.map(x => (x._2, x._1)).groupByKey().map(x => (x._1, x._2.size)).collectAsMap()
    var rates:Map[Int, Double] = Map()
    for((k, v) <- hostRegNums){
      val num = interNums.getOrElse(k, 0)
      if(num != 0 && v != 0){
        val rate = Math.round(num * 10000 / v) / 100.0
        rates(k) = rate
      }
    }
    rates
  }
  /**
   * 从RDD中统计出每个HostID中uid的个数并返回map
   */
  def getHostNum(uids:RDD[(Long, Int)]) = {
    val hostUids = uids.map(x => (x._2, x._1)).groupByKey().map(x => {
      val count = x._2.size
      (x._1, count)
    }).collectAsMap()
    hostUids
  }
  /**
   * 从RDD中统计出每个HostId中IMEI的个数并返回map
   */
  def getHostIMEINum(imeis:RDD[(String, Int)]) = {
    val hostIMEIs = imeis.map(x => (x._2, x._1)).groupByKey().map(x => {
      val count = x._2.size
      (x._1, count)
    }).collectAsMap()
  }
}