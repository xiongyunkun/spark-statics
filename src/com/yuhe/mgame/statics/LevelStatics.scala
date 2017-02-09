package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.LevelStaticsDB
import collection.mutable.Map
import com.yuhe.mgame.utils.DateUtils2
import collection.mutable.ArrayBuffer
/**
 * 玩家等级统计，统计每个等级的玩家人数，活跃人数，流失人数，充值人数，充值流失人数
 */
object LevelStatics extends Serializable with StaticsTrait{
  def statics(platformID: String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val userInfos = loadUserInfoFromDB(platformID, today)
    val loginMap = loadLoginInfoFromDB(platformID, today)
    val ontimeMap = loadOnTimeFromDB(platformID, today)
    val payMap = loadUserPayDayInfoFromDB(platformID, today)
    for((hostID, leveArray) <- userInfos){
      val levelResults = Map[Int, Map[String, Int]]()
      for(info <- leveArray){
        val uid = info._1
        val level = info._2
        var levelResult:Map[String, Int] = null
        if(levelResults.contains(level)){
          levelResult = levelResults(level)
        }else{
          levelResult = Map[String, Int]()
          levelResults(level) = levelResult
        }
        levelResult("TotalNum") = levelResult.getOrElse("TotalNum", 0) + 1
        if(!loginMap.contains(uid)){
          levelResult("TotalLostNum") = levelResult.getOrElse("TotalLostNum", 0) + 1
        }
        if(ontimeMap.contains(uid) && ontimeMap(uid) >= 14400){ //大于4小时的才算活跃用户
          levelResult("LivelNum") = levelResult.getOrElse("LivelNum", 0) + 1
          if(payMap.contains(uid)){
            levelResult("PayLivelNum") = levelResult.getOrElse("PayLivelNum", 0) + 1
          }
        }
        if(payMap.contains(uid)){
          levelResult("PayNum") = levelResult.getOrElse("PayNum", 0) + 1
        }
      }
      //记录数据库
      for((level, levelResult) <- levelResults){
        LevelStaticsDB.insert(platformID, hostID, today, level, levelResult)
      }
    }
    /*
     * 然后再计算昨天的流失率
     */
    val yesterday = DateUtils2.getOverDate(today, -1)
    val yesterdayLoginMap = loadLoginInfoFromDB(platformID, yesterday)
    val (yesterdayOntimeMap, yesterdayLevelMap) = loadLogoutInfoFromDB(platformID, yesterday)
    val yeserdayPayMap = loadUserPayDayInfoFromDB(platformID, yesterday)
    val yesterdayHostUidMap = Map[Int, ArrayBuffer[Long]]()
    //先整理成Map[hostID, Array[uid]]的格式
    val yesterdayTotalMap = yesterdayLoginMap ++ yeserdayPayMap
    for((uid, hostID) <- yesterdayTotalMap){
      val array = yesterdayHostUidMap.getOrElse(hostID, ArrayBuffer[Long]())
      array += uid
      yesterdayHostUidMap(hostID) = array
    }
    for((hostID, uidArray) <- yesterdayHostUidMap){
      val levelResults = Map[Int, Map[String, Int]]()
      for(uid <- uidArray){
        val ontime = yesterdayOntimeMap.getOrElse(uid, 0)
        if(ontime >= 14400 && !loginMap.contains(uid)){
          val level = yesterdayLevelMap(uid)
          val levelResult = levelResults.getOrElse(level, Map[String, Int]())
          levelResult("LiveLostNum") = levelResult.getOrElse("LiveLostNum", 0) + 1 //活跃流失
          if(yeserdayPayMap.contains(uid)){
            levelResult("PayLiveLostNum") = levelResult.getOrElse("PayLiveLostNum", 0) + 1 //活跃付费流失
          }
          levelResults(level) = levelResult
        }
        //计算付费流失
        if(yeserdayPayMap.contains(uid) && !loginMap.contains(uid) && yesterdayLevelMap.contains(uid)){
          val level = yesterdayLevelMap(uid)
          val levelResult = levelResults.getOrElse(level, Map[String, Int]())
          levelResult("PayLostNum") = levelResult.getOrElse("PayLostNum", 0) + 1
          levelResults(level) = levelResult
        }
      }
      //记录入库，更新昨天的等级统计
      for((level, levelResult) <- levelResults){
        LevelStaticsDB.update(platformID, hostID, yesterday, level, levelResult)
      }
    }
  }
  /**
   * 从tblUserInfo表中获得玩家信息
   */
  def loadUserInfoFromDB(platformID:String, maxRegTime:String) = {
    val tblName = platformID + "_statics.tblUserInfo"
    val timeOption = "RegTime <= '" + maxRegTime + " 23:59:59'"
    val options = Array(timeOption)
    val userRes = DBManager.query(tblName, options)
    userRes.select("HostID", "Uid", "Level").rdd.map(row => {
      val hostID = row.getInt(0)
      val uid = row.getLong(1)
      val level = row.getInt(2)
      (hostID, (uid, level))
    }).groupByKey.collectAsMap
  }
  /**
   * 从tblLoginlog表中获得登陆信息,返回uid与hostID的map
   */
  def loadLoginInfoFromDB(platformID:String, date:String) = {
    val tblName = platformID + "_log.tblLoginLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val loginRes = DBManager.query(tblName, options)
    loginRes.select("HostID", "Uid").rdd.map(row => {
      val hostID = row.getInt(0)
      val uid = row.getLong(1)
      (uid, hostID)
    }).reduceByKey((x, y) => x).collectAsMap
  }
  /**
   * 从tblLogoutlog表中获得在线时长，返回uid与在线时长的map
   */
  def loadOnTimeFromDB(platformID:String, date:String) = {
    val tblName = platformID + "_log.tblLogoutLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val logoutRes = DBManager.query(tblName, options)
    logoutRes.select("Uid", "OnTime").rdd.map(row => {
      val uid = row.getLong(0)
      val ontime = row.getInt(1)
      (uid, ontime)
    }).reduceByKey((x, y) => x+y).collectAsMap
  }
  
  /**
   * 从tblLogoutlog获得玩家的在线时长，等级等信息，并且返回tuple
   */
  def loadLogoutInfoFromDB(platformID:String, date:String) = {
    val tblName = platformID + "_log.tblLogoutLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val logoutRes = DBManager.query(tblName, options)
    logoutRes.persist //缓存一下
    val ontimeMap = logoutRes.select("Uid", "OnTime").rdd.map(row => {
      val uid = row.getLong(0)
      val ontime = row.getInt(1)
      (uid, ontime)
    }).reduceByKey((x, y) => x+y).collectAsMap
    //再获得每个玩家uid和等级level的映射关系
    val levelMap = logoutRes.select("Uid", "Level").rdd.map(row => {
      val uid = row.getLong(0)
      val level = row.getInt(1)
      (uid, level)
    }).reduceByKey((x, y) => {if(x>y)x else y}).collectAsMap
    (ontimeMap, levelMap)
  }
  /**
   * 从tblUserPayDayStatics表中加载当天玩家充值信息，并且返回uid与hostID的map
   */
  def loadUserPayDayInfoFromDB(platformID:String, date:String) = {
    val tblName = platformID + "_statics.tblUserPayDayStatics"
    val options = Array("Date = '" + date + "'")
    val payRes = DBManager.query(tblName, options)
    payRes.select("HostID", "Uid").rdd.map(row => {
      val hostID = row.getInt(0)
      val uid = row.getLong(1)
      (uid, hostID)
    }).reduceByKey((x, y) => x).collectAsMap
  }
}