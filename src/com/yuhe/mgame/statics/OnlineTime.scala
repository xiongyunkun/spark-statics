package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.OnlineTimeDB
import scala.util.control._
import scala.collection.mutable.{Map => MutableMap}

/**
 * 统计新老玩家在线时长情况
 */
object OnlineTime extends Serializable with StaticsTrait {
  def statics(platformID: String, today: String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val logoutRes = loadLogoutInfoFromDB(platformID, today)
    logoutRes.cache() //下面有2个action操作，要缓存一下
    //先计算出uid和hostID的对应关系
    val hostUidMap = logoutRes.select("HostID", "Uid").rdd.map(row => {
      val uid = row.getLong(1)
      val hostID = row.getInt(0)
      (hostID, uid)
    }).groupByKey().collectAsMap()
    //再计算出每个uid今天的在线总时长
    val uidOnTimeMap = logoutRes.select("Uid", "OnTime").rdd.map(row => {
      val uid = row.getLong(0)
      val ontime = row.getInt(1)
      (uid, ontime)
    }).reduceByKey((x, y) => x + y).collectAsMap()
    val newUidMap = loadRegInfoFromDB(platformID, today)
    for((hostID, uidArray) <- hostUidMap){
      val newUidResults = MutableMap[String, Int]()
      val oldUidResults = MutableMap[String, Int]()
      var newUidNum = 0
      var oldUidNum = 0
      var newTotalTime = 0
      var oldTotalTime = 0
      for(uid <- uidArray){
        val time = uidOnTimeMap.getOrElse(uid, 0)
        val period = getPeriodID(time)
//        println(time+","+period)
        if(newUidMap.contains(uid)){
          newUidResults(period) = newUidResults.getOrElse(period, 1)
          newUidNum += 1
          newTotalTime += time
        }else{
          oldUidResults(period) = oldUidResults.getOrElse(period, 1)
          oldUidNum += 1
          oldTotalTime += time
        }
      }
      newUidResults("TotalTimes") = newTotalTime
      newUidResults("TotalPlayers") = newUidNum
      oldUidResults("TotalTimes") = oldTotalTime
      oldUidResults("TotalPlayers") = oldUidNum
      //记录入库
      OnlineTimeDB.insert(platformID, hostID, today, 1, oldUidResults)
      OnlineTimeDB.insert(platformID, hostID, today, 2, newUidResults)
    }
  }
  /**
   * 从数据库blLogoutLog表中加载会哦的玩家在线时长信息
   */
  def loadLogoutInfoFromDB(platformID: String, date: String) = {
    val tblName = platformID + "_log.tblLogoutLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    DBManager.query(tblName, options)
  }
  /**
   * 从数据库tblAddPlayerLog表中加载获得当天注册的新玩家列表，并且封装成(uid,hostID)格式返回
   */
  def loadRegInfoFromDB(platformID: String, date: String) = {
    val tblReg = platformID + "_log.tblAddPlayerLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val regRes = DBManager.query(tblReg, options)
    regRes.select("HostID", "Uid").rdd.map(row => {
      val uid = row.getLong(1)
      val hostID = row.getInt(0)
      (uid, hostID)
    }).collectAsMap()
  }
  
  /**
   * 获得登陆时长区间
   */
  def getPeriodID(time: Int) = {
    val periods = Map(
      "Time0" -> Array(0, 1),
      "Time1" -> Array(1, 5),
      "Time5" -> Array(5, 10),
      "Time10" -> Array(10, 15),
      "Time15" -> Array(15, 30),
      "Time30" -> Array(30, 45),
      "Time45" -> Array(45, 60),
      "Time60" -> Array(60, 90),
      "Time90" -> Array(90, 120),
      "Time120" -> Array(120, 150),
      "Time150" -> Array(150, 180),
      "Time180" -> Array(180, 240),
      "Time240" -> Array(240, 300),
      "Time300" -> Array(300, 360),
      "Time360" -> Array(360, 420),
      "Time420" -> Array(420, 600),
      "Time600" -> Array(600, 900),
      "Time900" -> Array(900, 1200),
      "Time1200" -> Array(1200))
    val loop = new Breaks
    var periodID = "Time0" //默认值是Time0
    loop.breakable{
      for((name, array) <- periods){
        val length = array.size
        if(length == 1 && time >= (array(0) * 60)){
          periodID = name
          loop.break
        }else if(length == 2 && time>= (array(0) * 60) && time < (array(1) * 60)){
          periodID = name
          loop.break
        }
      }
    }
    periodID
  }
}