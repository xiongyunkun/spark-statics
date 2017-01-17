package com.yuhe.mgame.statics

import org.apache.spark._
import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.HistroyRegDB
import com.yuhe.mgame.utils.DateUtils2

/**
 * 统计历史注册情况
 */
object HistoryReg extends Serializable with StaticsTrait {
  def statics(sc: SparkContext, platformID: String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val hostUidMap = loadRegInfoFromDB(platformID, today)
    val yesterday = DateUtils2.getOverDate(today, -1)
    val yesterdayRegMap = loadRegStaticsFromDB(platformID, yesterday)
    for((hostID, uidList) <- hostUidMap){
      val todayNum = uidList.size //今天的注册人数
      val totalNum = todayNum + yesterdayRegMap.getOrElse(hostID, 0) //历史总注册人数
      //插入数据库
      HistroyRegDB.insert(platformID, hostID, today, todayNum, totalNum)
    }
  }
  
  /**
   * 查询blAddPlayerLog表获得注册玩家列表 
   */
  def loadRegInfoFromDB(platformID:String, date:String) = {
    val tblReg = platformID + "_log.tblAddPlayerLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val regRes = DBManager.query(tblReg, options)
    regRes.rdd.map(row => {
      val hostID = row.getInt(1)
      val uid = row.getLong(2)
      (hostID, uid)
    }).groupByKey().collectAsMap()
  }
  
  /**
   * 查询tblHistoryReg获得历史注册信息，需要进行历史注册人数的汇总
   */
  def loadRegStaticsFromDB(platformID:String, date:String) = {
    val tblName = platformID + "_statics.tblHistoryReg"
    val timeOption = "Date = '" + date + "'"
    val options = Array(timeOption)
    val regRes = DBManager.query(tblName, options)
    regRes.rdd.map(row =>{
      val hostID = row.getInt(1)
      val totalRegNum = row.getInt(5)
      (hostID, totalRegNum)
    }).collectAsMap()
  }
}