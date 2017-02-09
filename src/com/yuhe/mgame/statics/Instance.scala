package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.InstanceDB
import collection.mutable.Map
import collection.mutable.ArrayBuffer

/**
 * 关卡达成统计
 */
object Instance extends Serializable with StaticsTrait {

  val StageId2Id = Map(200001 -> 1, 200002 -> 1, 200003 -> 1, 200004 -> 1, 200005 -> 1, 200006 -> 1,
    200007 -> 1, 200008 -> 1, 210001 -> 2, 210002 -> 2, 210003 -> 2, 210004 -> 2, 210005 -> 2, 210006 -> 2,
    210007 -> 2, 210008 -> 2, 220001 -> 3, 220002 -> 3, 220003 -> 3, 220004 -> 3, 220005 -> 3, 220006 -> 3,
    220007 -> 3, 220008 -> 3, 230001 -> 4, 230002 -> 4, 230003 -> 4, 230004 -> 4, 230005 -> 4, 230006 -> 4,
    230007 -> 4, 230008 -> 4, 240001 -> 5, 240002 -> 5, 240003 -> 5, 240004 -> 5, 240005 -> 5, 240006 -> 5,
    240007 -> 5, 240008 -> 5)

  def statics(platformID: String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val instLogMap = loadInstanceLogFromDB(platformID, today)
    val regMap = loadRegNumFromDB(platformID, today)
    for((hostID, instLogList) <- instLogMap){
      val playUids = Map[Int, Map[Long, Int]]() //记录参与Uid
      val playNums = Map[Int, Int]() //记录参与人次
      val instResults = Map[Int, Map[String, Int]]()
      for(instLog <- instLogList){
        val uid = instLog._1
        val stageID = instLog._2
        val stageType = instLog._3
        val isFirstWin = instLog._4
        val isFirstEnter = instLog._5
        if(stageType == "活动关卡"){
          if(StageId2Id.contains(stageID)){
            val instType = StageId2Id(stageID)
            playUids(instType) = playUids.getOrElse(instType, Map[Long, Int]())
            val uids = playUids(instType)
            uids(uid) = 1
            playNums(instType) = playNums.getOrElse(instType, 0) + 1
          }
        }else{ //普通关卡和精英关卡
          if(isFirstWin == 1){
            instResults(stageID) = instResults.getOrElse(stageID, Map("PassNum" -> 0, "TotalNum" -> 0))
            instResults(stageID)("PassNum") = instResults(stageID)("PassNum") + 1
          }
          if(isFirstEnter == 1){
            instResults(stageID) = instResults.getOrElse(stageID, Map("PassNum" -> 0, "TotalNum" -> 0))
            instResults(stageID)("TotalNum") = instResults(stageID)("TotalNum") + 1
          }
        }        
      }
      //记录数据库
      val regNum = regMap.getOrElse(hostID, 0)
      for((stageID, numInfo) <- instResults){
        val totalNum = numInfo.getOrElse("TotalNum", 0)
        val passNum = numInfo.getOrElse("PassNum", 0)
        InstanceDB.insertInstance(platformID, hostID, today, stageID, totalNum, passNum, regNum)
      }
      //还要记录副本参与统计表
      for((instType, num) <- playNums){
        val uids = playUids(instType)
        val uidNum = uids.size
        InstanceDB.insertInstancePlay(platformID, hostID, today, instType, num, uidNum)
      }
    }
  }
  /**
   * 从tblInstanceLog表中获得关卡日志数据
   */
  def loadInstanceLogFromDB(platformID: String, date: String) = {
    val tblName = platformID + "_log.tblInstanceLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val instanceRes = DBManager.query(tblName, options)
    instanceRes.select("HostID", "Uid", "StageId", "StageType", "IsFirstWin", "IsFirstEnter").rdd.map(row => {
      (row.getInt(0), (row.getLong(1), row.getInt(2), row.getString(3), row.getInt(4), row.getInt(5)))
    }).groupByKey.collectAsMap
  }
  /**
   * 查询tblInstance表获得之前的统计数据
   */
  def loadInstanceFromDB(platformID: String, date: String) = {
    val tblName = platformID + "_statics.tblInstance"
    val timeOption = "Date = '" + date + "'"
    val options = Array(timeOption)
    val instanceRes = DBManager.query(tblName, options)
    instanceRes.select("HostID", "StageId", "TotalNum", "PassNum").rdd.map(row => {
      (row.getInt(0), (row.getInt(1), row.getInt(2), row.getInt(3)))
    }).groupByKey.collectAsMap
  }
  /**
   * 从tblHistoryReg表中获得注册人数
   */
  def loadRegNumFromDB(platformID: String, date: String) = {
    val tblName = platformID + "_statics.tblHistoryReg"
    val timeOption = "Date = '" + date + "'"
    val options = Array(timeOption)
    val regRes = DBManager.query(tblName, options)
    regRes.select("HostID", "TotalRegNum").rdd.map(row => {
      (row.getInt(0), row.getInt(1))
    }).reduceByKey((x, y) => x).collectAsMap
  }
}