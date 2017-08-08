package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.LoginStaticsDB
import com.yuhe.mgame.db.ImeiInfoDB
import com.yuhe.mgame.db.StartNumDB
import com.yuhe.mgame.utils.DateUtils2
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{ Set => MutableSet }
import scala.collection.mutable.{ Map => MutableMap }
import scala.collection.mutable.ArrayBuffer

object LoginStatics extends Serializable with StaticsTrait {
  val LOGIN_STANDARD_ID = 15 //以此登陆步骤ID的uid为标准，当天不在这里面的玩家不统计
  val DEVICE_STANDARD_ID = 1 //设备登陆标准ID
  val WWW_HOSTID = 666 //设备登陆流程分析记录的HostID

  def statics(platformID: String, today: String) = {
    //统计前一个小时的登陆过程分析
    val lastHour = DateFormatUtils.format(System.currentTimeMillis() - 3600000, "HH")
    userStatics(platformID, today, lastHour)
    deviceStatics(platformID, today, lastHour)
    staticsStartNum(platformID, today)
  }

  /**
   * 统计玩家登陆过程分析
   */
  def userStatics(platformID: String, date: String, hour: String) = {
    //先获得标准登陆步骤ID的uid列表
    val standardUidSet = getStandardUid(platformID, date)
    val options = Map(
      "StartTime" -> (date + " " + hour + ":00:00"),
      "EndTime" -> (date + " " + hour + ":59:59"))
    val uidMap = loadClientLoadLogFromDB(platformID, date, options)
    for ((hostID, infoList) <- uidMap) {
      val hostMap = MutableMap[Int, MutableSet[Long]]()
      for (info <- infoList) {
        val step = info._1
        val uid = info._2
        if (standardUidSet.contains(uid)) {
          //只统计在标准登陆步骤ID中的玩家
          hostMap(step) = hostMap.getOrElse(step, MutableSet[Long]())
          hostMap(step) += uid
        }
      }
      val numMap = MutableMap[Int, Int]()
      for ((stepID, uidSet) <- hostMap) {
        val size = uidSet.size
        numMap(stepID) = size
      }
      LoginStaticsDB.insert(platformID, hostID.toString, date, hour, numMap)
    }
  }
  /**
   * 获得标准步骤的玩家uid列表
   */
  def getStandardUid(platformID: String, today: String) = {
    val options = Map(
      "StartTime" -> (today + " 00:00:00"),
      "EndTime" -> (today + " 23:59:59"),
      "Step" -> LOGIN_STANDARD_ID.toString)
    val uidMap = loadClientLoadLogFromDB(platformID, today, options)
    val uidSet = MutableSet[Long]()
    for ((hostID, infoList) <- uidMap) {
      for (info <- infoList) {
        uidSet += info._2
      }
    }
    uidSet
  }
  /**
   * 从clientLoadLog表中加载登陆过程日志
   */
  def loadClientLoadLogFromDB(platformID: String, date: String, options: Map[String, String]) = {
    val tblName = platformID + "_log.tblClientLoadLog_" + date.replace("-", "")
    var optionStr = "Time >= '" + options("StartTime") + "' and Time <= '" + options("EndTime") + "'"
    if (options.contains("Step")) {
      optionStr += " and Step = '" + options("Step") + "'"
    }
    val selectOptions = Array(optionStr)
    val clientLoadRes = DBManager.query(tblName, selectOptions)
    clientLoadRes.select("HostID", "Uid", "Step").rdd.map(row => {
      val hostID = row.getInt(0)
      val uid = row.getLong(1)
      val step = row.getInt(2)
      (hostID, (step, uid))
    }).groupByKey.collectAsMap
  }
  /**
   * 统计设备登陆流程分析
   */
  def deviceStatics(platformID: String, date: String, hour: String) = {
    //先获得昨天之前的IMEI设备号
    val yesterdayImeiMap = loadIMEIFromDB(platformID, Map("EndTime" -> (date + " 00:00:00"), "Step" -> DEVICE_STANDARD_ID.toString))
    //再获得今天登陆过的设备号
    val todayImeiMap = loadIMEIFromDB(platformID, Map("StartTime" -> (date + " 00:00:00"), "EndTime" -> (date + " " + hour + "59:59")))
    //重新将今天的设备号封装一下
    val todayImeiSet = MutableMap[String, MutableSet[Int]]()
    for ((imei, list) <- todayImeiMap) {
      todayImeiSet(imei) = MutableSet[Int]()
      for (step <- list) {
        todayImeiSet(imei) += step
      }
    }
    //获得设备登陆流程日志
    val startTime = date + " " + hour + ":00:00"
    val endTime = date + " " + hour + ":59:59"
    val logMap = loadWwwLogFromDB(platformID, date, startTime, endTime)
    val resultMap = MutableMap[Int, MutableSet[String]]()
    for ((imei, list) <- logMap) {
      if (!yesterdayImeiMap.contains(imei)) {
        if (!todayImeiSet.contains(imei)) {
          //今天第一次出现的，把这些登陆步骤都加入进去
          for (step <- list) {
            resultMap(step) = resultMap.getOrElse(step, MutableSet[String]())
            resultMap(step) += imei
          }
        } else {
          //今天已经出现过了，判断有没有新的登陆步骤
          val stepSet = todayImeiSet(imei)
          for (step <- list) {
            if (!stepSet.contains(step)) {
              resultMap(step) = resultMap.getOrElse(step, MutableSet[String]())
              resultMap(step) += imei
            }
          }
        }
      }
    }
    //记录入库
    val numMap = MutableMap[Int, Int]()
    for ((step, imeiSet) <- resultMap) {
      numMap(step) = imeiSet.size
    }
    if (numMap.size > 0)
      LoginStaticsDB.insert(platformID, WWW_HOSTID.toString, date, hour, numMap)
    if (resultMap.size > 0) {
      //同时还要记录到tblIMEIInfo中
      val nowTime = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss")
      ImeiInfoDB.insert(platformID, nowTime, resultMap)
    }
  }
  /**
   * 从tblIMEIInfo加载之前登陆过游戏的设备号
   */
  def loadIMEIFromDB(platformID: String, options: Map[String, String]) = {
    val tblName = platformID + "_statics.tblIMEIInfo"
    var option = ArrayBuffer[String]()
    if (options.contains("StartTime")) {
      option += ("Time >= '" + options("StartTime") + "'")
    }
    if (options.contains("EndTime")) {
      option += ("Time <= '" + options("EndTime") + "'")
    }
    if (options.contains("Step")) {
      option += ("Step = '" + options("Step") + "'")
    }
    val selectOptions = Array(option.mkString(" and "))
    val imeiRes = DBManager.query(tblName, selectOptions)
    imeiRes.select("IMEI", "Step").rdd.map(row => {
      val imei = row.getString(0)
      val step = row.getInt(1)
      (imei, step)
    }).groupByKey.collectAsMap
  }
  /**
   * 从tblWwwLog中加载设备登陆过程日志
   */
  def loadWwwLogFromDB(platformID: String, date: String, startTime: String, endTime: String) = {
    val tblName = platformID + "_log.tblWwwLog_" + date.replace("-", "")
    val option = "Time >= '" + startTime + "' and Time <= '" + endTime + "'"
    val options = Array(option)
    val logRes = DBManager.query(tblName, options)
    logRes.select("Step", "IMEI").rdd.map(row => {
      val step = row.getInt(0)
      val imei = row.getString(1)
      (imei, step)
    }).groupByKey.collectAsMap
  }

  /**
   * 统计启动次数
   */
  def staticsStartNum(platformID: String, date: String) = {
    val startTime = date + " 00:00:00"
    val endTime = date + " 23:59:59"
    val phoneInfoMap = loadPhonInfoFromDB(platformID, date, startTime, endTime)
    val modelResult = MutableMap[String, Int]()
    val brandResult = MutableMap[String, Int]()
    val dpiResult = MutableMap[String, Int]()
    for ((phoneInfo, 1) <- phoneInfoMap) {
      val phoneInfos = phoneInfo.split(";")
      if (phoneInfos.size >= 7) {
        val model = phoneInfos(0)
        val brand = phoneInfos(1)
        val dpi = phoneInfos(5) + "*" + phoneInfos(6)
        modelResult(model) = modelResult.getOrElse(model, 0)
        modelResult(model) += 1
        brandResult(brand) = brandResult.getOrElse(brand, 0)
        brandResult(brand) += 1
        dpiResult(dpi) = dpiResult.getOrElse(dpi, 0)
        dpiResult(dpi) += 1
      }
    }
    //记录入库
    for ((model, num) <- modelResult) {
      StartNumDB.insert(platformID, WWW_HOSTID.toString, date, "Model", model, num)
    }
    for ((brand, num) <- brandResult) {
      StartNumDB.insert(platformID, WWW_HOSTID.toString, date, "Brand", brand, num)
    }
    for ((dpi, num) <- dpiResult) {
      StartNumDB.insert(platformID, WWW_HOSTID.toString, date, "DPI", dpi, num)
    }
  }

  def loadPhonInfoFromDB(platformID: String, date: String, startTime: String, endTime: String) = {
    val tblName = platformID + "_log.tblWwwLog_" + date.replace("-", "")
    val option = "Time >= '" + startTime + "' and Time <= '" + endTime + "'"
    val options = Array(option)
    val logRes = DBManager.query(tblName, options)
    logRes.select("PhoneInfo").rdd.map(row => {
      val phoneInfo = row.getString(0)
      (phoneInfo, 1)
    }).reduceByKey((x, y) => x).collectAsMap
  }
}