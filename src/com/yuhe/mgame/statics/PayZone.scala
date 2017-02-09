package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.PayZoneDB
import scala.util.control._
import collection.mutable.Map

/**
 * 统计充值区间
 */
object PayZone extends Serializable with StaticsTrait {
  def statics(platformID: String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val goldMap = loadUserPayDayFromDB(platformID, today)
    for ((hostID, goldList) <- goldMap) {
      val results = Map[Int, Int]()
      for (gold <- goldList) {
        val zoneID = getZoneID(gold)
        results(zoneID) = results.getOrElse(zoneID, 0) + 1
      }
      //记录数据库
      PayZoneDB.insert(platformID, hostID, today, results)
    }
  }
  /**
   * 从tblUserPayDayStatics获得充值钻石
   */
  def loadUserPayDayFromDB(platformID: String, date: String) = {
    val tblName = platformID + "_statics.tblUserPayDayStatics"
    val timeOption = "Date = '" + date + "'"
    val options = Array(timeOption)
    val payRes = DBManager.query(tblName, options)
    payRes.select("HostID", "TotalGold").rdd.map(row => {
      val hostID = row.getInt(0)
      val gold = row.getInt(1)
      (hostID, gold)
    }).groupByKey.collectAsMap
  }
  /**
   * 获得充值区间ID
   */
  def getZoneID(gold: Int) = {
    val zones = Array(Array(0, 9), Array(10, 49), Array(50, 99), Array(100, 199), Array(200, 499),
      Array(500, 999), Array(1000, 1999), Array(2000, 4999), Array(5000, 9999), Array(10000, 19999),
      Array(20000, 49999), Array(50000))
    val loop = new Breaks
    var zoneID = 1 //默认是1
    var index = 1
    loop.breakable {
      for (zone <- zones) {
        if (zone.length == 2 && gold >= zone(0) && gold < zone(1)) {
          zoneID = index
          loop.break
        } else if (zone.length == 1 && gold >= zone(0)) {
          zoneID = index
          loop.break
        }
        index += 1
      }
    }
    zoneID
  }
}