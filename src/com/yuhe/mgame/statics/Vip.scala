package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.VipDB
import collection.mutable.Map

object Vip extends Serializable with StaticsTrait {

  def statics(platformID: String, today: String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val vipMap = loadVipFromDB(platformID, today)
    for ((hostID, vipArray) <- vipMap) {
      val vipNumMap = Map[Int, Set[Long]]() //记录VIp等级人数
      val nowVipMap = Map[Int, Set[Long]]() //记录现有激活的VIp等级人数
      for (info <- vipArray) {
        val uid = info._1
        val vipLevel = info._2
        val isVip = info._3
        //先算VIp等级人数
        var vipSet = vipNumMap.getOrElse(vipLevel, Set[Long]())
        vipSet += uid
        vipNumMap(vipLevel) = vipSet
        //再计算VIp激活人数
        if (isVip == 1) {
          var nowVipSet = nowVipMap.getOrElse(vipLevel, Set[Long]())
          nowVipSet += uid
          nowVipMap(vipLevel) = nowVipSet
        }
      }
      //记录数据库
      for ((vipLevel, vipSet) <- vipNumMap) {
        var nowVipNum = 0
        if (nowVipMap.contains(vipLevel)) {
          nowVipNum = nowVipMap(vipLevel).size
        }
        val vipNum = vipSet.size
        val vipResult = Map("VipNum" -> vipNum, "NowVipNum" -> nowVipNum)
        VipDB.insert(platformID, hostID, today, vipLevel, vipResult)
      }
    }
  }
  /**
   * 从退出日志tblLogoutLog中获得VIp统计信息
   */
  def loadVipFromDB(platformID: String, date: String) = {
    val tblName = platformID + "_log.tblLogoutLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val logoutRes = DBManager.query(tblName, options)
    val vipRDD = logoutRes.rdd.filter(row => row.getInt(8) != 0) //过滤vip等级不为0的
    logoutRes.filter(logoutRes("VipLevel") !== 0).select("HostID", "Uid", "VipLevel", "IsVip").rdd.map(row => {
      val hostID = row.getInt(0)
      val uid = row.getLong(1)
      val vipLevel = row.getInt(2)
      val isVip = row.getInt(3)
      (hostID, (uid, vipLevel, isVip))
    }).groupByKey.collectAsMap
  }
}