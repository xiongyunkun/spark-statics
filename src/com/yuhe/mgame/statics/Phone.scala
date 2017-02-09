package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.PhoneDB
import collection.mutable.Map

/**
 * 手机设备型号统计
 */
object Phone extends Serializable with StaticsTrait{
  
  def statics(platformID: String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val imeiList = loadLoginIMEIFromDB(platformID, today)
    for((hostID, imeiArray) <- imeiList){
      val imeiResults = Map[String, Map[String, String]]()
      for(info <- imeiArray){
        val uid = info._1
        val phoneInfo = info._2
        val strArray = phoneInfo.split(";")
        if(strArray.size >= 9){
          val result = Map[String, String]()
          val imei = strArray(7)
          result("Model") = strArray(0)
          result("Brand") = strArray(1)
          result("SysVer") = strArray(2)
          result("SdkVer") = strArray(3)
          result("SimOperation") = strArray(4)
          result("DPI") = strArray(5) + "*" + strArray(6)
          result("IMEI") = imei
          result("IMSI") = strArray(8)
          result("Uids") = uid.toString
          imeiResults(imei) = result
        }
      }
      //记录数据库
      PhoneDB.insert(platformID, hostID, today, imeiResults)
    }
  }
  /**
   * 从tblLoginLog表中获得玩家设备信息
   */
  def loadLoginIMEIFromDB(platformID:String, date:String) = {
    val tblName = platformID + "_log.tblLoginLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val loginRes = DBManager.query(tblName, options)
    loginRes.select("HostID", "Uid", "PhoneInfo").rdd.map(row => {
      val hostID = row.getInt(0)
      val uid = row.getLong(1)
      val phoneInfo = row.getString(2)
      (hostID, (uid, phoneInfo))
    }).groupByKey.collectAsMap
  }
}