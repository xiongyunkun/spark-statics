package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import org.apache.spark.rdd.RDD
import java.util.Calendar
import com.yuhe.mgame.db.HistoryOnlineDB

/**
 * 统计历史在线信息，统计当日最高在线，最低在线，平均在线
 */
object HistoryOnline extends Serializable with StaticsTrait{
  
   def statics(platformID:String, today: String) = {
     val onlineNums = loadOnlineInfoFromDB(platformID, today)
     val period = getPeriod(today)
     val result = onlineNums.mapValues(x => {
       val array = x.toArray
       (array.max, array.min, Math.floorDiv(array.sum, period))
     }).collectAsMap
     for((hostID, numInfo) <- result){
       val (maxOnline, minOnline, aveOnline) = numInfo
       HistoryOnlineDB.insert(platformID, hostID, today, maxOnline, minOnline, aveOnline)
     }
   }
   /**
    * 从blOnline表中获取在线人数，并且返回HostID和在线人数列表的RDD 
    */
   def loadOnlineInfoFromDB(platformID:String, date:String) = {
     val tblName = platformID + "_statics.tblOnline"
     val timeOptions = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
     val options = Array(timeOptions)
     val onlineRes = DBManager.query(tblName, options)
     onlineRes.select("HostID", "OnlineNum").rdd.map(row => {
       val hostID = row.getInt(0)
       val onlineNum = row.getInt(1)
       (hostID, onlineNum)
     }).groupByKey
   }
   /**
    * 计算从今天0点开始到现在为止过了多少个5分钟
    */
   def getPeriod(date:String) = {
     //获得当天0点时间戳
     val benCal = Calendar.getInstance()
     benCal.set(Calendar.HOUR_OF_DAY, 0)
		 benCal.set(Calendar.SECOND, 0)
		 benCal.set(Calendar.MINUTE, 0)
		 benCal.set(Calendar.MILLISECOND, 0)
		 val benTime = benCal.getTimeInMillis()
		 //获得当前时间戳
		 val cal = Calendar.getInstance()
		 val nowTime = cal.getTimeInMillis()
		 val diff = nowTime - benTime
		 Math.floorDiv(diff, 300000)
   }
}