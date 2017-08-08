package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.GoldDB
import collection.mutable.ArrayBuffer

/**
 * 统计钻石产出和消费情况
 */
object Gold extends Serializable with StaticsTrait {

  def statics(platformID: String, today: String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val goldMap = loadGoldInfoFromDB(platformID, today)
    for ((hostID, goldArray) <- goldMap) {
      var resultMap = collection.mutable.Map[String, collection.mutable.Map[Int, Int]]()
      var addUidSet = collection.mutable.Set[Long]() //新增钻石用户列表
      val subUidSet = collection.mutable.Set[Long]() //消费钻石用户列表
      for (tuple <- goldArray) {
        val uid = tuple._1
        val changes = tuple._2
        val reason = tuple._3
        var reasonMap = resultMap.getOrElse(reason, collection.mutable.Map(1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0))
        //1:sub,2:add,3:consumeNumbers,4:addNumbers
        if (changes > 0) {
          reasonMap(2) = reasonMap(2) + changes
          addUidSet += uid
          reasonMap(4) = reasonMap(4) + 1
        } else if (changes < 0) {
          reasonMap(1) = reasonMap(1) + changes
          subUidSet += uid
          reasonMap(3) = reasonMap(3) + 1
        }
        resultMap(reason) = reasonMap
      }
      //记录入库
      for ((reason, reasonMap) <- resultMap) {
        //先记录消耗的钻石统计
        if(reasonMap(1) < 0){
          GoldDB.insert(platformID, hostID, today, reason, 1, reasonMap(1), subUidSet, reasonMap(3))
        }
        //再记录产出的钻石统计
        if(reasonMap(2) > 0){
          GoldDB.insert(platformID, hostID, today, reason, 2, reasonMap(2), addUidSet, reasonMap(4))
        }
      }
    }
  }
  /**
   * 从tblGoldLog表中加载数据,返回HostID与goldLog日志的对应关系
   */
  def loadGoldInfoFromDB(platformID: String, date: String) = {
    val tblName = platformID + "_log.tblGoldLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val goldRes = DBManager.query(tblName, options)
    goldRes.select("HostID", "Uid", "Changes", "Reason").rdd.map(row => {
      (row.getInt(0), (row.getLong(1), row.getInt(2), row.getString(3)))
    }).groupByKey.collectAsMap
  }
}