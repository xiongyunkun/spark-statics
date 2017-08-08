package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.MoneyDB
import collection.mutable.Map

object Money extends Serializable with StaticsTrait {
  def statics(platformID: String, today: String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val moneyInfos = loadMoneyInfoFromDB(platformID, today)
    for ((hostID, rowList) <- moneyInfos) {
      val totalResults = Map[Int, Map[String, Map[String, Int]]]()
      val uidSet = Map[Int, Map[String, Set[Long]]]()
      for (row <- rowList) {
        val uid = row.getLong(2)
        val changes = row.getInt(4)
        val reason = row.getString(5)
        if (changes >= 0) {
          val addOption = totalResults.get(2)
          var addResults: Map[String, Map[String, Int]] = null
          var addUidSet: Map[String, Set[Long]] = null
          if (addOption.isEmpty) {
            addResults = Map[String, Map[String, Int]]()
            totalResults(2) = addResults
            addUidSet = Map[String, Set[Long]]()
            uidSet(2) = addUidSet
          } else {
            addResults = addOption.get
            addUidSet = uidSet(2)
          }
          val channelOption = addResults.get(reason)
          var channelResult: Map[String, Int] = null
          var channelUidSet: Set[Long] = null
          if (channelOption.isEmpty) {
            channelResult = Map[String, Int]()
            addResults(reason) = channelResult
            channelUidSet = Set[Long]()
            addUidSet(reason) = channelUidSet
          } else {
            channelResult = channelOption.get
            channelUidSet = addUidSet(reason)
          }
          channelResult("Changes") = channelResult.getOrElse("Changes", 0) + changes
          channelResult("ConsumeNum") = channelResult.getOrElse("ConsumeNum", 0) + 1
          channelUidSet += uid
          addUidSet(reason) = channelUidSet //这里channelUidSet被重新定义了，要重新赋值
        } else {
          val subOption = totalResults.get(1)
          var subResults: Map[String, Map[String, Int]] = null
          var subUidSet: Map[String, Set[Long]] = null
          if (subOption.isEmpty) {
            subResults = Map[String, Map[String, Int]]()
            totalResults(1) = subResults
            subUidSet = Map[String, Set[Long]]()
            uidSet(1) = subUidSet
          } else {
            subResults = subOption.get
            subUidSet = uidSet(1)
          }
          val channelOption = subResults.get(reason)
          var channelResult: Map[String, Int] = null
          var channelUidSet: Set[Long] = null
          if (channelOption.isEmpty) {
            channelResult = Map[String, Int]()
            subResults(reason) = channelResult
            channelUidSet = Set[Long]()
            subUidSet(reason) = channelUidSet
          } else {
            channelResult = channelOption.get
            channelUidSet = subUidSet(reason)
          }
          channelResult("Changes") = channelResult.getOrElse("Changes", 0) + changes
          channelResult("ConsumeNum") = channelResult.getOrElse("ConsumeNum", 0) + 1
          channelUidSet += uid
          subUidSet(reason) = channelUidSet //这里channelUidSet被重新定义了，要重新赋值
        }
      }
      //记录数据库
      for ((staticsType, staticsResult) <- totalResults) {
        for ((channel, channelResult) <- staticsResult) {
          val changes = channelResult.get("Changes").getOrElse(0)
          val consumeNum = channelResult.get("ConsumeNum").getOrElse(0)
          val uids = uidSet.get(staticsType).get(channel)
          MoneyDB.insert(platformID, hostID, today, channel, staticsType, changes, consumeNum, uids)
        }
      }
    }
  }
  /**
   * 从tblMoneyLog表中获得今天的金币消费信息
   */
  def loadMoneyInfoFromDB(platformID: String, date: String) = {
    val tblName = platformID + "_log.tblMoneyLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val moneyRes = DBManager.query(tblName, options)
    moneyRes.rdd.map(row => {
      val hostID = row.getInt(1)
      (hostID, row)
    }).groupByKey().collectAsMap
  }
}