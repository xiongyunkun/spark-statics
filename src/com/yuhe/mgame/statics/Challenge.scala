package com.yuhe.mgame.statics

import org.apache.commons.lang.time.DateFormatUtils
import com.yuhe.mgame.db.DBManager
import com.yuhe.mgame.db.ChallengeDB
import collection.mutable.Map
import collection.mutable.ArrayBuffer
/**
 * 统计极限挑战
 */
object Challenge extends Serializable with StaticsTrait {
  
  def statics(platformID: String) = {
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val challengeMap = loadChallengeFromDB(platformID, today)
    for((hostID, challengeList) <- challengeMap){
      val results = Map[Int, Map[Int, Map[Int, Map[Long, Int]]]]()
      for(info <- challengeList){
        val uid = info._1
        val chapterID = info._2
        val idx = info._3
        val stageID = info._4
        val cResult = results.getOrElse(chapterID, Map[Int, Map[Int, Map[Long, Int]]]())
        val iResult = cResult.getOrElse(idx, Map[Int, Map[Long, Int]]())
        val sResult = iResult.getOrElse(stageID, Map[Long, Int]())
        sResult(uid) = 1
        iResult(stageID) = sResult
        cResult(idx) = iResult
        results(chapterID) = cResult
      }
      //记录数据库
      for((chapterID, cResult) <- results){
        for((idx, iResult) <- cResult){
          for((stageID, sResult) <- iResult){
            val num = sResult.size
            ChallengeDB.insert(platformID, hostID, today, chapterID, idx, stageID, num)
          }
        }
      }
    }
  }
  /**
   * 从tblChallengeLog表中获得极限挑战的日志数据
   */
  def loadChallengeFromDB(platformID:String, date:String) = {
    val tblName = platformID + "_log.tblChallengeLog_" + date.replace("-", "")
    val timeOption = "Time >= '" + date + " 00:00:00' and Time <= '" + date + " 23:59:59'"
    val options = Array(timeOption)
    val challengeRes = DBManager.query(tblName, options)
    challengeRes.select("HostID", "Uid", "ChapterId", "Idx", "StageId").rdd.map(row => {
      (row.getInt(0), (row.getLong(1), row.getInt(2), row.getInt(3), row.getInt(4)))
    }).groupByKey.collectAsMap
  }
}