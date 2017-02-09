package com.yuhe.mgame.db

import collection.mutable.ArrayBuffer

object LevelStaticsDB {
  /**
   * 插入当天的等级统计
   */
  def insert(platformID:String, hostID:Int, date:String, level:Int, levelResult:collection.mutable.Map[String, Int]) = {
    val cols = Array("TotalNum", "TotalLostNum", "LivelNum", "PayNum", "PayLivelNum")
    var sql = "insert into " + platformID + "_statics.tblLevelStatics(PlatformID, HostID, Date, Level, " 
		sql += cols.mkString(",") + ")" + " values('" + platformID + "','" + hostID + "','" 
		sql += date + "','" + level + "','"
		var values = ArrayBuffer[Int]()
		var updateValues = ArrayBuffer[String]()
		for(col <- cols){
		  val value = levelResult.getOrElse(col, 0)
		  values += value
		  if(levelResult.contains(col)){
		    updateValues += col + "= '" + levelResult(col) + "'"
		  }
		}
    sql += values.mkString("','") + "') on duplicate key update " + updateValues.mkString(",")
//    println(sql)
    DBManager.insert(sql)
  }
  /**
   * 更新流失数据字段
   */
  def update(platformID:String, hostID:Int, date:String, level:Int, levelResult:collection.mutable.Map[String, Int]) = {
    val cols = Array("LiveLostNum", "PayLostNum", "PayLiveLostNum")
    var sql = "update " + platformID + "_statics.tblLevelStatics set "
    var updateValues = ArrayBuffer[String]()
    for(col <- cols){
      if(levelResult.contains(col)){
        updateValues += col + "= '" + levelResult(col) + "'"
      }
    }
    sql += updateValues.mkString(",") + " where PlatformID = '" + platformID + "' and HostID = '"
		sql += hostID + "' and Date = '" + date + "' and Level = '" + level + "'"
//		println(sql)
		DBManager.insert(sql)
  }
}