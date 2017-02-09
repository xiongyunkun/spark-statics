package com.yuhe.mgame.db

object InstanceDB {
  /**
   * 插入tblInstance表
   */
  def insertInstance(platformID:String, hostID:Int, date:String, stageID:Int, totalNum:Int, 
      passNum:Int, regNum:Int) = {
    val values = Array(platformID, hostID, date, stageID, totalNum, passNum, regNum)
    var sql = "insert into "+platformID+"_statics.tblInstance(PlatformID, HostID, Date, StageId, TotalNum, PassNum, RegNum) values('" 
		sql += values.mkString("','")
		sql += "') on duplicate key update TotalNum = values(TotalNum),PassNum=values(PassNum),RegNum=values(RegNum)"
//		println(sql)
		DBManager.insert(sql)
  }
  
  /**
   * 插入tblInstancePlayStatics表
   */
  def insertInstancePlay(platformID:String, hostID:Int, date:String, instType:Int, playNum:Int, uidNum:Int) = {
    val cols = Array("PlatformID", "HostID", "Date", "InstanceType", "PlayNum", "UidNum")
    val values = Array(platformID, hostID, date, instType, playNum, uidNum)
    var sql = "replace into " + platformID + "_statics.tblInstancePlayStatics("
		sql += cols.mkString(",") + ") values('" + values.mkString("', '") + "')"
//		println(sql)
		DBManager.insert(sql)
  }
}