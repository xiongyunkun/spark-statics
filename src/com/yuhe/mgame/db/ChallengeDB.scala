package com.yuhe.mgame.db

object ChallengeDB {
  
  def insert(platformID:String, hostID:Int, date:String, chapterID:Int, idx:Int, stageID:Int, passNum:Int) = {
    val values = Array(platformID, hostID, date, chapterID, idx, stageID, passNum)
    var sql = "insert into " + platformID + "_statics.tblChallenge(PlatformID, HostID, Date, ChapterId, Idx, StageId, PassNum) values('" 
		sql += values.mkString("','") + "') on duplicate key update PassNum=values(PassNum)"
		DBManager.insert(sql)
  }
}