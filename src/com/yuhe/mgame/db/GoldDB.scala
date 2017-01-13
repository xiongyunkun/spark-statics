package com.yuhe.mgame.db

object GoldDB {
  def insert(platformID:String, hostID:Int, date:String, channel:String, staticsType:Int, changes:Int, 
      uidSet:collection.mutable.Set[Long], consumeNum:Int){
    val uidStr = uidSet.mkString(",")
    var sql = "insert into " + platformID + "_statics.tblGold(PlatformID, HostID, Date, Channel, StaticsType, Value,"
		sql	+= "Uids, ConsumeNum) values('" + platformID + "','"+ hostID + "','" + date + "','" 
		sql	+= channel + "','"+ staticsType + "','" + changes 
		sql	+= "','" + uidStr + "','" + consumeNum + "') on duplicate key update Value = '" + changes 
		sql	+= "', Uids = '"+ uidStr + "', ConsumeNum = '"+ consumeNum + "'"
		DBManager.insert(sql)
  }
}