package com.yuhe.mgame.db

object MoneyDB {
  def insert(platformID:String, hostID:Int, date:String, channel:String, staticsType:Int,
      value:Int, consumeNum:Int, uidSet:Set[Long]) = {
    val uidStr = uidSet.mkString(",")
    var sql = "insert into " + platformID + "_statics.tblMoney(PlatformID, HostID, Date, Channel, StaticsType, Value,"
		sql += "Uids, ConsumeNum) values('" + platformID + "','" + hostID + "','" + date + "','" 
		sql += channel + "','" + staticsType + "','" + value 
		sql += "','" + uidStr + "','" + consumeNum + "') on duplicate key update Value = '" + value 
		sql += "', Uids = '" + uidStr + "', ConsumeNum = '" + consumeNum + "'"
		println(sql)
		DBManager.insert(sql)
  }
}