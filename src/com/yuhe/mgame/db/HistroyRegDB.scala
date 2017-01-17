package com.yuhe.mgame.db

object HistroyRegDB {
  def insert(platformID:String, hostID:Int, date:String, regNum:Int, totalRegNum:Int) = {
    var sql = "insert into " + platformID + "_statics.tblHistoryReg(PlatformID, HostID, Date, RegNum, TotalRegNum) values('" 
		sql += platformID + "','" + hostID + "','" + date + "','"  + regNum + "','"  + totalRegNum 
		sql +=  "') on duplicate key update RegNum = '" + regNum + "', TotalRegNum = '" + totalRegNum + "'"
    DBManager.insert(sql)
  }
}