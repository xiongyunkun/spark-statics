package com.yuhe.mgame.db

object StartNumDB {
  def insert(platformID:String, hostID:String, date:String, index:String, cIndex:String, num:Int) = {
    var sql = "insert into " +  platformID + "_statics.tblStartNum(PlatformID, HostID, Date, `Index`, `CIndex`, Num) values('"
    val values = Array(platformID, hostID, date, index, cIndex, num)
    sql += values.mkString("','") + "') on duplicate key update Num = values(Num)"
    DBManager.insert(sql)
  }
}