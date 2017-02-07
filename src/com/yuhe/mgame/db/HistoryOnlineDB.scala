package com.yuhe.mgame.db

object HistoryOnlineDB {
  /**
   * 插入tblHistoryOnline表数据
   */
  def insert(platformID: String, hostID: Int, date: String, maxNum: Int, minNum: Int, aveNum: Long) = {
    var sql = "insert into " + platformID + "_statics.tblHistoryOnline(PlatformID, HostID, Date, MaxOnline, AveOnline, MinOnline) values('"
    sql += platformID + "','" + hostID + "','" + date + "','" + maxNum + "','" + aveNum + "','" + minNum + "') on duplicate key update MaxOnline = '"
    sql += maxNum + "', AveOnline = '" + aveNum + "', MinOnline = '" + minNum + "'"
//    println(sql)
    DBManager.insert(sql)
  }
}