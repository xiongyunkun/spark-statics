package com.yuhe.mgame.db

import scala.collection.mutable.ArrayBuffer
import collection.mutable.Map

object PayZoneDB {
  def insert(platformID:String, hostID:Int, date:String, zoneResults:Map[Int, Int]) = {
    val values = ArrayBuffer[String]()
    for((zoneID, num) <- zoneResults){
      val tuple = Array(platformID, hostID, date, zoneID, num)
      values += tuple.mkString("','")
    }
    //更新之前要把这个服这天的数据都清空
    var delSql = "delete from " + platformID + "_statics.tblPayZone where PlatformID = '" + platformID
		delSql += "' and HostID = '" + hostID + "' and Date = '" + date + "'"
//		println(delSql)
		DBManager.insert(delSql)
		var sql = "insert into " + platformID + "_statics.tblPayZone(PlatformID, HostID, Date, ZoneID, PayUserNum"
		sql += ") values('" + values.mkString("'),('") + "') on duplicate key update PayUserNum = values(PayUserNum)"
//		println(sql)
		DBManager.insert(sql)
  }
}