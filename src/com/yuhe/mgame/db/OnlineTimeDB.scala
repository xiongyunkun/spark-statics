package com.yuhe.mgame.db

import collection.mutable.ArrayBuffer

object OnlineTimeDB {
  def insert(platformID:String, hostID:Int, date:String, userType:Int, values:collection.mutable.Map[String, Int]) = {
    val cols = Array("Time0", "Time1", "Time5", "Time10", "Time15", "Time30","Time45","Time60", "Time90", "Time120", 
        "Time150", "Time180","Time240", "Time300","Time360", "Time420", "Time600", "Time900", "Time1200",
			  "TotalTimes", "TotalPlayers")
		var sql = "insert into " + platformID + "_statics.tblOnlineTime(PlatformID, HostID, Date, UserType,"
		sql += cols.mkString(",") + ") values('" + platformID + "','"+ hostID + "','" + date + "','" + userType + "','"
		var insertArray = ArrayBuffer[Int]()
		var updateArray = ArrayBuffer[String]()
		for(col <- cols){
		  insertArray += values.getOrElse(col, 0)
		  if(values.contains(col)){
		    updateArray += col + " ='" + values(col) + "'"
		  } 
		}
    sql += insertArray.mkString("','") + "') on duplicate key update " + updateArray.mkString(",")
//    println(sql)
    DBManager.insert(sql)
  }
}