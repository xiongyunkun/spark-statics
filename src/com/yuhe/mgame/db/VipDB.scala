package com.yuhe.mgame.db

import collection.mutable.Map
import collection.mutable.ArrayBuffer

object VipDB {
  def insert(platformID:String, hostID:Int, date:String, vipLevel:Int, vipResult:Map[String, Int]) = {
    val cols = Array("VipNum","NowVipNum")
    var sql = "insert into " + platformID + "_statics.tblVIP(PlatformID, HostID, Date, VipLevel, " + cols.mkString(",")
    val insertValues = ArrayBuffer[Int]()
    val updateValues = ArrayBuffer[String]()
    for(col <- cols){
      insertValues += vipResult.getOrElse(col, 0)
      if(vipResult.contains(col)){
        updateValues += col + " ='" + vipResult(col) + "'"
      }
    }
    sql += ") values('" + platformID + "','" + hostID + "','" + date + "','" + vipLevel + "','"
    sql += insertValues.mkString("','") + "') on duplicate key update " + updateValues.mkString(",")
//    println(sql)
    DBManager.insert(sql)
  }
}