package com.yuhe.mgame.db

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.ArrayBuffer

object LoginStaticsDB {
  
  def insert(platformID:String, hostID:String, date:String, hour:String, numMap:MutableMap[Int, Int]) = {
    var sql = "insert into " + platformID + "_statics.tblLoginStatics(PlatformID, HostID, Date, Hour, Step, Num) values('"
    val values = ArrayBuffer[String]()
    for((step, num) <- numMap){
      val value = Array[String](platformID, hostID, date, hour, step.toString, num.toString)
      values += value.mkString("','")
    }
    sql += values.mkString("')('") + "') on duplicate key update Num = values(Num)"
    DBManager.insert(sql)
  }
}