package com.yuhe.mgame.db

import scala.collection.mutable.{ Set => MutableSet }
import scala.collection.mutable.{ Map => MutableMap }
import collection.mutable.ArrayBuffer

object ImeiInfoDB {
  def insert(platformID:String, time:String, imeiMap:MutableMap[Int, MutableSet[String]]) = {
    var sql = "insert into " + platformID + "_statics.tblIMEIInfo(IMEI, Step, Time) values('"
    val values = ArrayBuffer[String]()
    for((step, imeiSet) <- imeiMap){
      val value = ArrayBuffer[String]()
      for(imei <- imeiSet){
        val array = Array(imei, step, time)
        value += array.mkString("','")
      }
      values += value.mkString("')('")
    }
    sql += values.mkString("") + "') on duplicate key update Flag = 'true'"
    DBManager.insert(sql)
  }
}