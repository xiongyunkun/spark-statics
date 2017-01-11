package com.yuhe.mgame.db

import collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

object RetentionDB {
  /**
   * 插入登陆留存率表
   */
  def insertLoginRetention(platformID: String, hostID: String, date: String, 
      results: Map[String, String]) = {
    val colNames = Array("LoginNum", "NewNum", "1Days", "2Days", "3Days", "4Days",
			"5Days", "6Days", "7Days", "10Days", "13Days", "15Days", "29Days", "30Days")
		val cols = ArrayBuffer[String]()
		val values = ArrayBuffer[String]()
		val updateValues = ArrayBuffer[String]()
		values += platformID
		values += hostID
		values += date
		for(i <- 0 until colNames.length){
		  val col = colNames(i)
		  if(results.contains(col)){
		    cols += col
		    val value = results(col)
		    values += value
		    updateValues += col + "= '" + value + "'"
		  }
		}
    var sql = "insert into " + platformID + "_statics.tblRetention(PlatformID, HostID, Date,"
    sql = sql + cols.mkString(",") + ") values('" + values.mkString("','")
    sql = sql + "') on duplicate key update " + updateValues.mkString(",")
    DBManager.insert(sql)
  }
  /**
   * 插入设备留存表
   */
  def insertIMEIRetention(platformID: String, hostID: String, date: String, 
      results: Map[String, String]) = {
    val colNames = Array("LoginNum", "NewNum", "1Days", "2Days", "3Days", "4Days",
			"5Days", "6Days", "7Days", "10Days", "13Days", "15Days", "29Days", "30Days")
		val cols = ArrayBuffer[String]()
		val values = ArrayBuffer[String]()
		val updateValues = ArrayBuffer[String]()
		values += platformID
		values += hostID
		values += date
		for(i <- 0 until colNames.length){
		  val col = colNames(i)
		  if(results.contains(col)){
		    cols += col
		    val value = results(col)
		    values += value
		    updateValues += col + "= '" + value + "'"
		  }
		}
    var sql = "insert into " + platformID + "_statics.tblPhoneRetention(PlatformID, HostID, Date,"
    sql = sql + cols.mkString(",") + ") values('" + values.mkString("','")
    sql = sql + "') on duplicate key update " + updateValues.mkString(",")
//    println(sql)
    DBManager.insert(sql)
  }
  /**
   * 插入付费留存表
   */
  def insertPayRetention(platformID: String, hostID: String, date: String, 
      results: Map[String, String]) = {
    val colNames = Array("LoginNum", "FirstPayUserNum", "1Days", "2Days", "3Days", "4Days",
			"5Days", "6Days", "7Days", "10Days", "13Days", "15Days", "29Days", "30Days")
		val cols = ArrayBuffer[String]()
		val values = ArrayBuffer[String]()
		val updateValues = ArrayBuffer[String]()
		values += platformID
		values += hostID
		values += date
		for(i <- 0 until colNames.length){
		  val col = colNames(i)
		  if(results.contains(col)){
		    cols += col
		    val value = results(col)
		    values += value
		    updateValues += col + "= '" + value + "'"
		  }
		}
    var sql = "insert into " + platformID + "_statics.tblPayRetention(PlatformID, HostID, Date,"
    sql = sql + cols.mkString(",") + ") values('" + values.mkString("','")
    sql = sql + "') on duplicate key update " + updateValues.mkString(",")
//    println(sql)
    DBManager.insert(sql)
  }
}