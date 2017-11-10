package com.yuhe.mgame.db

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set

object ServerDB {
  /**
   * 获得统计服(hostID => platformID)
   */
  def getStaticsServers() = {
    val serverMap = MutableMap[String, ArrayBuffer[String]]()
    val sql = "select a.HostID, a.PlatformID from smcs.tblMixServer a, smcs.tblServer b where a.HostID = b.HostID and b.IsStatics = '1'"
    val conn = DBManager.getConnection
    try{
      val smst = conn.createStatement
      val results = DBManager.query(smst, sql)
      while(results.next){
        val hostID = results.getString("HostID")
        val platformID = results.getString("PlatformID")
        serverMap(hostID) = serverMap.getOrElse(hostID, ArrayBuffer[String]())
        serverMap(hostID) += platformID
      }
      results.close
      smst.close
    }catch{
      case ex: Exception =>
        ex.printStackTrace()
    }finally{
      conn.close
    }
    serverMap
  }
  
  /**
   * 获得统计渠道列表
   */
  def getStaticsPlatformSet() = {
    val platformSet = Set[String]()
    val sql = "select a.HostID, a.PlatformID from smcs.tblMixServer a, smcs.tblServer b where a.HostID = b.HostID and b.IsStatics = '1'"
    val conn = DBManager.getConnection
    try{
      val smst = conn.createStatement
      val results = DBManager.query(smst, sql)
      while(results.next){
        val platformID = results.getString("PlatformID")
        platformSet += platformID
      }
      results.close
      smst.close
    }catch{
      case ex: Exception =>
        ex.printStackTrace()
    }finally{
      conn.close
    }
    platformSet
  }
}