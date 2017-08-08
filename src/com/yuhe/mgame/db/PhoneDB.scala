package com.yuhe.mgame.db

import collection.mutable.Map
import collection.mutable.ArrayBuffer

object PhoneDB {

  def insert(platformID: String, hostID: Int, date: String, resultMap: Map[String, Map[String, String]]) = {
    val cols = Array("PlatformID", "HostID", "IMEI", "IMSI", "Model", "Brand", "SysVer", "SdkVer",
      "SimOperation", "DPI", "Uids", "Date")
    val updateCols = Array("IMSI", "Model", "Brand", "SysVer", "SdkVer", "SimOperation", "DPI", "Uids", "Date")
    var sql = "insert into " + platformID + "_statics.tblPhone(" + cols.mkString(",") + ") values("
    val sqlValues = ArrayBuffer[String]()
    val values = resultMap.values
    for (map <- values) {
      val tValues = ArrayBuffer[String]()
      for (col <- cols) {
        if (col == "PlatformID") {
          tValues += "'" + platformID + "'"
        } else if (col == "HostID") {
          tValues += "'" + hostID.toString + "'"
        } else if (col == "Date") {
          tValues += "'" + date + "'"
        } else {
          tValues += "'" + map.getOrElse(col, "") + "'"
        }
      }
      sqlValues += tValues.mkString(",")
    }
    if (sqlValues.length > 0) {
      sql += sqlValues.mkString("),(") + ") on duplicate key update "
      val updateValues = ArrayBuffer[String]()
      for (col <- updateCols) {
        updateValues += col + " = values(" + col + ")"
      }
      sql += updateValues.mkString(",")
      DBManager.insert(sql)
    }

  }
}