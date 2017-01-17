package com.yuhe.mgame.statics

import org.apache.spark._

object StaticsFactory {
  def execute(sc: SparkContext) = {
    val platformID = "test"
    Retention.statics(sc, platformID) //登陆留存，设备留存，付费留存
    HistoryOnline.statics(sc, platformID) //历史在线
    Gold.statics(sc, platformID) //钻石统计
    OnlineTime.statics(sc, platformID) //在线时长
    HistoryReg.statics(sc, platformID) //历史注册
    Money.statics(sc, platformID) //金币统计
  }
}