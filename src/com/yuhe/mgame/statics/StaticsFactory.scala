package com.yuhe.mgame.statics

import org.apache.spark._
import com.yuhe.mgame.db.ServerDB
import org.apache.commons.lang.time.DateFormatUtils

object StaticsFactory {
  def execute() = {
    val platformSet = ServerDB.getStaticsPlatformSet
    val today = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    for (platformID <- platformSet) {
      Retention.statics(platformID, today) //登陆留存，设备留存，付费留存
      HistoryOnline.statics(platformID, today) //历史在线
      LoginStatics.statics(platformID, today) //登陆过程分析
      Gold.statics(platformID, today) //钻石统计
      OnlineTime.statics(platformID, today) //在线时长
      HistoryReg.statics(platformID, today) //历史注册
      Money.statics(platformID, today) //金币统计
      LevelStatics.statics(platformID, today) //等级统计
      Phone.statics(platformID, today) //手机设备统计
      Vip.statics(platformID, today) //VIP统计 
      UserPayDay.statics(platformID, today) //玩家日充值统计
      PayDay.statics(platformID, today) //日充值统计
      PayZone.statics(platformID, today) //充值区间统计
      Challenge.statics(platformID, today) //极限挑战
      Instance.statics(platformID, today) //关卡达成
    }
  }
}