package com.yuhe.mgame.statics

import org.apache.spark._

object StaticsFactory {
  def execute(sc: SparkContext) = {
    val platformID = "test"
    Retention.statics(platformID) //登陆留存，设备留存，付费留存
    HistoryOnline.statics(platformID) //历史在线
    Gold.statics(platformID) //钻石统计
    OnlineTime.statics(platformID) //在线时长
    HistoryReg.statics(platformID) //历史注册
    Money.statics(platformID) //金币统计
    LevelStatics.statics(platformID) //等级统计
    Phone.statics(platformID) //手机设备统计
    Vip.statics(platformID) //VIP统计 
    UserPayDay.statics(platformID) //玩家日充值统计
    PayDay.statics(platformID) //日充值统计
    PayZone.statics(platformID) //充值区间统计
    Challenge.statics(platformID) //极限挑战
    Instance.statics(platformID) //关卡达成
  }
}