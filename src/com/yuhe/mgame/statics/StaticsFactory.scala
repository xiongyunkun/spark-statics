package com.yuhe.mgame.statics

import org.apache.spark._

object StaticsFactory {
  def execute(sc: SparkContext) = {
    val platformID = "test"
    Retention.statics(sc, platformID)
    HistoryOnline.statics(sc, platformID)
    Gold.statics(sc, platformID)
  }
}