package com.yuhe.mgame.statics

import org.apache.spark._

trait StaticsTrait {
  /**
   * 统计指标
   */
  def statics(sc:SparkContext, platformID:String) 
}