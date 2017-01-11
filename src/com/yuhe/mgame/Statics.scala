package com.yuhe.mgame

import org.apache.spark._
import com.yuhe.mgame.statics.StaticsFactory
import com.yuhe.mgame.db.DBManager

object Statics {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("spark-statics")
    val sc = new SparkContext(conf)
    DBManager.init(sc)
    val sf = StaticsFactory.execute(sc)
    sc.stop()
  }
}