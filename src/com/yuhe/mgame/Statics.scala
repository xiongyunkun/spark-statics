package com.yuhe.mgame

import org.apache.spark._
import com.yuhe.mgame.statics.StaticsFactory
import com.yuhe.mgame.db.DBManager
import org.apache.log4j.{ Level, Logger }

object Statics {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.kafka").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("spark://localhost:7077").setAppName("statics")
    val sc = new SparkContext(conf)
    DBManager.init(sc)
    StaticsFactory.execute()
    sc.stop()
  }
}