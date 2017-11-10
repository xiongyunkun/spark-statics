package com.yuhe.mgame.db

import java.io.InputStream
import java.sql.{Connection, ResultSet, Statement}
import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DBManager {
  //先初始化连接池
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  private val prop = new Properties()
  private var in: InputStream = getClass().getResourceAsStream("dbcp.properties")
  //初始化配置
  try {
    prop.load(in)
    cpds.setJdbcUrl(prop.getProperty("url").toString())
    cpds.setDriverClass(prop.getProperty("driverClassName").toString())
    cpds.setUser(prop.getProperty("username").toString())
    cpds.setPassword(prop.getProperty("password").toString())
    cpds.setMaxPoolSize(Integer.valueOf(prop.getProperty("maxPoolSize").toString()))
    cpds.setMinPoolSize(Integer.valueOf(prop.getProperty("minPoolSize").toString()))
    cpds.setAcquireIncrement(Integer.valueOf(prop.getProperty("acquireIncrement").toString()))
    cpds.setInitialPoolSize(Integer.valueOf(prop.getProperty("initialPoolSize").toString()))
    cpds.setMaxIdleTime(Integer.valueOf(prop.getProperty("maxIdleTime").toString()))
  } catch {
    case ex: Exception => ex.printStackTrace()
  }
  //再初始化spark sql的配置
  private var sparkSession:SparkSession = null
  private val url = prop.getProperty("url").toString()
  private val sparkProp = new Properties()
  sparkProp.setProperty("user", prop.getProperty("username").toString())
  sparkProp.setProperty("password", prop.getProperty("password").toString())

  def init(sc: SparkContext) = {
    sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
  }
  /**
   * 查询数据库，返回DataSet结构
   */
  def query(tblName: String, options: Array[String]): DataFrame = {
    var newOptions: Array[String] = null
    if (options.length == 0)
      newOptions = Array[String]("1=1") //如果条件判断为0则用1=1判断    
    else
      newOptions = options
    sparkSession.read.jdbc(url, tblName, newOptions, sparkProp)
  }
  /**
   * 通过sql语句查询数据
   */
  def query(smst: Statement, sql: String) = {
    var rs: ResultSet = null
    try {
      rs = smst.executeQuery(sql)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        null
    }
    rs
  }

  /**
   * 获得连接
   */
  def getConnection: Connection = {
    try {
      return cpds.getConnection();
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        null
    }
  }
  /**
   * 插入数据库
   */
  def insert(sql: String) = {
    val conn = getConnection
    conn.setAutoCommit(false)
    val preparedStatement = conn.prepareStatement(sql)
    val flag = preparedStatement.execute()
    conn.commit()
    conn.close()
    flag
  }

}