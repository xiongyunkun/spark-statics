package com.yuhe.mgame.db

import org.apache.spark._
import org.apache.spark.sql.{Row, SQLContext}
import java.util.Properties
import java.io.{ InputStream, FileInputStream, File }
import org.apache.spark.sql.DataFrame
import java.sql.{ DriverManager, PreparedStatement, Connection }
import com.mchange.v2.c3p0.ComboPooledDataSource

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
  private var sqlContext:SQLContext = null
  private val url = prop.getProperty("url").toString()
  private val sparkProp = new Properties()
  sparkProp.setProperty("user", prop.getProperty("username").toString())
  sparkProp.setProperty("password", prop.getProperty("password").toString())
    
  def init(sc:SparkContext) = {
    sqlContext = new SQLContext(sc)
  }
  /**
   * 查询数据库，返回DataSet结构
   */
  def query(tblName:String, options:Array[String]):DataFrame = {
    var newOptions:Array[String] = null
    if(options.length == 0)
      newOptions = Array[String]("1=1") //如果条件判断为0则用1=1判断    
    else
      newOptions = options
    sqlContext.read.jdbc(url, tblName, newOptions, sparkProp)
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
  def insert(sql:String) = {
    val conn = getConnection
    conn.setAutoCommit(false)
    val preparedStatement = conn.prepareStatement(sql)
    val flag = preparedStatement.execute()
    conn.commit()
    conn.close()
    flag
  }
  
}