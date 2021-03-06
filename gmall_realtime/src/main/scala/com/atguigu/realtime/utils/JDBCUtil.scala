package com.atguigu.realtime.utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.kafka.common.TopicPartition
import scala.collection.mutable

object JDBCUtil {

  private val properties: Properties = PropertiesUtil.load("db.properties")

  // 创建连接池对象
  var dataSource:DataSource = init()

  // 连接池的初始化
  def init():DataSource = {

    val paramMap = new java.util.HashMap[String, String]()
    paramMap.put("driverClassName", properties.getProperty("jdbc.driver.name"))
    paramMap.put("url", properties.getProperty("jdbc.url"))
    paramMap.put("username", properties.getProperty("jdbc.user"))
    paramMap.put("password", properties.getProperty("jdbc.password"))
    paramMap.put("maxActive", properties.getProperty("jdbc.datasource.size"))

    // 使用Druid连接池对象
    DruidDataSourceFactory.createDataSource(paramMap)
  }

  // 从连接池中获取连接对象
  def getConnection(): Connection = {
    dataSource.getConnection
  }

  def main(args: Array[String]): Unit = {

    println(getConnection())

  }

  def readHitoryOffsetsFromMysql(groupId: String) : Map[TopicPartition, Long] = {

    val offsetsMap: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()

    var conn:Connection=null
    var ps:PreparedStatement=null
    var rs:ResultSet=null

    val sql:String=
      """
        |
        |SELECT
        |  `topic`,`partition`,`offset`
        |FROM `offset`
        |WHERE `groupid`=?
        |
        |
        |""".stripMargin


    try {

      conn=JDBCUtil.getConnection()
      ps=conn.prepareStatement(sql)
      ps.setString(1,groupId)
      rs= ps.executeQuery()

      while(rs.next()){
        val topic: String = rs.getString("topic")
        val partitionid: Int = rs.getInt("partition")
        val offset: Long = rs.getLong("offset")
        val topicPartition = new TopicPartition(topic, partitionid)
        offsetsMap.put(topicPartition,offset)
      }

    }catch {
      case e:Exception =>
        e.printStackTrace()
        throw new RuntimeException("查询偏移量出错！")

    }finally {

      if (rs != null){
        rs.close()
      }

      if (ps != null){
        ps.close()
      }

      if (conn != null){
        conn.close()
      }
    }
    //将可变map转为不可变map
    offsetsMap.toMap
  }
}
