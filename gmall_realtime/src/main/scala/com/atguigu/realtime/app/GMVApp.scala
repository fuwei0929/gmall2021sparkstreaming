package com.atguigu.realtime.app

import java.sql.{Connection, PreparedStatement}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.JSON
import com.atguigu.constants.MyConstants
import com.atguigu.realtime.app.DAUApp.{appName, batchDuration, context}
import com.atguigu.realtime.beans.OrderInfo
import com.atguigu.realtime.utils.{JDBCUtil, MyJDBCUtils, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Smexy on 2021/9/4
 *
 *    将每一天各个时段统计的GMV数据，写入Mysql
 *
 *    为了保证数据的精确一次，采取 将 数据 + offsets 在一个事务中写入Mysql
 */
object GMVApp extends BaseApp {
  override var appName: String = "GMVApp"
  override var batchDuration: Int = 10

  val groupId = "0422test";

  def mapRecordToOrderInfo(rdd: RDD[ConsumerRecord[String, String]]):RDD[OrderInfo] = {

    rdd.map(record => {

      val orderInfo: OrderInfo = JSON.parseObject(record.value(),classOf[OrderInfo])

      // "create_time": "2021-09-04 16:37:00",
      val formatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val formatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")
      val formatter3: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

      //将create_time，由自定义的formatter3格式解析为  LocalDateTime
      val localDateTime: LocalDateTime = LocalDateTime.parse(orderInfo.create_time, formatter3)

      //根据自定义的格式，对LocalDateTime进行格式化
      orderInfo.create_date=localDateTime.format(formatter1)
      orderInfo.create_hour=localDateTime.format(formatter2)

      orderInfo

    })

  }

  def main(args: Array[String]): Unit = {

    context= new StreamingContext("local[*]",appName,Seconds(batchDuration))

    //查询当前组消费的offsets信息
    val offsets: Map[TopicPartition, Long] = MyJDBCUtils.readHitoryOffsetsFromMysql(groupId)

    println(offsets)

    runApp{

      val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Array(MyConstants.GMALL_ORDER_INFO),context,groupId,
        true,offsets)

      ds.foreachRDD(rdd => {

        if (!rdd.isEmpty()) {
          // 当前批次的获取偏移量
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          //将 ConsumerRecord 转换为样例类 {"payment_way":"2","delivery_address":"YbmfQPyaFanoFkjoRriP","consignee":"ltlWam","create_time":"2021-09-04 16:37:00","order_comment":"CqYUCcEOgpgMryqlfVjl","expire_time":"","order_status":"1","out_trade_no":"5088714924","tracking_no":"","total_amount":"553.0","user_id":"1","img_url":"","province_id":"7","consignee_tel":"13124729998","trade_body":"","id":"2","parent_order_id":"","operate_time":""}
            val rdd1: RDD[OrderInfo] = mapRecordToOrderInfo(rdd)

          //计算，当前这批数据在每一天的每个时段的GMV
          val rdd2: RDD[((String, String), Double)] = rdd1.map(orderinfo => ((orderinfo.create_date, orderinfo.create_hour), orderinfo.total_amount))
            .groupByKey()
            .mapValues(totalAmounts => totalAmounts.sum)

          //将结果收集到Driver端
          val result: Array[((String, String), Double)] = rdd2.collect()

          //将数组和偏移量在一个事务中写入Mysql
          writeDataAndOffsetsToMysql(result,offsetRanges)

        }

      })

    }

  }


  def writeDataAndOffsetsToMysql(result: Array[((String, String), Double)], ranges: Array[OffsetRange]): Unit = {

    var connection : Connection = null
    var ps1: PreparedStatement=null
    var ps2: PreparedStatement=null

   val sql1="INSERT INTO gmvstats VALUES(?,?,?) ON DUPLICATE KEY UPDATE gmv = gmv + VALUES(gmv)"

  /* val sql1=
      """
        |
        | INSERT INTO gmvstats(create_date,create_hour,gmv) VALUES(?,?,?)
        |    ON DUPLICATE KEY UPDATE
        |    gmv = gmv + VALUES(gmv)
        |
        |""".stripMargin*/


    val sql2="INSERT INTO offset(groupid,topic,`partition`,offset) VALUES(?,?,?,?)  ON DUPLICATE KEY UPDATE  offset = values(offset) "
 /*   val sql2=
      """
        |  INSERT INTO offsetsstore(groupid,topic,`partition`,offsets) VALUES(?,?,?,?)
        |    ON DUPLICATE KEY UPDATE
        |    offsets = values(offsets)
        |
        |
        |""".stripMargin*/

    try {
      connection = JDBCUtil.getConnection()

      //取消事务的自动提交
      connection.setAutoCommit(false)

      ps1 = connection.prepareStatement(sql1)
      ps2 = connection.prepareStatement(sql2)

      for (((create_date,create_hour),gmv) <- result) {

        ps1.setString(1,create_date)
        ps1.setString(2,create_hour)
        ps1.setBigDecimal(3,new java.math.BigDecimal(gmv))

        ps1.addBatch()
      }


      //数据执行写入
      val ints: Array[Int] = ps1.executeBatch()

      println("data:"+ints.size)

      for (offsetRange <- ranges) {

        println(ranges.size)

        println("offset"+offsetRange.toString())

        ps2.setString(1,groupId)
        ps2.setString(2,offsetRange.topic)
        ps2.setInt(3,offsetRange.partition)
        // 放入的是这批数据的 结束位置的offset
        ps2.setLong(4,offsetRange.untilOffset)

        ps2.addBatch()

      }

      // 偏移量执行写入
      val offsetsSize: Array[Int] = ps2.executeBatch()

      println("offsets"+offsetsSize.size)

      println("------------")

      //提交事务
      connection.commit()


    } catch {
      case  e:Exception=> {

        e.printStackTrace()
        println(e.getMessage)

        //回滚事务
        connection.rollback()

        //停止app  发送停止的信号 ， 在HDFS上新建目录  /close+appname
        // context.stop(true)


        throw  new RuntimeException("写入失败!")

      }
    } finally {

      if (ps1 != null){
        ps1.close()
      }

      if (ps2 != null){
        ps1.close()
      }


      if (connection != null){
        connection.close()
      }

    }


  }

}
