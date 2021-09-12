package com.atguigu.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.constants.MyConstants
import com.atguigu.realtime.app.DAUApp.{appName, batchDuration, context}
import com.atguigu.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * Created by Smexy on 2021/9/8
 *
 *    将偏移量手动维护到kafka +  redis端输出 幂等性 = 精确一次
 *
 *    set(k1,v1)
 */
object UserApp extends  BaseApp {
  override var appName: String = "UserApp"
  override var batchDuration: Int = 10

  def main(args: Array[String]): Unit = {

    context= new StreamingContext("local[*]",appName,Seconds(batchDuration))


    runApp{

      val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Array(MyConstants.GMALL_USER_INFO),context,"0422test")

      ds.foreachRDD(rdd => {

        if (!rdd.isEmpty()) {
          // 当前批次的获取偏移量
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          rdd.foreachPartition(partition => {

            val jedis: Jedis = RedisUtil.getJedisClient()

            partition.foreach(record => {

              val jsonStr: String = record.value()

              val jSONObject: JSONObject = JSON.parseObject(jsonStr)

              jedis.set("user:"+ jSONObject.getString("id") , jsonStr)


            })

            jedis.close()

          })

          //提交offsets
          ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)


        }

      })





    }


  }
}
