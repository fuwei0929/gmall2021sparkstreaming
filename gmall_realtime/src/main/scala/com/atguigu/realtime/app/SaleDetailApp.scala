package com.atguigu.realtime.app

import java.time.LocalDate
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.constants.MyConstants
import com.atguigu.realtime.app.AlertApp.{appName, batchDuration, context}
import com.atguigu.realtime.beans.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.realtime.utils.{MyKafkaUtil, RedisUtil}
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import org.elasticsearch.spark._

/**
 * Created by Smexy on 2021/9/8
 *
 *    只有 DS[(K,V)] 才能Join
 *      order_info.id  join   order_detail.order_id
 *
 *
 *      双流Join 能Join上的前提是 关联的数据必须在两个流的同一个批次！
 *
 *      如果要关联的数据，在两个流的不同批次，此时是Join不上的！
 */
object SaleDetailApp extends  BaseApp {
  override var appName: String = "SaleDetailApp"
  override var batchDuration: Int = 10

  val groupId:String ="saleDetail0422test"

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(appName).setMaster("local[*]")
    //设置，设置ES的一些参数
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes","hadoop102")
    conf.set("es.port","9200")

    context = new StreamingContext(conf, Seconds(batchDuration))

    runApp{


      // 初始ds
      val ds1: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Array(MyConstants.GMALL_ORDER_INFO),context,groupId)
      val ds2: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Array(MyConstants.GMALL_ORDER_DETAIL),context,groupId)

      var orderInfoOffsetRanges: Array[OffsetRange] = null
      var orderDetailOffsetRanges: Array[OffsetRange] = null

      // 转换为样例类
      val ds3: DStream[(String, OrderInfo)] = ds1.transform(rdd => {

        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd.map(record => {

          val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

          (orderInfo.id, orderInfo)

        })

      })

      val ds4: DStream[(String, OrderDetail)] = ds2.transform(rdd => {

        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd.map(record => {

          val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])

          (orderDetail.order_id, orderDetail)

        })

      })

      // Join + 缓存
      val ds5: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = ds3.fullOuterJoin(ds4)

      val ds6: DStream[SaleDetail] = ds5.mapPartitions(partition => {

        //获取连接
        val jedis: Jedis = RedisUtil.getJedisClient()

        val results: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()

        partition.foreach {
          case (orderId, (orderInfoOption, orderDetailOption)) => {

            val gson = new Gson()

            // 如果orderInfoOption 不为NONE
            if (orderInfoOption != None) {

              val OI: OrderInfo = orderInfoOption.get

              if (orderDetailOption != None) {

                val OD: OrderDetail = orderDetailOption.get

                // 1. 当前批次能Join上的就关联
                results.append(new SaleDetail(OI, OD))

              }

              //2. orderInfo无条件写入redis
              jedis.setex("orderInfo:" + orderId, 5 * 60 * 2, gson.toJson(OI))

              //3. 从redis中找，是否有早到的orderDetail，有就匹配
              val earlyCommingorderDetails: util.Set[String] = jedis.smembers("orderDetail:" + orderId)

              earlyCommingorderDetails.forEach(orderDetailJsonStr => {

                val detail: OrderDetail = JSON.parseObject(orderDetailJsonStr, classOf[OrderDetail])

                results.append(new SaleDetail(OI, detail))


              })


            } else {

              // 右侧一定不为 NONE
              val OD: OrderDetail = orderDetailOption.get

              //从redis查询是否有早到的Order_info

              val orderInfoJsonStr: String = jedis.get("orderInfo:" + OD.order_id)

              // 4. 从redis中找是否有早到的Order_info，有就关联
              if (orderInfoJsonStr != null) {

                val OI: OrderInfo = JSON.parseObject(orderInfoJsonStr, classOf[OrderInfo])

                results.append(new SaleDetail(OI, OD))

              } else {

                //5. 没有早到的order_info，说明当前的Order_detail早到了，写入redis等待后续批次的 数据
                jedis.sadd("orderDetail:" + OD.order_id, gson.toJson(OD))

                jedis.expire("orderDetail:" + OD.order_id, 5 * 60 * 2)


              }

            }

          }
        }


        jedis.close()

        results.toIterator

      })

      // 从reids中获取用户的其他信息
      val ds7: DStream[SaleDetail] = ds6.mapPartitions(partition => {

        val jedis: Jedis = RedisUtil.getJedisClient()

        val result: Iterator[SaleDetail] = partition.map(saleDetail => {

          val userJsonStr: String = jedis.get("user:" + saleDetail.user_id)

          saleDetail.mergeUserInfo(JSON.parseObject(userJsonStr, classOf[UserInfo]))

          saleDetail

        })

        jedis.close()

        result

      })

      // 写入ES
      ds7.foreachRDD(rdd => {

        val indexName="gmall2021_sale_detail" + LocalDate.now()

        println("即将写入:"+rdd.count())

        rdd.saveToEs(indexName + "/_doc", Map("es.mapping.id" -> "order_detail_id"))

        //提交offset
        ds1.asInstanceOf[CanCommitOffsets].commitAsync(orderInfoOffsetRanges)
        ds2.asInstanceOf[CanCommitOffsets].commitAsync(orderDetailOffsetRanges)

      })
    }
  }
}
