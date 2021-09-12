package com.atguigu.realtime.app

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.constants.MyConstants
import com.atguigu.realtime.app.DAUApp.context
import com.atguigu.realtime.beans.{Action, ActionsLog, CommonInfo, CouponAlertInfo}
import com.atguigu.realtime.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

//导入java和scala集合转换的方法
import collection.JavaConverters._

// 提供了 saveToEs的方法  EsSpark.saveToEs(rdd, "spark/docs", Map("es.mapping.id" -> "id"))
import org.elasticsearch.spark._

/**
 * Created by Smexy on 2021/9/7
 *
 *    将offsets维护在kafka端 +  ES输出的幂等性 = 精确一次
 */
object AlertApp extends BaseApp {
  override var appName: String = "AlertApp"
  override var batchDuration: Int = 10

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkConf
    val conf: SparkConf = new SparkConf().setAppName(appName).setMaster("local[*]")
    //设置，设置ES的一些参数
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes","hadoop102")
    conf.set("es.port","9200")

    context = new StreamingContext(conf, Seconds(batchDuration))

    runApp{

      // 初始ds
      val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Array(MyConstants.ACTIONS_LOG),context,"0422test")

      var offsetRanges: Array[OffsetRange] = null
      //只有初始的DS(KafkaDS),才能获取Offset
      val ds1: DStream[ActionsLog] = ds.transform(rdd => {

        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // 封装为样例类
        rdd.map(record => {

          val jSONObject: JSONObject = JSON.parseObject(record.value())

          /*
                解析： actions: List[Action]
                 kafka中的结构： "actions": [ ]
                  util.List[Action] : java的List集合  ，要的是List[Action]，scala的List

                  将java的List转换为scala的List

                  导入工具类： import collection.JavaConverters._

                  java 转 scala ： java集合.asScala

                  scala转java ： scala集合.asJava
             */
          val actions: List[Action] = JSON.parseArray(jSONObject.getString("actions"), classOf[Action]).asScala.toList

          //解析common
          val commonInfo: CommonInfo = JSON.parseObject(jSONObject.getString("common"), classOf[CommonInfo])

          ActionsLog(actions, jSONObject.getString("ts").toLong, commonInfo)

        })

      })

      // 开窗5分钟，以 (设备，用户)为key，将同一个设备，5分钟内，每一个用户的行为分为一组
      val ds2: DStream[((String, Long), Iterable[ActionsLog])] = ds1.window(Minutes(5))
        .map(log => ((log.common.mid, log.common.uid), log))
        .groupByKey()

      //判断这个用户在5分钟内，是否发生了 添加收获地址行为
      val ds3: DStream[(String, Iterable[ActionsLog])] = ds2.map {
        case ((mid, uid), actionsLogs) => {

          //声明一个标记
          var ifNeedAlert: Boolean = false

          // 判断当前用户是否有预警嫌疑
          Breaks.breakable {
            //遍历5分钟所有的行为
            actionsLogs.foreach(actionsLog => {

              actionsLog.actions.foreach(action => {

                if ("trade_add_address".equals(action.action_id)) {
                  //有需要预警的嫌疑
                  ifNeedAlert = true

                  //跳出循环
                  Breaks.break
                }

              })

            })
          }

          //根据是否需要预警，返回结果
          if (ifNeedAlert) {

            (mid, actionsLogs)
          } else {
            (null, null)
          }

        }

      }

      // 过滤掉不需要预警的
      val ds4: DStream[(String, Iterable[ActionsLog])] = ds3.filter(_._1 != null)

      // 进一步判断，每个设备，5分钟登录的用户数是否超过2个
      val ds5: DStream[(String, Iterable[Iterable[ActionsLog]])] = ds4.groupByKey()

      val ds6: DStream[(String, Iterable[Iterable[ActionsLog]])] = ds5.filter(_._2.size >= 2)

      // 真正需要预警的设备
      val ds7: DStream[(String, Iterable[ActionsLog])] = ds6.mapValues(_.flatten)

      // 生成预警日志
      val ds8: DStream[CouponAlertInfo] = ds7.map {
        case (mid, actionsLogs) => {

          var uids: mutable.Set[String] = new mutable.HashSet[String]

          var itemIds: mutable.Set[String] = new mutable.HashSet[String]

          var events: ListBuffer[String] = new mutable.ListBuffer[String]

          actionsLogs.foreach(actionsLog => {

            uids.add(actionsLog.common.uid.toString)

            actionsLog.actions.foreach(action => {

              events.append(action.action_id)

              // 要预警的设备，如果用户点击了 收藏商品，记录要收藏的 商品id
              if ("favor_add".equals(action.action_id)) {
                itemIds.add(action.item)
              }

            })

          })


          val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

          val ts: Long = System.currentTimeMillis()

          val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Shanghai"))

          //获取预警日志的分钟
          val minute: String = dateTime.format(formatter)

          CouponAlertInfo(mid + "_" + minute, uids, itemIds, events, ts)

        }
      }

      ds8.cache()

      println("即将打印：")
    ds8.count().print()

      //写入ES,提交offset
      ds8.foreachRDD(rdd => {


        val index_name= "gmall_coupon_alert" + LocalDate.now()
        /*
            saveToEs(resource: String, cfg: scala.collection.Map[String, String])

            resource: 要写入的Index名，每天的数据，单独保存在这天的index
            cfg: 写的配置
         */
        rdd.saveToEs(index_name + "/_doc", Map("es.mapping.id" -> "id"))


        //ds,只能是初始的DS，只有初始的DS是kafkaDS,提交offset
        ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })


    }

  }
}
