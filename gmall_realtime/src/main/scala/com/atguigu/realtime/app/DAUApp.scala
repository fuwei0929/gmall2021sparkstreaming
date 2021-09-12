package com.atguigu.realtime.app

import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.constants.MyConstants
import com.atguigu.realtime.beans.{StartLogInfo, StartUpLog}
import com.atguigu.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
// 为了使用 RDD.saveToPhnenix
import  org.apache.phoenix.spark._

/**
 * Created by Smexy on 2021/9/3
 *
 *    精确一次：
 *
 *        如果是聚合类运算：    使用是个事务，将聚合的结果和offset一起保存
 *
 *        如果是非聚合类的运算：   可以使用  at least once +  幂等输出 实现 精确一次
 *
 *                                  at least once：  取消offset的自动提交 +  将offset维护到kafka
 *
 *                                  幂等输出:   使用hbase
 */
object DAUApp extends  BaseApp {

  override var appName: String = "DAUApp"
  override var batchDuration: Int = 10

  def parseData(rdd:RDD[ConsumerRecord[String, String]]) : RDD[StartUpLog] = {

    rdd.map(record => {

      val jsonobject: JSONObject = JSON.parseObject(record.value())

      // 封装common部分
      val log: StartUpLog = JSON.parseObject(jsonobject.getString("common"),classOf[StartUpLog])
      // 将start部分封装为一个独立的bean
      val startlogInfo: StartLogInfo = JSON.parseObject(jsonobject.getString("start"),classOf[StartLogInfo])

      // 合并startlogInfo 到 StartUpLog
      log.mergeStartInfo(startlogInfo)

      // 封装ts
      log.ts = jsonobject.getString("ts").toLong

      val formatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val formatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")


      val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(log.ts),ZoneId.of("Asia/Shanghai"))

      log.logDate=dateTime.format(formatter1)
      log.logHour=dateTime.format(formatter2)

      log
    })


  }

  /*
      同批次去重：
          Mid1_启动日志1_ts1
          Mid1_启动日志2_ts2

          取当日中 ts最小的 StartUpLog

              ①将同一天，同一个设备，在当前批次产生的日志分组
                  ( (mid,logDate) , log)

              ②在一组中，将所有的log，按照 ts升序排序，取第一名


   */
  def removeDuplicateLogInCommonBatch(rdd: RDD[StartUpLog]):RDD[StartUpLog] = {

    val result: RDD[StartUpLog] = rdd.map(log => ((log.mid, log.logDate), log))
      .groupByKey()
      .flatMap {
        case ((mid, logDate), logs) => {
          val logs1: List[StartUpLog] = logs.toList.sortBy(_.ts).take(1)
          logs1
        }
      }
    result
  }

  /*
      读取Redis，判断哪些 mid 在今日已经记录过 log了，将符合条件的过滤掉

          存：   日期，mid

        Redis中记录 今日已经记录过 log的mid
            K:   唯一

            V:  单值： string ,hash
                集合：  set,list,zset

        粒度：  一个信息是一个 K-V
                      某天已经记录log的一个mid 是一个 K-V
                          k: 日期_mid
                          v: string

                          不方便管理！

                一组信息是一个K-V(采取)
                      某天所有记录log的所有mid 是一个 K-V
                          k: 日期
                          v: set<mid>

            注意：以分区为单位创建连接
   */
  def removeDuplicateLogInDiffBatch(rdd: RDD[StartUpLog]):RDD[StartUpLog] = {

    rdd.mapPartitions(partition => {

      //获取连接
      val jedis: Jedis = RedisUtil.getJedisClient()

      // 判断当前rdd中的 每个log的 mid是否已经在redis的集合中存在，如果存在就不要
      // 留下 判断条件为 true
      val filteredLogs: Iterator[StartUpLog] = partition.filter(log => !jedis.sismember(log.logDate , log.mid))

      //关闭连接
      jedis.close()

      filteredLogs

      //返回处理后的分区
    })

  }

  def main(args: Array[String]): Unit = {


    context= new StreamingContext("local[*]",appName,Seconds(batchDuration))

      runApp{

        // 初始ds
        val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(Array(MyConstants.STARTUP_LOG),context,"0422test")

        ds.foreachRDD(rdd => {

          if (!rdd.isEmpty()){
            // 当前批次的获取偏移量
            val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            // 将 ConsumerRecord 转换为 自定义的样例类
            val rdd1: RDD[StartUpLog] = parseData(rdd)

            println("------------------------")

            println("未去重:"+rdd1.count())

            // 进行同批次去重
            val rdd2: RDD[StartUpLog] = removeDuplicateLogInCommonBatch(rdd1)

            println("同批次去重后:"+rdd2.count())

            //跨批次去重：判断当前批次中有哪些mid在今日已经记录过 log，将符合条件的过滤掉
            val rdd3: RDD[StartUpLog] = removeDuplicateLogInDiffBatch(rdd2)

            rdd3.cache()

            println("跨批次去重后:"+rdd3.count())

            // 写入hbase    RDD -------->隐式转换 -----> ProductRDDFunctions.saveToPhoenix
            rdd3.saveToPhoenix("GMALL2021_STARTUPLOG",
              // 将RDD中的 T类型的每一个属性，写入表的哪些列
              Seq("AR", "BA", "CH", "IS_NEW", "MD", "MID", "OS", "UID", "VC", "ENTRY", "LOADING_TIME","OPEN_AD_ID","OPEN_AD_MS","OPEN_AD_SKIP_MS","LOGDATE","LOGHOUR","TS"),

              // new Configuration 只能读取 hadoop的配置文件，无法读取hbase-site.xml和hbase-default.xml
              HBaseConfiguration.create(),
              Some("hadoop102:2181")
            )

            //将已经写入phoneix完成的 mid写入到 redis
            rdd3.foreachPartition(partition => {

              val jedis: Jedis = RedisUtil.getJedisClient()

              partition.foreach(log => {

                jedis.sadd(log.logDate , log.mid)
                // 这个key要保存多久？  1天即可
                jedis.expire(log.logDate, 60 * 60 * 24)

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
