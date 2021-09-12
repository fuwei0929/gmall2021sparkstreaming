package com.atguigu.realtime.utils

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

// 读取kafka中的数据，封装为DS
object MyKafkaUtil {

  // 读取配置文件中的信息
  private val properties: Properties = PropertiesUtil.load("config.properties")

  // kafka集群地址
  val broker_list: String = properties.getProperty("kafka.broker.list")

  def getKafkaStream(topics: Array[String], ssc: StreamingContext, groupId:String, saveOffsetToMysql:Boolean = false ,
                     offsetsMap: Map[TopicPartition, Long] =null , ifAutoCommit:String ="false" ): InputDStream[ConsumerRecord[String, String]] = {

    //kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> broker_list,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> ifAutoCommit
    )

    var ds: InputDStream[ConsumerRecord[String, String]] =null;

    if (saveOffsetToMysql){

      ds = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParam,offsetsMap)
      )
    }else{

      ds = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParam))
    }

    ds
  }
}
