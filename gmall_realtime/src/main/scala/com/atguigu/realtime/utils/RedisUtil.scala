package com.atguigu.realtime.utils

/**
 * Created by Smexy on 2021/9/3
 */
import java.util.Properties
import redis.clients.jedis.{Jedis}

object RedisUtil {

  val config: Properties = PropertiesUtil.load("config.properties")
  val host: String = config.getProperty("redis.host")
  val port: String = config.getProperty("redis.port")

  def getJedisClient():Jedis={

    new Jedis(host,port.toInt)

  }
}
