package com.atguigu.realtime.testcases

import java.util

import com.atguigu.realtime.utils.RedisUtil
import redis.clients.jedis.Jedis

/**
 * Created by Smexy on 2021/9/8
 */
object JedisTest {

  def main(args: Array[String]): Unit = {

    val jedis: Jedis = RedisUtil.getJedisClient()

    val set: util.Set[String] = jedis.smembers("aaaaa")

    println(set)

    println(jedis.get("bbbbb"))

    jedis.close()


  }

}
