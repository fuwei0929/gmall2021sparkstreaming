package com.atguigu.realtime.testcases

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Date

/**
 * Created by Smexy on 2021/9/3
 *
 *    新的时间日期API：  java.time.xxx
 *
 *      Date :     LocalDate (日期)  | LocalDateTime(日期时间)
 *
 *      SimpleDateFormat ： DateTimeFormatter
 */
object TimeParse {

  def main(args: Array[String]): Unit = {

    val ts:Long = 1630485344000L

    //获取日期和小时

    val date = new Date(ts)

    //将date 格式化为想要的格式
    val format1 = new SimpleDateFormat("yyyy-MM-dd")
    val format2 = new SimpleDateFormat("HH")

    println(format1.format(date))
    println(format2.format(date))

    println("-----------------是线程不安全的！ java8之后提供了新的api------------")


    val formatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val formatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")


    val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts),ZoneId.of("Asia/Shanghai"))

    println(dateTime.format(formatter1))
    println(dateTime.format(formatter2))

  }

}
