package com.atguigu.realtime.utils

import java.io.InputStreamReader
import java.util.Properties

// 加载类路径的一个properties文件，只需要调用load方法，自动将properties文件读取，封装一个Properties返回
object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

  def main(args: Array[String]): Unit = {

    println(load("config.properties"))

  }

}
