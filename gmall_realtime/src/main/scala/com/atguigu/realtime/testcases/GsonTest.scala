package com.atguigu.realtime.testcases

import com.alibaba.fastjson.JSON
import com.google.gson.Gson

/**
 * Created by Smexy on 2021/9/8
 *
 * Error:(16, 18) ambiguous reference to overloaded definition,
 * both method toJSONString in class JSON of type (x$1: Any, x$2: com.alibaba.fastjson.serializer.SerializerFeature*)String
 * and  method toJSONString in class JSON of type (x$1: Any)String
 * match argument types (com.atguigu.realtime.testcases.Cat) and expected result type Any
 * println(JSON.toJSONString(cat))
 */
object GsonTest {

  def main(args: Array[String]): Unit = {

    //将Cat对象，转为 JSONstr

    val cat: Cat = Cat("小花")

   // println(JSON.toJSONString(cat))

    val gson = new Gson()

    println(gson.toJson(cat))


  }

}

case class  Cat(name:String)
