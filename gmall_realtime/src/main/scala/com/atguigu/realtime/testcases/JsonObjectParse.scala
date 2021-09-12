package com.atguigu.realtime.testcases

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.realtime.beans.{StartLogInfo, StartUpLog}

/**
 * Created by Smexy on 2021/9/3
 */
object JsonObjectParse {

  def main(args: Array[String]): Unit = {

    val str ="{\"common\":{\"ar\":\"110000\",\"ba\":\"Xiaomi\",\"ch\":\"web\",\"is_new\":\"0\",\"md\":\"Xiaomi Mix2 \",\"mid\":\"mid_360\",\"os\":\"Android 11.0\",\"uid\":\"444\",\"vc\":\"v2.1.134\"},\"start\":{\"entry\":\"icon\",\"loading_time\":16605,\"open_ad_id\":20,\"open_ad_ms\":6991,\"open_ad_skip_ms\":6571},\"ts\":1630485344000}";

    val jsonobject: JSONObject = JSON.parseObject(str)

    val log: StartUpLog = JSON.parseObject(jsonobject.getString("common"),classOf[StartUpLog])
    val startlogInfo: StartLogInfo = JSON.parseObject(jsonobject.getString("start"),classOf[StartLogInfo])

    log.mergeStartInfo(startlogInfo)

    log.ts = jsonobject.getString("ts").toLong

    println(log)


  }

}
