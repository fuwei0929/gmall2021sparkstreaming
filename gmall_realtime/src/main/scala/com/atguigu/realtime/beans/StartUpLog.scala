package com.atguigu.realtime.beans

/**
 * Created by Smexy on 2021/9/3
 *    phnoeix 辅助建表，指定主键(rowkey)
 *        (logDate,mid)
 *
 */
case class StartUpLog(
                       //common
                       var ar:String,
                       var ba:String,
                       var  ch:String,
                       var is_new:Int,
                       var md:String,
                       var mid:String,
                       var os:String,
                       var  uid:Long,
                       var   vc:String,
                       // start
                       var entry:String,
                       var loading_time:Int,
                       var open_ad_id:Int,
                       var open_ad_ms:Int,
                       var open_ad_skip_ms:Int,
                       //kafka里面没有，为了统计最终结果，额外设置的字段，需要从ts转换得到
                       var logDate:String,
                       var logHour:String,
                     // kafka中有的
                       var ts:Long){


  def mergeStartInfo(startInfo:StartLogInfo):Unit={

    if (startInfo != null){

      this.entry = startInfo.entry
      this.loading_time = startInfo.loading_time
      this.open_ad_id = startInfo.open_ad_id
      this.open_ad_ms = startInfo.open_ad_ms
      this.open_ad_skip_ms = startInfo.open_ad_skip_ms

    }
  }
}
