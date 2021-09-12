package com.atguigu.realtime.beans

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by Smexy on 2021/9/7
 *    代表 actions中的一个 action
 */
case class Action(action_id:String,
                  item:String,
                  item_type:String,
                  ts:Long
                 )

// actions中的common部分
case class CommonInfo(
                       ar:String,
                       ba:String,
                       ch:String,
                       is_new:Int,
                       md:String,
                       mid:String,
                       os:String,
                       uid:Long,
                       vc:String
                     )

// actions_log主题中的一行数据
case class ActionsLog(
                       actions: List[Action],
                       ts:Long,
                       common:CommonInfo
                     )

// 预警日志
case class CouponAlertInfo(
                          // 预警日志的唯一标识
                            id:String,
                          // 要预警的设备5分钟内登录的所有 uid
                           uids:mutable.Set[String],
                          // 要预警的设备，如果用户点击了 收藏商品，记录要收藏的 商品id
                           itemIds:mutable.Set[String],
                          // 这个设备5分钟内，发生的所有 事件
                           events:ListBuffer[String],
                          // 日志产生的时间戳
                           ts:Long)


