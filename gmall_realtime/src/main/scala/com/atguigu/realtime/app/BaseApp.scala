package com.atguigu.realtime.app

import org.apache.spark.streaming.StreamingContext

import scala.util.control.BreakControl

/**
 * Created by Smexy on 2021/9/3
 *
 *    父类，需要让其他类集成。
 *
 *    编写SparkStreamingAPP的一般套路
 *
 *        ①创建 StreamingContext
 *        ② 从StreamingContext中获取 DS
 *
 *            //每个需求需要根据业务不同进行不同的转换
 *            ③根据业务需要对 DS进行各种转换
 *
 *        ④启动APP
 *        ⑤阻塞当前线程，让APP一直运行
 *
 *     将编写SparkStreamingAPP的一般套路，封装为一个方法，子类只需要调用此方法，省略一些代码！
 *
 *     控制抽象，参考 Breaks.break()
 *
 *
 * def breakable(op: => Unit) {
 *     try {
 *         op
 *     } catch {
 *    case ex: BreakControl =>
 *     if (ex ne breakException) throw ex
 *    }
 * }
 *    op: => Unit  的完整写法  op :() => Unit , 参数是 {}=>Unit,代表一段没有返回值的代码！
 *
 *
 *      集合.foreach()   ===>  集合.foreach{
 *
 *                                case () => {
 *
 *                                 }
 *                              }
 *
 *
 */
abstract class BaseApp {

  //需要子类继承后赋值
  var appName:String

  var batchDuration:Int

  var context:StreamingContext = null

  def runApp(op: => Unit) {
    try {
      // 子类传入的自定义的计算逻辑
      op

      // 启动app
      context.start()

      context.awaitTermination()
    } catch {
      case ex: Exception =>{

        ex.printStackTrace()

      }

    }
  }

}
