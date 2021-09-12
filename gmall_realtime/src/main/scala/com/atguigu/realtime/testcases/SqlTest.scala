package com.atguigu.realtime.testcases

/**
 * Created by Smexy on 2021/9/4
 */
object SqlTest {

  def main(args: Array[String]): Unit = {
    val sql1=
      """
        |
        |INSERT INTO gmvstats(create_date,create_hour,gmv) VALUES(?,?,?)
        |    ON DUPLICATE KEY UPDATE
        |    gmv = gmv + VALUES(gmv)
        |
        |""".stripMargin

    println(sql1)


  }

}
