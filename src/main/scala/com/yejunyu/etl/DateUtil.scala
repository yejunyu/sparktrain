package com.yejunyu.etl

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期解析工具类
  *
  * @author yejunyu
  * @date 18-7-27. 
  */
object DateUtil {
  val TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    var strr = "[15/Sep/2018:10:33:04 +0800]"
    println(parse(strr))
  }

  /**
    * 日期转换
    *
    * @param time
    */
  def parse(time: String): String = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 字符串转时间戳
    * [30/Dec/2018:21:48:17 +0800]
    *
    * @param time
    */
  def getTime(time: String): Long = {
    try {
      TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.indexOf("]"))).getTime()
    } catch {
      case e: Exception => {
        0L
      }
    }
  }
}
