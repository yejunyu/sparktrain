package com.yejunyu.sparkSQL

import java.sql.DriverManager

/**
  * @author yejunyu
  * @date 18-7-24. 
  */
object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("")
    val conn = DriverManager.getConnection("","","")
    val pstmt = conn.prepareStatement()
  }
}
