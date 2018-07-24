package com.yejunyu.sparkSQL

import java.sql.DriverManager

/**
  * @author yejunyu
  * @date 18-7-24. 
  */
object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://localhost:10000", "yejunyu", "")
    val pstmt = conn.prepareStatement("select word,count(1) from hive_wordcount lateral view explode(split(context,\" \")) wc as word group by word")
    val rs = pstmt.executeQuery()
    while (rs.next()) {
      println("word: " + rs.getString("word")
        + " , count: " + rs.getInt("count(1)")
      )
    }
  }
}