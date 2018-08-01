package com.yejunyu.etl.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * @author yejunyu
  * @date 18-7-31. 
  */
object MySQLUtil {
  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

  def getConnection(): Connection = {
    val user = "root"
    val password = "123456"
    DriverManager.getConnection("jdbc:mysql://localhost:3306/sparktest", user, password)
  }

  def release(con: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (con != null) {
        con.close()
      }
    }
  }
}
