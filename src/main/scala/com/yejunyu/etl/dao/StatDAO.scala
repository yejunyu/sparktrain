package com.yejunyu.etl.dao

import java.sql.{Connection, PreparedStatement}

import com.yejunyu.etl.model.DayVideoAccessStat
import com.yejunyu.etl.utils.MySQLUtil

import scala.collection.mutable.ListBuffer

/**
  * @author yejunyu
  * @date 18-7-31. 
  */
object StatDAO {

  /**
    * 批量保存到数据库
    */
  def insertDayVideoAcessTopN(list: ListBuffer[DayVideoAccessStat]): Unit = {
    var con: Connection = null
    var pstmt: PreparedStatement = null
    try {
      con = MySQLUtil.getConnection()
      // 批处理,关闭自动提交
      con.setAutoCommit(false)
      val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?)"
      pstmt = con.prepareStatement(sql)
      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      con.commit() // 手工提交
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(con, pstmt)
    }
  }
}
