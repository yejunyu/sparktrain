package com.yejunyu.etl

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * @author yejunyu
  * @date 18-7-30. 
  */
object AcessConvertUtil {
  // 定义输出的字段
  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", IntegerType),
      StructField("flow", IntegerType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )

  def parseLog(log: String): Row = {
    try {
      val splits = log.split(",")
      val url = splits(1)
      val ip = splits(3)
      val flow = splits(2).toInt
      val domain = "http://localhost:8888/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")
      var cmsType = ""
      var cmsId = 0
      if (cmsTypeId.length == 2) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).replace(".html", "").toInt
      }
      val city = IpUtil.getIpRegion(ip)
      val time = splits(0).split(" ")(0)
      val day = time.replaceAll("/", "")
      Row(url, cmsType, cmsId, flow, ip, city, time, day)
    } catch {
      case e: Exception => Row(0)
    }
  }
}
