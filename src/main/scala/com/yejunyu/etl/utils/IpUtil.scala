package com.yejunyu.etl.utils

import com.ggstar.util.ip.IpHelper

/**
  * @author yejunyu
  * @date 18-7-30. 
  */
object IpUtil {

  def main(args: Array[String]): Unit = {
    println(getIpRegion("120.92.77.237"))
  }

  def getIpRegion(ip: String) = {
    IpHelper.findRegionByIp(ip)
  }
}
