package com.yejunyu.etl

import com.ggstar.util.ip.IpHelper

/**
  * @author yejunyu
  * @date 18-7-30. 
  */
object IpUtil {

  def main(args: Array[String]): Unit = {
    println(getIpRegion("121.40.225.209"))
  }

  def getIpRegion(ip: String) = {
    IpHelper.findRegionByIp(ip)
  }
}
