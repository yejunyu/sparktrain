package com.yejunyu.etl

import org.apache.spark.sql.SparkSession
import org.eclipse.jetty.util.{MultiMap, UrlEncoded}

/**
  * @author yejunyu
  * @date 18-7-27. 
  */
object etl_e {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("etl_e")
      .master("local[2]").getOrCreate()
    var path = "file:///home/yejunyu/IdeaProjects/gotest/dig.log"
    val access = spark.sparkContext.textFile(path)
    //    access.take(10).foreach(println)

    var path1 = "./aaaa"
    access.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3) + " " + splits(4)
      val flow = splits(9)
      val url = splits(6)
      val datetime = DateUtils.parse(time)
      DateUtils.parse(time) + "," + getUrlStr(url) + "," + flow + "," + ip
      //      (DateUtils.parse(time),getUrlStr(url),flow,ip)
    }).saveAsTextFile(path1)
    spark.stop()
  }

  def getUrlStr(url: String) = {
    var multiMap = new MultiMap
    UrlEncoded.decodeTo(url.substring(url.indexOf("?") + 1), multiMap, "UTF-8")
    multiMap.get("url")
  }
}
