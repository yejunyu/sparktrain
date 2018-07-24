package com.yejunyu.sparkSQL

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yejunyu
  * @date 18-7-24. 
  */
object HiveContextApp {
  def main(args: Array[String]): Unit = {
    // 1. 创建context
    val sparkConf = new SparkConf()
    // 测试环境
    //    sparkConf.setAppName("HiveContextApp")
    val sc = new SparkContext(sparkConf)
    var hiveContext = new HiveContext(sc)

    // 2 处理
    val path = "src/main/scala/com/yejunyu/sparkSQL/example/people.json"
    hiveContext.table("hive_wordcount").show()

    // 3 关闭资源
    sc.stop()
  }
}
