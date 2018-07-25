package com.yejunyu.sparkSQL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yejunyu
  * @date 18-7-24. 
  */
object SQLContextApp {

  def main(args: Array[String]): Unit = {
    // 1. 创建context
    val sparkConf = new SparkConf()
    // 测试环境
    sparkConf.setAppName("SQLContextAPP").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    var sqlContext = new SQLContext(sc)

    // 2 处理
    val path = "src/main/scala/com/yejunyu/sparkSQL/example/people.json"
//    val path = "hdfs://yejunyu-pc:8020/word"
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()
    // 3 关闭资源
    sc.stop()
  }
}
