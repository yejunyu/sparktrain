package com.yejunyu.sparkSQL

import org.apache.spark.sql.SparkSession

/**
  * @author yejunyu
  * @date 18-7-24. 
  */
object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSessionApp")
      .master("local[2]")
      .getOrCreate()
    val path = "src/main/scala/com/yejunyu/sparkSQL/example/employees.json"
    //    spark.read.format("json").load(path)
    val emp = spark.read.json(path)
    emp.show()
    spark.stop()
  }

}
