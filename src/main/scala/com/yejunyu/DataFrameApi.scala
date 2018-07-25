package com.yejunyu

import org.apache.spark.sql.SparkSession

/**
  * @author yejunyu
  * @date 18-7-25. 
  */
object DataFrameApi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApi")
      .master("local[2]")
      .getOrCreate()
    val path = "hdfs://yejunyu-pc:8020/example/people.json"
    val empDF = spark.read.format("json").load(path)
    empDF.printSchema()
    spark.stop()
  }
}
