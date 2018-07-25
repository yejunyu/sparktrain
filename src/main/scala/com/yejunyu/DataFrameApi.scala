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
    // 默认是20条记录
    empDF.show(100)
    // 查询某一列的数据
    empDF.select("name").show()
    empDF.select(empDF.col("name")).show()
    // 查询某几列的数据并进行并行计算
    empDF.select(empDF.col("name"), empDF.col("age") + 10).show()
    empDF.select(empDF.col("name"),
      empDF.col("age") + 10).as("age2").show()
    // 条件查询
    empDF.filter(empDF.col("age") > 19).show()
    empDF.where(empDF.col("age") > 19).show()
    // 按年龄分组显示
    empDF.groupBy("age").count().show()
    spark.stop()
  }
}
