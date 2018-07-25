package com.yejunyu.sparkSQL

import org.apache.spark.sql.SparkSession

/**
  * @author yejunyu
  * @date 18-7-25. 
  */
object DatasetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DatasetApp")
      .master("local[2]")
      .getOrCreate()

    val path = "src/main/scala/com/yejunyu/sparkSQL/example/people.csv"
    // 解析csv,带头
    val peopleDF = spark.read.option("header", "true")
      .option("inferSchema", "true").csv(path)
    peopleDF.show()

    // 需要转换
    import spark.implicits._
    val ds = peopleDF.as[Info]
    ds.map(line => line.name + " : " + line.age.toInt).show()
    spark.stop()
  }

  case class Info(id: Int, name: String, age: Double)

}
