package com.yejunyu

import org.apache.spark.sql.SparkSession

/**
  * @author yejunyu
  * @date 18-7-25. 
  */
object DataFrameRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDD")
      .master("local[2]")
      .getOrCreate()
    // 用反射把RDD ==> DataFrame
    val path = "hdfs://yejunyu-pc:8020/example/people.txt"
    val rdd = spark.sparkContext.textFile(path)

    import spark.implicits._
    val infoDF = rdd.map(_.split(","))
      .map(line => Info(line(0).toInt, line(1), line(2).trim.toInt))
      .toDF()
    infoDF.show()

    // df转成表用sql操作
    infoDF.createOrReplaceTempView("infos_table")
    spark.sql("select * from infos_table where age >20").show()
    spark.stop()
  }

  case class Info(id: Int, name: String, age: Int)

}
