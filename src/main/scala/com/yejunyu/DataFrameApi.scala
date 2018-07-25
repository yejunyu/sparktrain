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

    /*
    // 默认位true,过长的信息隐藏
    studentDF.show(30, false)
    studentDF.take(10)
    studentDF.first()
    studentDF.head(3)
    studentDF.select("email").show(30,false)
    studentDF.filter("name=''").show
    studentDF.filter("name='' OR name='NULL'").show
    //name以M开头的人
    studentDF.filter("SUBSTR(name,0,1)='M'").show
    studentDF.sort(studentDF("name")).show
    studentDF.sort(studentDF("name").desc).show
    studentDF.sort("name","id").show
    studentDF.sort(studentDF("name").asc, studentDF("id").desc).show

    studentDF.select(studentDF("name").as("student_name")).show
    val studentDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    studentDF.join(studentDF2, studentDF.col("id") === studentDF2.col("id")).show
    */
  }
}
