package com.yejunyu

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @author yejunyu
  * @date 18-7-25. 
  */
object DataFrameRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDD")
      .master("local[2]")
      .getOrCreate()

    //    inferReflect(spark)
    programe(spark)

    spark.stop()
  }

  /**
    * 编程方式把rdd转成dataframe
    *
    * @param spark
    */
  def programe(spark: SparkSession): Unit = {
    val path = "hdfs://yejunyu-pc:8020/example/people.txt"
    val rdd = spark.sparkContext.textFile(path)
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).trim.toInt))

    // 事先不知道结构,需要人为定义struct
    val structType = StructType(Array(StructField("id", IntegerType),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))
    val infoDF = spark.createDataFrame(infoRDD, structType)
    infoDF.printSchema()
    infoDF.show()
  }

  /**
    * 通过反射
    *
    * @param spark
    */
  private def inferReflect(spark: SparkSession) = {
    val path = "hdfs://yejunyu-pc:8020/example/people.txt"
    val rdd = spark.sparkContext.textFile(path)

    // 用反射把RDD ==> DataFrame
    import spark.implicits._
    val infoDF = rdd.map(_.split(","))
      .map(line => Info(line(0).toInt, line(1), line(2).trim.toInt))
      .toDF()
    infoDF.show()

    // df转成表用sql操作
    infoDF.createOrReplaceTempView("infos_table")
    spark.sql("select * from infos_table where age >20").show()
  }

  case class Info(id: Int, name: String, age: Int)

}
