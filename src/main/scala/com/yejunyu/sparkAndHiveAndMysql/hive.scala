package com.yejunyu.sparkAndHiveAndMysql

import org.apache.spark.sql.SparkSession

/**
  * create table active_user(id bigint,channel string,count int,countType int) partitioned by(dt string);
  *
  * @author yejunyu
  * @date 18-7-26. 
  */
object hive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApi")
      .master("local[2]")
      .getOrCreate()
    val path = "hdfs://yejunyu-pc:8020/example/active_user.csv"
    val userDF = spark.read.csv(path)
    import spark.implicits._
    val ds = userDF.as[activeUser]
    ds.map(line => line.id + " : " + line.channel+" : "+line.count).show()
    userDF.show()

  }

  case class activeUser(id:Int,channel:String,count:Int,countType:Short,dt:Long)
}


