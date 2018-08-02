package com.yejunyu.etl

import com.yejunyu.etl.dao.StatDAO
import com.yejunyu.etl.model.{DayVideoAccessStat, DayVideoCityAccessStat, DayVideoFlowAccessStat}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * @author yejunyu
  * @date 18-7-31. 
  */
object etl_l {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("etl_l")
      // 自动推断类型,day被推断成了integer,关闭推断功能
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()

    val accessDf = spark.read.parquet("/home/yejunyu/hadoopTest/clean")
    val day = ""
    // 先清空当天的数据
    StatDAO.deleteTable(day)
    // 20180511那一天最受欢迎视频课程
    videoAccessTopNStat(spark, accessDf, day)
    // 按照地市统计
    cityAccessTopNStat(spark, accessDf, day)
    //     按照流量统计
    flowAccessTopNStat(spark, accessDf, day)
    spark.stop()
  }

  def flowAccessTopNStat(spark: SparkSession, frame: DataFrame, day: String): Unit = {
    // dataframe api方式
    import spark.implicits._
    val flowAccessTopNDF = frame.filter($"cmsType" === "video")
      .groupBy("day", "cmsId").agg(sum("flow").as("flow"))
      .orderBy($"flow".desc)
    // 把dataframe装配成一个list,然后写进mysql
    try {
      flowAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoFlowAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Int]("cmsId")
          val flow = info.getAs[Long]("flow")

          list.append(DayVideoFlowAccessStat(day, cmsId, flow))
        })
        StatDAO.insertDayVideoFlowAcessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    // sql方式操作
    accessDF.createOrReplaceTempView("access_tmp_table")
    val cityAccessTopNDF = spark.sql("select day,cmsId,city,count(cmsId) times from access_tmp_table where cmsType='video'" +
      " group by day,cmsId,city" +
      " order by times desc")
    // sql窗口函数
    val cityVideoTop3Df = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy("city")
        .orderBy(cityAccessTopNDF("times").desc))
        .as("rank")
    ).filter("rank <=3")

    try {
      cityVideoTop3Df.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoCityAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val city = info.getAs[String]("city")
          val cmsId = info.getAs[Int]("cmsId")
          val times = info.getAs[Long]("times")
          val rank = info.getAs[Int]("rank")

          list.append(DayVideoCityAccessStat(day, cmsId, times, city, rank))
        })
        StatDAO.insertDayVideoCityAcessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    // dataframe api方式
    import spark.implicits._
    val videoAccessTopNDF = accessDF.filter($"cmsType" === "video")
      .groupBy("day", "cmsId").agg(count("cmsId").as("times"))
      .orderBy($"times".desc)
    // 把dataframe装配成一个list,然后写进mysql
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Int]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })
        StatDAO.insertDayVideoAcessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
