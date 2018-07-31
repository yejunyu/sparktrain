package com.yejunyu.etl

import com.yejunyu.etl.dao.StatDAO
import com.yejunyu.etl.model.DayVideoAccessStat
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
    //    accessDf.printSchema()
    //    accessDf.show()
    // 20180511那一天最受欢迎视频课程
    videoAcessTopNStat(spark, accessDf)
    // 按照地市统计
    cityAcessTopNStat(spark, accessDf)
    spark.stop()
  }

  def videoAcessTopNStat(spark: SparkSession, accessDF: DataFrame): Unit = {
    // dataframe api方式
    import spark.implicits._
    val videoAcessTopNDF = accessDF.filter($"day" === "20180511" && $"cmsType" === "video")
      .groupBy("day", "cmsId").agg(count("cmsId").as("times"))
      .orderBy($"times".desc)
    // 把dataframe装配成一个list,然后写进mysql
    try {
      videoAcessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })
        StatDAO.insertDayVideoAcessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def cityAcessTopNStat(spark: SparkSession, accessDF: DataFrame): Unit = {
    // sql方式操作
    accessDF.createOrReplaceTempView("access_tmp_table")
    val cityAcessTopNDF = spark.sql("select day,cmsId,city,count(cmsId) times from access_tmp_table where day='20180302' and cmsType='video'" +
      " group by day,cmsId,city" +
      " order by times desc")
    cityAcessTopNDF.show()

    // sql窗口函数
    cityAcessTopNDF.select(
      cityAcessTopNDF("day"),
      cityAcessTopNDF("cmsId"),
      cityAcessTopNDF("city"),
      cityAcessTopNDF("times"),
      row_number().over(Window.partitionBy("city")
        .orderBy(cityAcessTopNDF("times").desc))
        .as("rank")
    ).filter("rank <=3").show()
  }
}
