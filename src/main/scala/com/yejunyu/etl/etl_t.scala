package com.yejunyu.etl

import com.yejunyu.etl.utils.AcessConvertUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author yejunyu
  * @date 18-7-30. 
  */
object etl_t {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("etl_t")
      .master("local[2]").getOrCreate()
    val path = "file:///home/yejunyu/IdeaProjects/sparktrain/diglog"
    val accessRDD = spark.sparkContext.textFile(path)
    // rdd=>dataframe
    val accessDF = spark.createDataFrame(accessRDD.map(x => AcessConvertUtil.parseLog(x)), AcessConvertUtil.struct)
    //    accessDF.printSchema()
    //    accessDF.show()
    // 以天分区
    // coalesce设置输出文件个数（*重要）
    accessDF.coalesce(1).write.format("parquet").partitionBy("day")
      .mode(SaveMode.Overwrite)
      .save("/home/yejunyu/hadoopTest/clean")
    spark.stop()
  }
}
