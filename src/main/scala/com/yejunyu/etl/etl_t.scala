package com.yejunyu.etl

import org.apache.spark.sql.SparkSession

/**
  * @author yejunyu
  * @date 18-7-30. 
  */
object etl_t {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("etl_t")
      .master("local[2]").getOrCreate()
    val path = "file:///home/yejunyu/IdeaProjects/sparktrain/diglog/dig.log"
    val accessRDD = spark.sparkContext.textFile(path)
    // rdd=>dataframe
    val accessDF = spark.createDataFrame(accessRDD.map(x => AcessConvertUtil.parseLog(x)), AcessConvertUtil.struct)
    accessDF.printSchema()
    accessDF.show()
    spark.stop()
  }
}
