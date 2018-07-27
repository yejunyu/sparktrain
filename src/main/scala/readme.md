## 读
```scala
  df = spark.read
        .format("json")
        // 按10%抽样
        .option("samplingRatio","0.1")
        .load(path) 
```

## 写
```scala
   DF.write
     .format("csv")
     // append追加,overwrite覆写,ignore不存在写,存在忽略
     .mode("append")
     .partitionBy("year")
     // 数据存到hive表
     .saveAsTable("hive_table_name")
```