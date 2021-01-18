package com.qf.bigdata

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 统计各个省市的数据量
 */
object ProCityCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\hadoop-common")
    val spark = SparkSession.builder()
      .appName("test")
      .master("local")
      .getOrCreate()

    // 导入内置函数
    import org.apache.spark.sql.functions._
    // 读取数据
    val df = spark.read.load("output/rs")
    // 按照省市分组求Count
//    df.groupBy("provincename","cityname")
//      .count()
//      .show()
    df.createTempView("log")
    spark.sql("select provincename,cityname,count(1) as counts from log group by provincename,cityname")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("provincename","cityname")
      .json("city/rs")

  }
}
