package com.minboyin.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.sql.{SparkSession, types}

/**
 * Created by minbo on 2021/4/13 21:02
 *  定义schema方式，定义DataFrame，并执行 sql ，然后将数据保存到指定位置
 */

object SparkSQLDemo1 {
  def main(args: Array[String]): Unit = {
    // 日志格式设置
    Logger.getLogger("org").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress","false")

    // 创建 SparkSession 对象
    val spark = SparkSession.builder().master("local").appName("SparkSQLDemo1").getOrCreate()

    // 通过读取本地文件，创建 RDD
    val studentRDD = spark.sparkContext.textFile("D:\\data\\SparkDemo\\student.txt.txt").map(_.split(" "))

    // 通过 schema,创建 DF
    val schema = types.StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    // 将 RDD 映射到 RowRDD 行的数据上

    // 创建 DF

    // 注册视图、表

    // 执行 sql

    // 释放资源

  }
}
