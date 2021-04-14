package com.minboyin.spark

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by minbo on 2021/4/2 20:09
 */

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    //创建一个sparkContext实例
    val sc = new SparkContext(conf)

    //执行WordCount程序的逻辑
    val result = sc.textFile("hdfs://bigdata1:9000/data/data.txt")
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)

    result.foreach(println)

    sc.stop()
}
}
