package com.minboyin.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by minbo on 2021/4/13 9:56
 */

object TwoTimesSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("work2").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd1 = sc.textFile("D:\\data\\data.txt.txt")

    rdd1.map(line=>{
      val arr = line.split(",")
      val key = arr(0)
      val value = arr(1)
      (key,value)
    })
      .groupByKey()
      .map(t => (t._1, t._2.toList.sortWith(_.toInt < _.toInt).mkString(",")))
      .sortByKey(true)
      .foreach(println)

    sc.stop()
  }

}
