package com.minboyin.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by minbo on 2021/4/12 23:12
 */

object FileTopN {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("work3").setMaster("local")
      val sc = new SparkContext(conf)
      //sc.setLogLevel("ERROR")

      val rdd1 = sc.textFile("D:\\data\\Spark\\*")

      var index = 0

      rdd1.map(x=>x.split(",")(2))
        .map(x=>(x.toInt," "))
        .sortByKey(false)
        .take(5)
        .map(x=>x._1)
        .foreach(x=>{
          index+=1
          println(index + " " + x)
        })

      sc.stop()
  }

}
