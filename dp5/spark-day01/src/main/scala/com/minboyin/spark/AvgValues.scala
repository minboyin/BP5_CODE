package com.minboyin.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by minbo on 2021/4/12 21:04
 */

object AvgValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("work1").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd1 = sc.parallelize(List(("spark",2),("hadoop",6),("hadoop",4),("spark",6)))

    val rdd2 = rdd1.groupByKey()

    rdd2.map(x=>{
      var num = 0
      var sum = 0
      for(i<-x._2){
        sum = sum +i
        num = num +1
      }
      val avg = sum / num
      (x._1,avg)
    }).foreach(println)

    sc.stop()
  }
}
