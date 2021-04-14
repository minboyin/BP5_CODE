package com.minboyin.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by minbo on 2021/4/8 13:17
 *  客户端IP          访问时间                       访问的资源                         状态码   流量
 *  192.168.88.1 - - [30/Jul/2017:12:53:43 +0800] "GET /MyDemoWeb/java.jsp HTTP/1.1" 200      259
 */

object WebPV {
  def main(args: Array[String]): Unit = {
    // 创建sc实例
    val conf = new SparkConf().setAppName("WebPV").setMaster("local")
    val sc = new SparkContext(conf)

    // 读取数据
    val rdd1 = sc.textFile("D:\\data\\input\\localhost_access_log.2020-11-21.txt")
      .map(
        line => {
          // 解析每一行数据：获取每一行数据中的 “...” 部分数据
          // 寻找第一个 “ 的 index
          val index1 = line.indexOf("\"")
          // 寻找第二个 ” 的 index
          val index2 = line.lastIndexOf("\"")
          //获取双引号中间的数据,由于左闭右开，所以使用 index+1
          val line1 = line.substring(index1 + 1, index2)

          val index3 = line1.indexOf(" ")
          val index4 = line1.lastIndexOf(" ")
          val line2 = line1.substring(index3 + 1, index4)

          val jspName = line2.substring(line2.lastIndexOf("/") + 1)

          (jspName, 1)

        }
      )
    // 按照jsp的名字进行聚合操作 ((java.jsp,10)(hadoop.jsp,5)..)
    val rdd2 = rdd1.reduceByKey(_ + _)

    // 排序,根据 value 排序，所以 _._2
    val rdd3 = rdd2.sortBy(_._2, false)

    // 取出访问量最高的两个网页
    rdd3.take(2).foreach(println)

    //释放资源
    sc.stop()
  }
}
