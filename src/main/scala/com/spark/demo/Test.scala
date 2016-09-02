package com.spark.demo

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Test {
  var sc: SparkContext = null
  def main(args: Array[String]): Unit = {
    init
    var a = sc.textFile("inputFile/testone.txt")
      .map { line=> var b = line.split(","); ((b(0), b(1)), (b(3).toInt)) }
      .groupByKey
      .map { case(date_user,value) => (date_user, value.toList.sortWith((x, y) => x > y)) }
      .map { case((date,user),value) =>
        val size = value.size
        var max = value(0)
        var sum = value.reduce(_ + _)
        var zhongweishu = if (size % 2 == 0) {
          (value(size / 2 - 1) + value(size / 2)) / 2
        } else value(size / 2)
        (user, ((max - zhongweishu) / (sum + 0.0), 1))
      }
      .reduceByKey {case ((v1,c1), (v2,c2)) => (v1 + v2, c1 + c2) }
      .map { case(user,(value,count)) =>(user, value / count)}

    a.foreach(println)
  }

  def init() {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Test")
    sc = new SparkContext(sparkConf)
  }
}