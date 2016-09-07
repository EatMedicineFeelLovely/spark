package com.spark.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.spark.util.CustomFunction._
object TestMain {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
                    .setMaster("local")
                    .setAppName("SparkStreaming Flume")
    System.setProperty("hadoop.home.dir", "E:\\eclipse\\hdplocal2.6.0")
    var sc = new SparkContext(conf)
    testMySelfRDD(sc)
    
  }
  /**
   * 自定义一个RDD，将一个RDD转换成自定义的RDD。使用隐式函数
   */
  def testMySelfRDD(sc:SparkContext){
    val preData=sc.textFile("E:\\ZhiziYun\\allData")
        var result=preData.mergeString("我是拼接的字符串")
        preData.map { x => ??? }
        result.take(10).foreach { println }
    
    var test=preData.test(preData)
    
  }
}