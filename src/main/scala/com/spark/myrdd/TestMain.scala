package com.spark.myrdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.CoGroupedRDD
object TestMain {
    System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
                    .setMaster("local")
                    .setAppName("SparkStreaming Flume")
  
    var sc = new SparkContext(conf)
    testMySelfRDD(sc)
    
  }
 implicit class CustomFunctions3(rdd:RDD[String]) {
  //将两个字符串合并
  def  mergeString(data:String) = new MySelfRDD2(rdd,data)
}
  /**
   * 自定义一个RDD，将一个RDD转换成自定义的RDD。使用隐式函数
   */
  def testMySelfRDD(sc:SparkContext){
    val preData=sc.parallelize(Array("a","2"))
        var result=preData.mergeString("@")
        result.take(10).foreach { println }
    
  }
}