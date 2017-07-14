package com.test

import java.io.File
import java.util.Properties
import java.io.FileInputStream
import java.io.InputStreamReader
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkYarnclusterroperTest {
  //在yarn-cluster模式下使用配置文件
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Test"))
    val property = getConfigFromFilePath("./dmpdcreplay.properties")
    val param = property.getProperty("param").toInt
    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 4)
    rdd.foreach { x => println(">>>>>>>>>>>>>>" + x * param) 
    }

    sc.stop()
  }
  def getConfigFromFilePath(filePath: String) = {
    var file = new File(filePath)
    var p = new Properties()
    if (file.exists()) {
      var in = new FileInputStream(file)
      p.load(new InputStreamReader(in))
      in.close()
    } else {
      println(" Configuration Path Is Not Exist : " + filePath)
      System.exit(1)
    }
    p
  }
}