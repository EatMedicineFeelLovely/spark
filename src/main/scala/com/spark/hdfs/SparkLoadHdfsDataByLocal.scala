package com.spark.hdfs

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkLoadHdfsDataByLocal {
  def main(args: Array[String]): Unit = {
    val sc=new SparkContext(new SparkConf().setMaster("local").setAppName("w"))
    sc.textFile("hdfs://mongodb3:8020/user/linmingqiang/input/suningpclog")
    .foreach { println }
    
  }
}