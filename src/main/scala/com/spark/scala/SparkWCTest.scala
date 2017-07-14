package com.spark.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkWCTest {
  def main(args: Array[String]): Unit = {
    val sc=new SparkContext(new SparkConf().setAppName("Test"))
    sc.parallelize(1 to 100).collect().foreach(println)
  }
}