package com.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
object SparkSqlTest {
  def main(args: Array[String]): Unit = {
  val spark = getSC
  SpaekSqlFunc.selectFunc(spark)
  
  }
  def getSC() = {
    SparkSession
      .builder()
      .master("local[4]")
      //.enableHiveSupport()//是否支持hive
      .appName("Spark SQL basic example")
      .getOrCreate()
  }
  
}