package com.spark.learn

import org.apache.spark.sql.SparkSession

object TestHiveSparkSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()


    spark.sql("select * from test123").show
  }
}
