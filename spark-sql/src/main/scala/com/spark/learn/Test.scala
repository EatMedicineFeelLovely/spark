package com.spark.learn

import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.parallelize(
      Array(("t1", Array("1", "2"),1),
        ("t1", Array("1", "2","3"),1))
    ).toDF("table","key","count")
      .groupBy("table","key")
      .sum("count")
      .show

  }
}
