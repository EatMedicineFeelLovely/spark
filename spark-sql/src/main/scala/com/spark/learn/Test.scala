package com.spark.learn

import java.util.Date

import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, min, sum}

object Test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext
      .parallelize(
        Array(("t1", Array("1", "2"), 1), ("t1", Array("1", "2", "3"), 1))
      )
      .toDF("table", "key", "count")
      .groupBy("table", "key")
      .sum("count")
    // .show
    registUDF(spark)
    val df = spark.sparkContext
      .parallelize(
        Array("1576552655873")
      )
      .toDF("time")
    df.withColumn("dayhour", expr("getDayHourFromTimestamp(time)"))
      .withColumn("day", $"dayHour" (0).as("day"))
      .withColumn("hour", $"dayHour" (1).as("hour"))
      .show
      // .printSchema()
  }

  /**
    *
    * @param spark
    */
  def registUDF(spark: SparkSession): Unit = {

    val getDayHourFromTimestamp = (times: String) => {
      val t = DateFormatUtils
        .format(new Date(times.toLong), "yyyyMMdd HH")
      Array(t.substring(0, 8), t.substring(9, 11))
    }

    spark.udf.register("getDayHourFromTimestamp", getDayHourFromTimestamp)

  }
}
