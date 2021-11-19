package com.spark.learn.sql.test
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr
import scala.util.control.Breaks
object Q3 {
  def main(args: Array[String]): Unit = {
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("Q3")
//      .config("hive.exec.dynamici.partition", true)
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      // .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()
    // 本地测试
    val ip_china: DataFrame = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("/Users/eminem/Downloads/Quiz/ËØ²Ä/ip_china.csv")

    val ip_chinaMap = ip_china.collect().map(row => {
      val long_ip_start = row.getLong(2);
      val long_ip_end = row.getLong(3);
      val country = row.getString(4)
      val province = row.getString(5)
      (long_ip_start, long_ip_end, country, province)
    })

    spark.udf.register("date_format", (logtime: String) => {logtime.substring(0,10)})

    val login_data: DataFrame = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("/Users/eminem/Downloads/Quiz/ËØ²Ä/login_data.csv")
      .withColumn("dt", expr("date_format(logtime)"))

    spark.udf.register("ipMapping", (ip: String) => {
      val ipLong = ipToNumber(ip)
      var country = "null"
      var province = "null"
      val loop = new Breaks;
      loop.breakable {
        for (ipStartEnd <- ip_chinaMap) {
          if (ipStartEnd._1 <= ipLong && ipLong <= ipStartEnd._2) {
            country = ipStartEnd._3
            province = ipStartEnd._4
            loop.break;
          }
        }
      }
      (country, province)
    })

    // 使用广播的方式计算
    login_data.withColumn("country_province", expr("ipMapping(ip)"))
      .withColumn("country", expr("country_province._1"))
      .withColumn("province", expr("country_province._2"))
      .drop("country_province")
      .write
      .option("header", true)
      .csv("/Users/eminem/Downloads/Quiz/ËØ²Ä/login_data_country_province")
//      .createOrReplaceTempView("tmp_table")

    // 写入hive
//    spark.sql(
//      s"""insert into table login_data_country_province select * from tmp_table""".stripMargin)

  }

  def ipToNumber(ip: String): Long = {
    var ipLong = 0L
    val ipNumbers = ip.split("\\.")
    for (ipNumber <- ipNumbers) {
      ipLong = ipLong << 8 | ipNumber.toInt
    }
    ipLong
  }
}
