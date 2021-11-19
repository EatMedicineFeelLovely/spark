package com.spark.learn.sql.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q5 {
  def main(args: Array[String]): Unit = {
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("Q5")
      //      .config("hive.exec.dynamici.partition", true)
      //      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      // .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    // 本地测试
    spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("/Users/eminem/Downloads/Quiz/ËØ²Ä/login_data_country_province.csv")
      .createOrReplaceTempView("login_data_country_province")

    // 去重，防止某个id重复登录
    spark.sql(
      s"""SELECT province,account_id, min(logtime) as logtime
         |    FROM login_data_country_province GROUP BY province,account_id """.stripMargin)
      .createOrReplaceTempView("login_data_distinct")
        spark.sql(
          s"""select
             | province,
             | 1_account_id as account_id_1,
             |  1_login_time as login_time_1,
             | 2_account_id as account_id_2,
             |  2_login_time as login_time_2,
             | 3_account_id as account_id_3,
             |  3_login_time as login_time_3
             | FROM (
             | SELECT * FROM
             | (
             |  SELECT province,account_id,logtime, ROW_NUMBER() OVER (PARTITION BY province ORDER BY logtime) rank
             |  FROM login_data_distinct
             |  ) where rank <= 3)
             |                 pivot
             |                 (
             |                     min(`logtime`) as login_time,min(account_id) as account_id for rank in('1','2','3')
             |                 )""".stripMargin)
          .show()
  }
}
