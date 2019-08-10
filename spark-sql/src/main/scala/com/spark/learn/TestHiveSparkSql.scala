package com.spark.learn

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
object TestHiveSparkSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //动态分区,nonstrict 表示可以支持多个分区字段的动态分区，如果是strict表示必须有一个字段是静态分区
    spark.sql("SET hive.exec.dynamic.partition = true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    writeToHiveTable(spark)
    //val test123_copy_orc2 = spark.sql("select * from test123_copy_orc2")
    //test123_copy_orc2.show
    //println(test123_copy_orc2.count())

    //spark.read.table("test123_copy_orc").show
  }

  /**
    *
    * @param spark
    */
  def writeToHiveTable(spark: SparkSession): Unit ={
    spark
      .sql("""
             select user_key,md5_phone,occupation,age,date
             |from test123""".stripMargin)
      .write
      .insertInto("test123_copy_orc2")


//
//        spark.sql("""
//            |insert into table test123_copy_orc2
//            | partition(date,age)
//            |select user_key,md5_phone,occupation,age,date
//            |from test123
//          """.stripMargin)

  }
}
