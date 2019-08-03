package com.spark.learn

import org.apache.spark.sql.SparkSession

import com.spark.learn.cassclass.kv
import com.spark.learn.udf.SparkSqlMaxUDFA
import org.apache.spark.sql.functions._
import com.spark.learn.udf._

object TestSparkSql {
  def main(args: Array[String]): Unit = {

    val conf = getParamConf(args)
    println(conf)
    val spark = SparkSession
      .builder()
      .appName("test")
      //.master("local")
      .getOrCreate()
    import spark.implicits._
    registerUDF(spark) //注册所需的udf
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.sparkContext
      .textFile(conf.path)
      .map(x => kv(x.toInt / 5, x.toInt))
      .toDF()
    df.cache() //df 的 cache 和 tablecache 是一样的，毕竟testTable是从df来得额
    //spark.table("testTable").cache()

    df.createOrReplaceTempView("testTable") //不会出发jo
//    spark.sql(s"""
//                 |SELECT k,v,test_udf(k,v) as k_v_sum
//                 |FROM testTable
//                 |WHERE k > ${10}
//      """.stripMargin).show

// 以下两种写法效果一样，plan也是一样的
//    df.where($"k" > 10)
//      .groupBy($"k")
//      .agg(expr("self_max(v) as max_v"))
//      .show
    spark
      .sql(s"""
                 |SELECT k,self_max(v) as max_v
                 |FROM testTable
                 |WHERE k > ${10}
                 |group by k
      """.stripMargin)
      .show

  }

  /**
    * 自定义uid
    */
  val testUdf = (x: Int, y: Int) => x + y

}
