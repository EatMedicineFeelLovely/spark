package com.spark.learn

import org.apache.spark.sql.SparkSession

import com.spark.learn.cassclass.kv
import com.spark.learn.udf.SparkSqlMaxUDFA
object TestSparkSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext
      .parallelize(1 to 100)
      .map(x => kv(x/5, x))
      .toDF()
      .cache()
      .createOrReplaceTempView("testTable")
    spark.udf.register("test_udf", testUdf)
    spark.udf.register("self_max", new SparkSqlMaxUDFA)
//    spark.sql(s"""
//                 |SELECT k,v,test_udf(k,v) as k_v_sum
//                 |FROM testTable
//                 |WHERE k > ${10}
//      """.stripMargin).show

    spark.sql(s"""
                 |SELECT k,self_max(v) as max_v
                 |FROM testTable
                 |WHERE k > ${10}
                 |group by k
      """.stripMargin).show
  }
  /**
   * 自定义uid
   */
  val testUdf = (x: Int, y: Int) => x + y


}
