package com.antrl.test

import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import com.spark.sql.engine.SparksqlEngine
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

/**
  * @author ${user.name}
  */
class App extends SparkFunSuite with ParamFunSuite {
  val sparkEngine = SparksqlEngine(spark)

  /**
    * 测试spark的sql功能
    */
  test("spark sql") {
    val schame = new StructType()
      .add("word", "string")
      .add("count", "int")
    val df = spark.createDataset(Seq(Row("word1", 1), Row("word2", 2)))(
      RowEncoder(schame))
    df.createOrReplaceTempView("lefttable")
    sparkEngine.sql(
      "CREATE OR REPLACE TEMPORARY VIEW fence_flow AS" +
        " select * from lefttable")
    sparkEngine.spark.table("fence_flow").show
  }

  /**
    *
    */
  test("ckp test") {
    sparkEngine.sql("checkpoints ab.`table` into 'hdfs:///ssxsxs/ssxxs'")
  }

  /**
    *
    */
  test("select hbase") {
    sparkEngine
      .sql("select info.ac,info.bc FROM hbasetable where key='abc'")
      .show
  }

  /**
    * join 功能
    */
  test("join test") {
    def f(a: Any) = { a + ": UDF" }
    def f2(a: Any, b: Any) = { a + " : " + b }
    sparkEngine.register("testudf", f _)
    sparkEngine.register("testudf2", f2 _)

    val schame = new StructType()
      .add("word", "string")
      .add("count", "int")
    val df = spark.createDataset(Seq(Row("word1", 1), Row("word2", 2)))(
      RowEncoder(schame))
    df.createOrReplaceTempView("lefttable")

    sparkEngine
      .sql(
        s"""CREATE OR REPLACE TEMPORARY VIEW wordcountJoinHbaseTable AS
           |select word,count,testudf(info.ac), testudf2('hello : ', word)
           |FROM lefttable
           |JOIN default:hbasetable
           |ON ROWKEY = word
           |CONF ZK = 'localhost:2181'""".stripMargin
      )
      .show

  }


  /**
   * 基础的查询功能
   */
  test("query test") {
    def f(a: Any) = { a + ": UDF" }
    def f2(a: Any, b: Any) = {a + " : " + b }
    sparkEngine.register("testudf", f _)
    sparkEngine.register("testudf2", f2 _)

    val schame = new StructType()
      .add("word", "string")
      .add("count", "int")
    val df = spark.createDataset(Seq(Row("word1", 1), Row("word2", 2)))(
      RowEncoder(schame))
    df.createOrReplaceTempView("wordcounttable")
    sparkEngine.sql(s"""
         |CREATE OR REPLACE TEMPORARY VIEW tmpTable AS
         |select testudf(testudf(testudf2('word值：', word))) as a,
         |testudf(count) as c,
         |testudf2("word的count数： ", count),
         |'hhh' as cc,
         |''
         |from wordcounttable
         """.stripMargin)
      sparkEngine.spark.table("tmpTable")
      .show
      // .explain()
      // .show()

  }
}


