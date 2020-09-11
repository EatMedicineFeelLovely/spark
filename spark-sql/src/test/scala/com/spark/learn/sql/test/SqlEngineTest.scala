package com.spark.learn.sql.test

import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import com.spark.sql.engine.SparksqlEngine
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder

class SqlEngineTest  extends SparkFunSuite with ParamFunSuite {
  val sparkEngine = SparksqlEngine(spark)

  test("sparkEngine"){


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
      .show
    // .explain()
    // .show()

  }
}
