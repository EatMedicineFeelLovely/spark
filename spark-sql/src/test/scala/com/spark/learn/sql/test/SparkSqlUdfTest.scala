package com.spark.learn.sql.test

import com.spark.learn.test.bean.TestCaseClass.WordCount
import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

class SparkSqlUdfTest extends SparkFunSuite with ParamFunSuite {
  import spark.implicits._

  /**
   *
   */
  test("udf") {
    val ds = spark.read.json(
      "/Users/eminem/workspace/git_pro/spark-learn/resources/datafile/arr_map_struc.json")
    ds.createOrReplaceTempView("test")
// 两种方式注册
    val udf1 = (id: String) => { (id + "_h") }
    val udf_row = (id: String) => { WordCount(id + "_h", 1) }

    def udf1Func(id: String): String = {
      id + "_hell"
    }
    spark.udf.register("udf1Func", udf1Func _)
    spark.udf.register("udf1", udf1)
    spark.udf.register("udf_row", udf_row)
    val udf_func = udf(udf1)

    ds.select(udf_func($"id")).show
    val nds = spark.sql(s"select * , udf1(id),udf1Func(id),udf_row(id) from test")
    nds.printSchema()
    nds.show


  }

}
