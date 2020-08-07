package com.spark.learn.sql.test

import com.spark.learn.test.bean.TestCaseClass.WordCount
import com.spark.learn.test.bean.SqlSchameUtil._
import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{
  DataType,
  IntegerType,
  StringType,
  StructField,
  StructType
}

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._

class SparkSqlCoreTest extends SparkFunSuite with ParamFunSuite {

  import spark.implicits._

  /**
    *
    */
  test("spark sql dataset") {
    // 方法1
    val df =
      spark.createDataset(List(Row("a", 1)))(RowEncoder(WORD_COUNT_SCHAME))
    df.show()
    // 方法2
    val df2 = spark.createDataset(List(WordCount("a", 1))) // (RowEncoder(WORD_COUNT_SCHAME))
    df2.show()
  }

  /**
    *
    */
  test("读取json文件为dataset") {
    val df = spark.read.json(
      "/Users/eminem/workspace/git_pro/spark-learn/resources/datafile/json.log")
    df.printSchema()
    // df.show()
    // 将json字符串转为struct结构
    df.withColumn("json_value",
                  from_json($"json".cast(StringType), JSON_SCHAME))
      .show()

    // df.select("c.k").show
  }

  /**
    *
    */
  test("array结构字段") {
    val ds = spark.read.json(
      "/Users/eminem/workspace/git_pro/spark-learn/resources/datafile/arr_map_struc.json")
    //ds.printSchema()
    // ds.show()

    ds.selectExpr("arrtype[0]").show
    ds.selectExpr("arrtype[100]").show

    ds.select(size($"arrtype").as("size_arr")).show

    ds.createOrReplaceTempView("test")
    spark.sql("select *,arrtype[1],size(arrtype) from test").show

    // array_contains
    spark
      .sql(
        "select *,arrtype[1],size(arrtype),array_contains(arrtype, '1') from test")
      .show

    // 必须是同类型
    spark.sql("select *,array(id,id) from test").show

  }

  /**
    *
    */
  test("struct结构的读取和创建") {
    val ds = spark.read.json(
      "/Users/eminem/workspace/git_pro/spark-learn/resources/datafile/arr_map_struc.json")
    ds.select($"structtype".getField("a")).show()

    ds.createOrReplaceTempView("test")
    spark.sql("select *,structtype.a from test")

  }

  /**
    *
    */
  test("map结构的读取和创建") {
    val ds = spark.read
      .json(
        "/Users/eminem/workspace/git_pro/spark-learn/resources/datafile/arr_map_struc.json")
      .withColumn("maptype", map($"id", $"structtype"))
    ds.show

    ds.select(expr("maptype['id'] as mid"), expr("maptype['null'] as mnull")).show

    ds.createOrReplaceTempView("test")
    spark.sql(s"select *,maptype['id'], maptype['null'] from test").show

    // map_form_arrays 将两个数组合成map
  }
}
