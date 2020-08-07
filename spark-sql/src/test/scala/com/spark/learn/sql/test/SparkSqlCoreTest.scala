package com.spark.learn.sql.test

import com.spark.code.udt.RegisterSet
import com.spark.learn.test.bean.TestCaseClass.{PageViewLog, WordCount}
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
import org.apache.spark.sql.types.udt.HyperLogLog2

import scala.collection.mutable

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

    ds.select(expr("maptype['id'] as mid"), expr("maptype['null'] as mnull"))
      .show

    ds.createOrReplaceTempView("test")
    spark.sql(s"select *,maptype['id'], maptype['null'] from test").show

    // map_form_arrays 将两个数组合成map
  }

  /**
    *
    */
  test("spark udt 自定义 类型") {
    def log2m(rsd: Double): Int =
      (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2)).toInt

    // 计算hyper
    val hyper_udf = (list: mutable.WrappedArray[String]) => {
      val hyperLogLog2 = HyperLogLog2(log2m(0.05), new RegisterSet(1 << log2m(0.05)))
      list.foreach(r => hyperLogLog2.offer(r))
      (list.size, hyperLogLog2)
    }
    // 从hyper信息中算uv
    val hyper_uv = (hyper: HyperLogLog2) => {
      hyper.cardinality()
    }

    spark.udf.register("hyper_udf", hyper_udf)
    spark.udf.register("hyper_uv", hyper_uv)

    val ds = spark.createDataset(
      Array(PageViewLog("url1", "user1"),
            PageViewLog("url1", "user2"),
            PageViewLog("url1", "user1"),
            PageViewLog("url2", "user4"),
            PageViewLog("url2", "user4")))
    ds
      .groupBy($"url")
      .agg(collect_list("user").as("user_list"))
      .withColumn("pv_uv", expr("hyper_udf(user_list)"))
      .withColumn("pv", $"pv_uv".getField("_1"))
      .withColumn("uv", $"pv_uv".getField("_2"))
      .show


  }
}
