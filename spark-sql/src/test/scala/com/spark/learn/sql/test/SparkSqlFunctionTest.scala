package com.spark.learn.sql.test

import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import org.apache.spark.sql.functions._

/**
  * sql里面的自带函数
  */
class SparkSqlFunctionTest extends SparkFunSuite with ParamFunSuite {
  import spark.implicits._

  test("一行表多行 explode") {
    val ds = spark.read.json(
      "/Users/eminem/workspace/git_pro/spark-learn/resources/datafile/arr_map_struc.json")
    ds.printSchema()
    ds.show()

    val explodeDs = ds.withColumn("explode_arr", explode($"arrtype"))
    explodeDs.printSchema()
    explodeDs.show()

    ds.createOrReplaceTempView("test")
    val sqlds =
      spark.sql("select * , explode(arrtype) as explode_arr from test")
    sqlds.printSchema()
    sqlds.show()
  }

  test("pivot"){

  }

  /**
   *
   */
  test("collect_set") {
    val ds = spark.read.json(
      "/Users/eminem/workspace/git_pro/spark-learn/resources/datafile/arr_map_struc.json")
    ds.printSchema()
    ds.show()

    ds.groupBy("id")
      .agg(collect_set($"structtype").as("collect_set_v"))
      .show()

    ds.createOrReplaceTempView("test")
    spark
      .sql(
        s"select id,collect_set(structtype) as collect_set_v from test group by id")
      .show

  }

}
