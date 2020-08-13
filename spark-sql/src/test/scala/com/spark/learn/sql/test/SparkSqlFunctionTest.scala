package com.spark.learn.sql.test

import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.Row

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

  test("lateral view explode") {
    val structype = new StructType()
      .add("day", StringType)
      .add("item", StringType)
    val df = spark.createDataset(
      Array(Row("2018-01", "项目1,项目2,项目3"), Row("2018-01", "项目1,项目4"), Row("2018-01", "项目2,项目2")))(
      RowEncoder(structype))
    df.createOrReplaceTempView("test")

    spark.sql(
      s"""select t1.*,ep
         | from test t1 lateral view explode(split(item,',')) as ep""".stripMargin)
      .show()

  }
  /**
   * 将数据转为字段
   */
  test("pivot") {
    val structype = new StructType()
      .add("day", StringType)
      .add("item", StringType)
      .add("income", IntegerType)
    val df = spark.createDataset(
      Array(Row("2018-01", "项目1", 100), Row("2018-01", "项目1", 100), Row("2018-01", "项目2", 200), Row("2018-01", "项目44", 200)))(
      RowEncoder(structype))
    df.groupBy("day")
      .pivot("item", List("项目1", "项目2"))
      .agg(sum("income"))
      .show

    df.groupBy("day")
      .pivot("item", List("项目1", "项目2", "项目3"))
      .agg(sum("income"))
      .show

    df.groupBy("day")
      .pivot("item", List("项目1"))
      .agg(sum("income"))
      .show

    // 全部都做字段
    df.groupBy("day")
      .pivot("item")
      .agg(sum("income"))
      .show

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
