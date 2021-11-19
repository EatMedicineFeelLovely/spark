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
      Array(Row("2018-01", "项目1", 100), Row("2018-01", "项目1", 100), Row("2018-02", "项目2", 200), Row("2018-01", "项目44", 200)))(
      RowEncoder(structype))

    df.groupBy("day")
      .pivot("item", List("项目1", "项目2"))
      .agg(sum("income"))
      .show

//    df.groupBy("day")
//      .pivot("item", List("项目1", "项目2", "项目3"))
//      .agg(sum("income"))
//      .show
//
//    df.groupBy("day")
//      .pivot("item", List("项目1"))
//      .agg(sum("income"))
//      .show

    // 全部字段的实现原理是把数据distinct一遍然后collect出来拼进去的
    df.groupBy("day")
      .pivot("item")
      .agg(sum("income"))
      .show

    df.createOrReplaceTempView("test")
    // 语句里面虽然没有group by，但是只有没用上的，全部都是group by 的key
    spark.sql(
      s"""select * from test
         |                 pivot
         |                 (
         |                     sum(`income`) as income for item in('项目1' as a_1,'项目2' as a_2)
         |                 )""".stripMargin)
      .show()
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

  test("pivot2") {
    val structype = new StructType()
      .add("province", StringType)
      .add("count_id", StringType)
      .add("account_id_times", StringType)
      .add("log_time", StringType)
    val df = spark.createDataset(
      Array(Row("P1", "user1", "account_id_1","time1"),
        Row("P1", "user2", "account_id_2","time2"),
        Row("P1", "user3", "account_id_3","time3"),
        Row("P2", "user1", "account_id_1","time1"),
        Row("P2", "user2", "account_id_2","time2"),
        Row("P3", "user1", "account_id_1","time"),
        Row("P3", "user2", "account_id_2","time2"),
        Row("P3", "user3", "account_id_3","time3")))(
      RowEncoder(structype))

    df.groupBy("province")
      .pivot("account_id_times", List("account_id_1", "account_id_2","account_id_3"))
      .agg(max("count_id"), max("log_time"))
      .show

    //    df.groupBy("day")
    //      .pivot("item", List("项目1", "项目2", "项目3"))
    //      .agg(sum("income"))
    //      .show
    //
    //    df.groupBy("day")
    //      .pivot("item", List("项目1"))
    //      .agg(sum("income"))
    //      .show

    // 全部字段的实现原理是把数据distinct一遍然后collect出来拼进去的
//    df.groupBy("day")
//      .pivot("item")
//      .agg(sum("income"))
//      .show

//    df.createOrReplaceTempView("test")
    // 语句里面虽然没有group by，但是只有没用上的，全部都是group by 的key
//    spark.sql(
//      s"""select * from test
//         |                 pivot
//         |                 (
//         |                     sum(`income`) as income for item in('项目1' as a_1,'项目2' as a_2)
//         |                 )""".stripMargin)
//      .show()
  }
}
