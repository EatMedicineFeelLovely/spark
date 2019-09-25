package com.spark.learn

import org.apache.spark.sql.{Row, SparkSession}
import com.spark.learn.cassclass.kv
import com.spark.learn.udf.SparkSqlMaxUDFA
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
object TestSparkSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext
      .parallelize(1 to 10)
      .toDF("b")
      .foreachPartition({ x: Iterator[Row] =>

      })
    // leftJoinTest(spark)
    //arrayColumns(spark)
    // udfTest(spark)
  }

  /**
    *
    * @param spark
    */
  def leftJoinTest(spark: SparkSession): Unit = {
    import spark.implicits._
    spark.sparkContext
      .parallelize(1 to 10)
      .toDF("b")
      .createOrReplaceTempView("left1")

    spark.sparkContext
      .parallelize(Array((1, 2)))
      .toDF("a", "c")
      .createOrReplaceTempView("left2")

    spark
      .sql(s"""select *,case when l2.a is not null then 'a'
           |when l2.c is not null then 'c'
           |else 'ok' end as result
           |from left1 l1
           |full join
           |left2 l2
           |on l1.b=l2.c""".stripMargin)
      // .filter($"c".isNull)
      .show

  }

  /**
    * 自定义uid
    */
  val testUdf = (x: Int, b: Int) => {
    x + b
  }

  /**
    *
    * @param broadcast
    * @return
    */
  def getBrocastUDF(broadcast: Broadcast[Array[Int]]): (Int) => Array[Int] = {
    (a: Int) =>
      broadcast.value
  }
  case class ArrayBean(a: Array[Int], b: String)

  /**
    *
    * @param spark
    */
  def udfTest(spark: SparkSession): Unit = {
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val broadcast = spark.sparkContext.broadcast(Array(1, 2, 3, 4))
    spark.sparkContext
      .parallelize(1 to 10)
      .map(x => kv(x / 5, x))
      .toDF()
      .cache()
      .createOrReplaceTempView("testTable")

    spark.udf.register("brocast_udf", getBrocastUDF(broadcast))
    spark.udf.register("test_udf", testUdf)
    spark.udf.register("self_max", new SparkSqlMaxUDFA)
    //    spark.sql(s"""
    //                 |SELECT k,v,test_udf(k,v) as k_v_sum
    //                 |FROM testTable
    //                 |WHERE k > ${10}
    //      """.stripMargin).show
    spark.sql(s"""
                 |SELECT brocast_udf(k) as brocast_udf,self_max(v) as max_v
                 |FROM testTable
                 |WHERE k <100
                 |group by k
      """.stripMargin).withColumn("ex", explode($"brocast_udf")).show
  }

  /**
    *
    * @param spark
    */
  def arrayColumns(spark: SparkSession) = {
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.sparkContext
      .parallelize(1 to 100)
      .map(x => ArrayBean((1 to x).toArray, x.toString))
      .toDF()
      .cache()
    //.createOrReplaceTempView("testTable")
    df.show
  }

}
