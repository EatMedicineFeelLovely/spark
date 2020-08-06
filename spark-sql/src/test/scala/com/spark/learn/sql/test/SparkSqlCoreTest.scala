package com.spark.learn.sql.test

import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

import scala.collection.JavaConverters._

class SparkSqlCoreTest extends SparkFunSuite with ParamFunSuite {
  import spark.implicits._
  test("spark sql createDataFrame") {
    case class WordCount(word: String, count: Long)
    // 方法1
   val df =  spark.createDataFrame(
      List(Row("a")).asJava,
      StructType(Array(
        StructField("a", StringType, false)
      )))
    df.show()
    // 方法2
    val df2 = spark.createDataFrame(
      spark.sparkContext.makeRDD(Array(WordCount("a", 1))),
      WordCount.getClass)
    df2.show()

  }
}
