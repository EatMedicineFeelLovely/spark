package com.spark.learn.sql.test

import com.spark.learn.test.bean.TestCaseClass.WordCount
import com.spark.learn.test.bean.SqlSchameUtil._
import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

import scala.collection.JavaConverters._

class SparkSqlCoreTest extends SparkFunSuite with ParamFunSuite {

  import spark.implicits._

  test("spark sql createSet") {
    // 方法1
    val df = spark.createDataset(List(Row("a", 1)))(RowEncoder(WORD_COUNT_SCHAME))
    df.show()
    // 方法2
    val df2 = spark.createDataset(List(WordCount("a", 1))) // (RowEncoder(WORD_COUNT_SCHAME))
     df2.show()
  }
}
