package com.spark.learn

import com.spark.learn.udf.SparkSqlMaxUDFA
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object UDFATest {
  val strcut = RowEncoder(StructType(
    Array(StructField("a", StringType, true),
          StructField("b", StringType, true))))
  case class WC(w: String)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(Array(s"""{"a":"a"}"""))
    val df1 = spark.read.json(rdd).map(r => Row.merge(r, Row("hello")))(strcut).show
    // spark.createDataFrame(df1, strcut).show
    //    spark.udf.register("mymax", new SparkSqlMaxUDFA)
//    spark.sql(s"""select key, mymax(c), collect_list(c) from test group by key """)
//      .explain()

  }
}
