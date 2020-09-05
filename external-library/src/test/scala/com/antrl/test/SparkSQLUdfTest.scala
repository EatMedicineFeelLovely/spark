package com.antrl.test

import com.spark.learn.test.core.{ParamFunSuite, SparkFunSuite}
import org.apache.spark.sql.api.java.{UDF0, UDF1, UDF14}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

class SparkSQLUdfTest extends SparkFunSuite with ParamFunSuite {

  /**
   *
   */
  test("udfTest") {
    val udff = spark.udf.register("testUdf", (a: String) => { Array(a) })
    val schame = new StructType()
      .add("word", "string")
      .add("count", "int")
    val df = spark.createDataset(Seq(Row("word1", 1), Row("word2", 2)))(
      RowEncoder(schame))
    df.createOrReplaceTempView("lefttable")
    // spark.sql(s"""select testUdf(word) from lefttable""").show
        val ff =
          udff.inputTypes.size match {
            case 1 => udff.f.asInstanceOf[Function1[Any, udff.dataType.type ]]
          }

    spark.table("lefttable").mapPartitions(x => {
      x.map(r => {
        Row(ff(r.get(0)+":hello"))
      })
    })(RowEncoder(StructType(Array(StructField("b", udff.dataType)))))
      .show

//    val ff =
//      udff.inputTypes.size match {
//        case 1 => udff.f.asInstanceOf[Function1[Any, udff.dataType.type ]]
//      }
//
//
//
//    spark.udf.register("testUdf2", ff)
//
//    println(ff("hellp"))
  }
}
