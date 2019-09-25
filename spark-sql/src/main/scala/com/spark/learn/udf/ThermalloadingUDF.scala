package com.spark.learn.udf

import com.spark.code.util.{ClassCreateUtils, ScalaGenerateFuns}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}

import scala.util.Try

object ThermalloadingUDF extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val (fun, argumentTypes, returnType) = ScalaGenerateFuns(
      s"""def myUDFtoUps(str:Int):Int = {
         |str*100
         |}""".stripMargin)
    val inputTypes = Try(argumentTypes.toList).toOption // IntegerType
    def builder(e: Seq[Expression]) =
      ScalaUDF(fun,
               returnType,
               e,
               inputTypes.map(_.map(_ => true)).getOrElse(Seq.empty[Boolean]),
               inputTypes.getOrElse(Nil),
               Some("myUDFtoUps"))

    spark.sessionState.functionRegistry
      .registerFunction(new FunctionIdentifier("myUDFtoUps"), builder)
    spark.udf.register("", () => {})
//    spark.sparkContext
//      .parallelize(1 to 100)
//      .toDF("a")
//      .createOrReplaceTempView("test")
//    spark.sql("select myUDFtoUps(a) as A from test" ) .show
//

    spark.sparkContext
      .parallelize(Array("a", "b", "c"))
      .toDF("a")
      // .mapPartitions(f _)(RowEncoder[])
  }

  def f(itor: Iterator[Row]) = {
    itor.map(r => { Row(r.getString(0).toUpperCase) })
  }
}
