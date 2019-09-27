package com.spark.learn.udf

import com.spark.code.util.ClassFuncReflectUtils
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
    runMappartitionFunc(spark)
  }

  /**
    * 动态UDF。通过 字符串来自定义udf
    * @param spark
    */
  def runUdf(spark: SparkSession): Unit = {
    import spark.implicits._
    val (funcName, fun, argumentTypes, returnType) =
      SparkSqlThermalloadUdfHandler(s"""def myUDFtoUps(str:String):String = {
                                       |str+100
                                       |}""".stripMargin)
    val inputTypes = Try(argumentTypes.toList).toOption // IntegerType
    def builder(e: Seq[Expression]) =
      ScalaUDF(fun,
               returnType,
               e,
               inputTypes.map(_.map(_ => true)).getOrElse(Seq.empty[Boolean]),
               inputTypes.getOrElse(Nil),
               Some(funcName))
    spark.sessionState.functionRegistry
      .registerFunction(new FunctionIdentifier(funcName), builder)
    spark.sparkContext
      .parallelize(Array("a", "b"))
      .toDF("a")
      .createOrReplaceTempView("test")
    spark.sql(s"select myUDFtoUps(a) as A from test").show
  }

  /**
    * 动态func。用于mappartition或者其他得算子
    * @param spark
    */
  def runMappartitionFunc(spark: SparkSession): Unit = {
    import spark.implicits._
    val funCode = s"""
                     |import org.apache.spark.sql.Row
               |def toUpstr(itor: Iterator[Row])={
               |    itor.map(r => Row(r.toSeq.map(_.toString.toUpperCase): _*))
               |  }""".stripMargin
    val df = spark.sparkContext
      .parallelize(Array("a", "b"))
      .toDF("a")
    df.mapPartitions(x => {
        val funcInfo = ClassFuncReflectUtils.createFunc(funCode, "toUpstr")
        funcInfo.call[Iterator[Row]](x)
      })(RowEncoder(df.schema))
      .show(false)
  }
}
