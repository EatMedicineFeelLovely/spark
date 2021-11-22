package com.spark.sql.engine

import com.antlr4.parser.{CustomSqlParserLexer, CustomSqlParserParser}
import com.antrl4.visit.parser.impl.{CustomSqlParserVisitorImpl, ExtErrorListener}
import com.spark.sql.engine.common.UserDefinedFunction2
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedFunction}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

class AbstractSqlEngine(val spark: SparkSession) {
  val udfManager = new mutable.HashMap[String, UserDefinedFunction2]

  def register[RT: TypeTag, A1: TypeTag](
                                          name: String,
                                          func: (A1) => RT): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Nil
    val udf = spark.udf.register(name, func)
    udfManager.put(name, UserDefinedFunction2(1,
      func,
      outputEncoder.get.dataTypeAndNullable.dataType,
      Some(inputEncoders.map(x =>{
        x.get.dataTypeAndNullable.dataType
      }))))
    udf
  }

  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag](
                                          name: String,
                                          func: (A1, A2) => RT): UserDefinedFunction = {
    val outputEncoder = Try(ExpressionEncoder[RT]()).toOption
    val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Try(ExpressionEncoder[A1]()).toOption :: Nil
    val udf = spark.udf.register(name, func)
    udfManager.put(name, UserDefinedFunction2(2,
      func,
      outputEncoder.get.dataTypeAndNullable.dataType,
      Some(inputEncoders.map(x =>{
        x.get.dataTypeAndNullable.dataType
      }))))
    udf
  }


  /**
   *
   * @param sqltext
   * @return
   */
  def visit(sqltext: String): AnyRef = {
    val inputStream = CharStreams.fromString(sqltext)
    val lexer = new CustomSqlParserLexer(inputStream)
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new CustomSqlParserParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(new ExtErrorListener) // 用来抛异常的
    val state = parser.statment()
    val visitor = new CustomSqlParserVisitorImpl()
    visitor.visit(state)
  }
}
