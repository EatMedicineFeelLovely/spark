package com.spark.sql.engine

import com.antlr4.parser.{CustomSqlParserLexer, CustomSqlParserParser}
import com.antrl4.visit.parser.impl.{CustomSqlParserVisitorImpl, ExtErrorListener}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

class AbstractSqlEngine(val spark: SparkSession) {
  val udfManager = new mutable.HashMap[String, UserDefinedFunction]

  def register[RT: TypeTag, A1: TypeTag](
                                          name: String,
                                          func: Function1[A1, RT]): UserDefinedFunction = {
    val udf = spark.udf.register(name, func)
    udfManager.put(name, udf)
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
    val state = parser.customcal()
    val visitor = new CustomSqlParserVisitorImpl()
    visitor.visit(state)
  }
}
