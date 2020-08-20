package com.antrl.test

import com.antlr4.parser.{CustomSqlParserLexer, CustomSqlParserParser}
import com.antrl4.visit.operation.impl.{AbstractVisitOperation, CheckpointVisitOperation, HbaseJoinInfoOperation, HbaseSearchInfoOperation, HelloWordVisitOperation}
import com.antrl4.visit.parser.impl.CustomSqlParserVisitorImpl
import org.antlr.v4.runtime.{CharStreams, CodePointCharStream, CommonTokenStream}

/**
 * @author ${user.name}
 */
object App {
  def main(args: Array[String]) {

    val inputStream = CharStreams.fromString("print 'xxx'")
    val inputStream2 = CharStreams.fromString("checkpoint ab.`table` into 'hdfs:///ssxsxs/ssxxs'")

    val inputStream3 = CharStreams.fromString(
      "select info(name1 string , name2 string),info3(name3 string , name4 string) FROM hbasetable where key='abc'")

    val inputStream4 = CharStreams.fromString(
      "select a,b,info.ac FROM lefttable JOIN default:hbasetable ON ROWKEY = joinkeyxxx")


    visit(inputStream4) match {
      case a: HelloWordVisitOperation => println(a)
      case b: CheckpointVisitOperation => println(b)
      case c: HbaseSearchInfoOperation => println(c)
      case d: HbaseJoinInfoOperation => println(d)
      case _ =>
    }
  }


  def visit(inputStream: CodePointCharStream): AbstractVisitOperation = {
    val lexer = new CustomSqlParserLexer(inputStream)
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new CustomSqlParserParser(tokenStream)
    val state = parser.customcal()
    val visitor = new CustomSqlParserVisitorImpl()
    visitor.visit(state)
  }

}
