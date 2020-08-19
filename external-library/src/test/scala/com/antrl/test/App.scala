package com.antrl.test

import com.antlr4.parser.{CustomSqlParserLexer, CustomSqlParserParser}
import com.antrl4.visit.operation.impl.{AbstractVisitOperation, CheckpointVisitOperation, HbaseSearchInfoOperation, HelloWordVisitOperation}
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


    visit(inputStream3) match {
      case a: HelloWordVisitOperation => println(a)
      case b: CheckpointVisitOperation => println(b)
      case c: HbaseSearchInfoOperation => println(c)
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
