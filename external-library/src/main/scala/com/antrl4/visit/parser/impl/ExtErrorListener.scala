package com.antrl4.visit.parser.impl

import org.antlr.v4.runtime.{BaseErrorListener, Parser, RecognitionException, Recognizer}
import scala.collection.JavaConverters._
class ExtErrorListener extends BaseErrorListener {
  override def syntaxError(recognizer: Recognizer[_, _],
                           offendingSymbol: Any,
                           line: Int,
                           charPositionInLine: Int,
                           msg: String,
                           e: RecognitionException): Unit = {

    val ruleLine = recognizer.asInstanceOf[Parser].getRuleInvocationStack()
    val ruleOrdered = ruleLine.asScala.reverse
    val errorMessage =
      s"""[语法错误] : ${ruleOrdered.mkString("->")} \n
         | 第 ${line} 行 ， 第 ${charPositionInLine} 列 ： ${offendingSymbol} \n
         | errorMsg : ${msg}
         |""".stripMargin
    throw new Exception(errorMessage)
  }
}
