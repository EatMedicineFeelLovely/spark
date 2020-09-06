package com.antrl4.visit.parser.impl

import com.antlr4.parser.CustomSqlParserParser
import com.antlr4.parser.CustomSqlParserParser.ColumnDefineStateContext
import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory._
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.TerminalNodeImpl

import scala.collection.JavaConverters._
import scala.collection.mutable
object SqlParserVisitorUtils {

  def getColumnsInfo(cols: mutable.Buffer[CustomSqlParserParser.ColumnUdfStateContext]): Seq[ColumnsInfo] ={
    cols.map(x => {
      if (x.udfname == null)
        ColumnsInfo(
          ColumnsNameInfo(x.col.colname.getText,
            if(x.col.family != null) x.col.family.getText else null),
          ColumnsSchameInfo(null, -1, null),
          null)
      else {
        val asColName = if (x.asColName != null) x.asColName.getText()
        else s"${getUdfSqlText(x)}"
        ColumnsInfo(
          ColumnsNameInfo(asColName),
          ColumnsSchameInfo(null, -1, null),
          UdfInfo(x.udfname.getText, getColumnsInfo(x.paramcols.asScala), null)
        )
      }
    })
  }

  def getUdfSqlText(cxt: ParserRuleContext): String ={
    cxt.children.asScala.map(p => {
      p match {
        case p: TerminalNodeImpl =>
          p.toString
        case c : CustomSqlParserParser.ColumnUdfStateContext =>
          getUdfSqlText(c)
        case d: ColumnDefineStateContext =>
          getUdfSqlText(d)
      }
    }).mkString
  }
}
