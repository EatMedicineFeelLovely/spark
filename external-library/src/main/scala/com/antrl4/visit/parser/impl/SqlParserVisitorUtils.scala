package com.antrl4.visit.parser.impl

import com.antlr4.parser.CustomSqlParserParser
import com.antlr4.parser.CustomSqlParserParser.ColumnDefineStateContext
import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory._
import com.spark.sql.engine.common.SQLEnumeration
import com.spark.sql.engine.common.SQLEnumeration._
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.TerminalNodeImpl
import org.apache.spark.sql.types.{
  DoubleType,
  LongType,
  StringType,
  StructField
}

import scala.collection.JavaConverters._
import scala.collection.mutable
object SqlParserVisitorUtils {

  def getColumnsInfo(
      cols: mutable.Buffer[CustomSqlParserParser.ColumnUdfStateContext])
    : Seq[ColumnsInfo] = {
    cols.map(x => {
      getColTransType(x) match {
        case BASIC_COL =>
          ColumnsInfo(
            BASIC_COL,
            ColumnsBasicInfo(x.col.colname.getText, null, null),
            ColumnsSchameInfo(null, -1, null),
            null
          )
        case HBASE_COL =>
          val basicInfo =
            ColumnsBasicInfo(x.col.colname.getText, null, x.col.family.getText)
          ColumnsInfo(
            HBASE_COL,
            basicInfo,
            ColumnsSchameInfo(null,
                              -1,
                              StructField(basicInfo.toString, StringType)),
            null
          )
        case CONSTANTPARAM_COL =>
          var consV = x.constantParam.getText
          val structType =
            if (consV.startsWith("'") || consV.startsWith("\"")) {
              consV =
                if (consV.size > 2) consV.substring(1, consV.size - 2) else ""
              StringType
            } else {
              if (consV.contains(".")) {
                DoubleType
              } else {
                LongType
              }
            }
          val colName = if (x.asColName == null) consV else x.asColName.getText
          val basicInfo =
            ColumnsBasicInfo(colName, consV, null)
          ColumnsInfo(
            CONSTANTPARAM_COL,
            basicInfo,
            ColumnsSchameInfo(null,
                              -1,
                              StructField(basicInfo.toString, structType)),
            null
          )
        case UDF_COL =>
          val asColName =
            if (x.asColName != null) x.asColName.getText()
            else s"${getUdfColName(x)}"
          ColumnsInfo(
            UDF_COL,
            ColumnsBasicInfo(asColName, null),
            ColumnsSchameInfo(null, -1, null),
            UdfInfo(x.udfname.getText,
                    getColumnsInfo(x.paramcols.asScala),
                    null)
          )
        case _ =>
          throw new Exception("字段类型不匹配")
      }
    })
  }

  /**
    * 当udf没用as colname的时候，默认的获取colname的方法
    * @param cxt
    * @return
    */
  def getUdfColName(cxt: ParserRuleContext): String = {
    cxt.children.asScala
      .map(p => {
        p match {
          case p: TerminalNodeImpl =>
            var consV = p.toString
            if (consV.startsWith("'") || consV.startsWith("\"")) {
              consV =
                if (consV.size > 2) consV.substring(1, consV.size - 2) else ""
              StringType
            }
            consV
          case c: CustomSqlParserParser.ColumnUdfStateContext =>
            getUdfColName(c)
          case d: ColumnDefineStateContext =>
            getUdfColName(d)
        }
      })
      .mkString
  }

  /**
   *
   * @param x
   * @return
   */
  def getColTransType(
      x: CustomSqlParserParser.ColumnUdfStateContext): SQLEnumeration = {
    if (x.udfname == null) {
      if (x.constantParam != null) {
        CONSTANTPARAM_COL
      } else {
        if (x.col.family != null) {
          HBASE_COL
        } else
          BASIC_COL
      }
    } else {
      UDF_COL
    }
  }
}
