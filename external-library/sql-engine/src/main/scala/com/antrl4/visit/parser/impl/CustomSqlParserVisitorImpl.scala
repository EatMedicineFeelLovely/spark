package com.antrl4.visit.parser.impl

import com.antlr4.parser.{CustomSqlParserBaseVisitor, CustomSqlParserParser}
import com.antlr4.parser.CustomSqlParserParser.{
  BooleanExpressionContext,
  LogicalBinaryContext,
  PredicatedContext
}
import com.antrl4.visit.operation.impl._
import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory.{
  ColumnsBasicInfo,
  ColumnsInfo,
  ColumnsSchameInfo
}
import com.antrl4.visit.operation.impl.TableInfoVisitOperationFactory._
import com.spark.sql.engine.common.SQLEnumeration._

import scala.collection.JavaConverters._

class CustomSqlParserVisitorImpl extends CustomSqlParserBaseVisitor[AnyRef] {

//  override def visitCheckpointStatement(
//      ctx: CustomSqlParserParser.CheckpointStatementContext): AnyRef = {
//    CheckpointVisitOperation(ctx.table.getText, ctx.location.getText)
//  }

//
//  /**
//    *
//    * @param ctx the parse tree
//   **/
//  override def visitSelectHbase(
//      ctx: CustomSqlParserParser.SelectHbaseContext): AnyRef = {
//    val hbaseInfo = ctx.hBaseSearchState()
//    val familyInfos = SqlParserVisitorUtils.getColumnsInfo(hbaseInfo.cols.asScala)
//    HbaseSearchInfoOperation(hbaseInfo.tableName.getText,
//                             hbaseInfo.key.getText,
//                             familyInfos)
//  }

  override def visitSimpleQuery(
      ctx: CustomSqlParserParser.SimpleQueryContext): AnyRef = {
    TableQueryInfo(
      visitTableNameDefineState(ctx.createTableDefineState().createTablename),
      visitTableNameDefineState(ctx.simpleQueryState().sourceTable), // 如果是join的话就是lefttable
      SqlParserVisitorUtils.getColumnsInfo(ctx.simpleQueryState().cols.asScala),
      visitJoin(ctx.join()),
      visitWhere(ctx.where()).asInstanceOf[WhereInfo],
      visitConf(ctx.conf)
    )
  }

  /**
    *
    * @param ctx the parse tree
    *    */
  override def visitWhere(ctx: CustomSqlParserParser.WhereContext): AnyRef = {
    if (ctx == null) null
    else {

      ctx.whereExpression


      super.visitWhere(ctx)
    }
  }

  /**
    *
    * @param ctx the parse tree
    *    */
  override def visitValueExpression(
      ctx: CustomSqlParserParser.ValueExpressionContext): WhereInfo = {
    WhereInfo(
      SqlParserVisitorUtils.getColumnInfo(ctx.left.columnUdfState()),
      SqlParserVisitorUtils.getColumnInfo(ctx.right.columnUdfState()),
      ctx.comparisonOperator().operator.getText
    )
  }

  override def visitPredicated(ctx: PredicatedContext): WhereInfo = {
    visitValueExpression(ctx.valueExpression())
  }

  /**
    * 只需循环遍历left，参照spark源码写吧
    * @param ctx the parse tree
    *    */
  override def visitLogicalBinary(ctx: LogicalBinaryContext): WhereInfo = {
    if (ctx.left.isInstanceOf[PredicatedContext]) {
      visitPredicated(ctx.left.asInstanceOf[PredicatedContext])
    } else {
      visitLogicalBinary(ctx.left.asInstanceOf[LogicalBinaryContext])
    }
    null
  }

  override def visitConf(ctx: CustomSqlParserParser.ConfContext): ConfInfo = {
    if (ctx != null) {
      ConfInfo(ctx.confParam.asScala.map(x => {
        x.key.getText.toUpperCase() -> x.value.getText
      }).toMap)
    } else
      null
  }

  override def visitJoin(ctx: CustomSqlParserParser.JoinContext): JoinInfo = {
    if (ctx != null) {
      val rightTable = visitTableNameDefineState(ctx.rightTable)
      if (rightTable != null) {
        val whereInfo = visitJoinExpression(ctx.joinExpression())
        if (rightTable.tableType == HBASE_TABLE
            && whereInfo.left.colName.toString.toUpperCase() != "ROWKEY") {
          throw new Exception("hbase join 的 on条件必须为 ： rowkey")
        }
        JoinInfo(rightTable, Seq(whereInfo))
      } else {
        null
      }
    } else null
  }

  /**
    *
    * @param ctx the parse tree
    *    */
  override def visitJoinExpression(
      ctx: CustomSqlParserParser.JoinExpressionContext): WhereInfo = {
    WhereInfo(SqlParserVisitorUtils.getColumnInfo(ctx.left.columnUdfState()),
              SqlParserVisitorUtils.getColumnInfo(ctx.right.columnUdfState()),
              "=")
  }

  /**
    *
    * @param ctx the parse tree
    *    */
  override def visitTableNameDefineState(
      ctx: CustomSqlParserParser.TableNameDefineStateContext): TableInfo = {
    if (ctx != null) {
      if (ctx.db != null)
        TableInfo(ctx.db.getText + "." + ctx.table.getText, HIVE_TABLE)
      else {
        if (ctx.table.getText.contains(":"))
          TableInfo(ctx.table.getText, HBASE_TABLE)
        else
          TableInfo(ctx.table.getText, BASIC_TABLE)
      }
    } else {
      null
    }
  }

//
//  override def visitQueryStateTest(ctx: CustomSqlParserParser.QueryStateTestContext): AnyRef = {
//    val colsInfo = SqlParserVisitorUtils.getColumnsInfo(ctx.cols.asScala)
//    TableSelectInfoOperation(ctx.tablename.getText, colsInfo)
//  }
//
//  override def visitQuerystate(ctx: CustomSqlParserParser.QuerystateContext): AnyRef = {
//     visitQueryStateTest(ctx.queryStateTest())
//  }

//  /**
//    *
//    * @param ctx the parse tree
//   **/
//  override def visitHbaseJoin(
//      ctx: CustomSqlParserParser.HbaseJoinContext): AnyRef = {
//    val hbasestate = ctx.hbaseJoinState()
//    val tb = hbasestate.createTableDefineState()
//    val createTablename = if (tb == null) "" else tb.createTablename.getText
//    val cols = SqlParserVisitorUtils.getColumnsInfo(hbasestate.cols.asScala)
//    TableJoinHbaseInfoOperation(createTablename,
//                                cols,
//                                hbasestate.tablename.getText,
//                                hbasestate.hbasetable.getText,
//                                hbasestate.joinkey.getText,
//                                hbasestate.zk.getText)
//  }

//  /**
//   *
//   * @param ctx the parse tree
//   *    */
//  override def visitCollectState(ctx: CustomSqlParserParser.CollectStateContext): AnyRef = {
//      val dfCountState = ctx.dataframCollectState()
//      val paramName = dfCountState.paramName.getText
//      val tableName = dfCountState.tableName.getText
//      val actionName = dfCountState.actionName.getText
//    DatasetCollectInfoOperation(paramName, tableName, actionName)
//  }
}
