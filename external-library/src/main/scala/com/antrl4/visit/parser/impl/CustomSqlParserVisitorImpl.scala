package com.antrl4.visit.parser.impl

import com.antlr4.parser.{CustomSqlParserBaseVisitor, CustomSqlParserParser}
import com.antrl4.visit.operation.impl._
import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory._
import com.antrl4.visit.operation.impl.TableInfoVisitOperationFactory.{DatasetCollectInfoOperation, TableJoinHbaseInfoOperation, TableSelectInfoOperation}

import scala.collection.JavaConverters._

class CustomSqlParserVisitorImpl extends CustomSqlParserBaseVisitor[AnyRef] {

  override def visitHelloWordStatement(
      ctx: CustomSqlParserParser.HelloWordStatementContext): AnyRef = {
    HelloWordVisitOperation(ctx.word.getText)
  }

  override def visitCheckpointStatement(
      ctx: CustomSqlParserParser.CheckpointStatementContext): AnyRef = {
    CheckpointVisitOperation(ctx.table.getText, ctx.location.getText)
  }

  /**
    * 先进的这个，再进 @link  visitCheckpointStatement
    *
    * @param ctx the parse tree
   **/
  override def visitCheckpoint(
      ctx: CustomSqlParserParser.CheckpointContext): AnyRef = {
    visitCheckpointStatement(ctx.checkpointStatement())
  }

  override def visitHelloWord(
      ctx: CustomSqlParserParser.HelloWordContext): AnyRef = {
    visitHelloWordStatement(ctx.helloWordStatement())
  }

  /**
    *
    * @param ctx the parse tree
   **/
  override def visitSelectHbase(
      ctx: CustomSqlParserParser.SelectHbaseContext): AnyRef = {
    val hbaseInfo = ctx.hBaseSearchState()
    val familyInfos = hbaseInfo.familyColumns.asScala.map(family => {
      val columnsInfo = family.columns.asScala.map(colms => {
        HbaseColumnsInfoOperation(colms.colName.getText, colms.colType.getText)
      })
      HbaseFamilyColumnsInfoOperation(family.familyName.getText, columnsInfo)
    })
    HbaseSearchInfoOperation(hbaseInfo.tableName.getText,
                             hbaseInfo.key.getText,
                             familyInfos)
  }

  override def visitUdfFunctionStateTest(
      ctx: CustomSqlParserParser.UdfFunctionStateTestContext): AnyRef = {
    val colsInfo = SqlParserVisitorUtils.getColumnsInfo(ctx.cols.asScala)
    TableSelectInfoOperation(ctx.tablename.getText, colsInfo)
  }

  override def visitUdfstate(
      ctx: CustomSqlParserParser.UdfstateContext): AnyRef = {
    val r = visitUdfFunctionStateTest(ctx.udfFunctionStateTest())
    r
  }

  /**
    *
    * @param ctx the parse tree
   **/
  override def visitHbaseJoin(
      ctx: CustomSqlParserParser.HbaseJoinContext): AnyRef = {
    val hbasestate = ctx.hbaseJoinState()
    val tb = hbasestate.createTableDefineState()
    val createTablename = if (tb == null) "" else tb.createTablename.getText
    val cols = SqlParserVisitorUtils.getColumnsInfo(hbasestate.cols.asScala)
    TableJoinHbaseInfoOperation(createTablename,
                                cols,
                                hbasestate.tablename.getText,
                                hbasestate.hbasetable.getText,
                                hbasestate.joinkey.getText,
                                hbasestate.zk.getText)
  }

  /**
   *
   * @param ctx the parse tree
   *    */
  override def visitCollectState(ctx: CustomSqlParserParser.CollectStateContext): AnyRef = {
      val dfCountState = ctx.dataframCollectState()
      val paramName = dfCountState.paramName.getText
      val tableName = dfCountState.tableName.getText
      val actionName = dfCountState.actionName.getText
    DatasetCollectInfoOperation(paramName, tableName, actionName)
  }
}
