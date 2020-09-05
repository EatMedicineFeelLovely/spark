package com.antrl4.visit.parser.impl

import com.antlr4.parser.{CustomSqlParserBaseVisitor, CustomSqlParserParser}
import com.antrl4.visit.operation.impl._
import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory.{
  ColumnsInfoOperation,
  ColumnsInfoWithRowIndexOperation,
  ColumnsWithUdfInfoOperation
}
import com.antrl4.visit.operation.impl.TableInfoVisitOperationFactory.{
  TableJoinHbaseInfoOperation,
  TableSelectInfoOperation
}

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
    val colsInfo = ctx.cols.asScala.map(x => {
      if (x.udfname == null)
        ColumnsVisitOperationFactory
          .ColumnsWithUdfInfoOperation(
            "",
            null,
            ColumnsInfoOperation(x.cols.getText, null));
      else
        ColumnsVisitOperationFactory.ColumnsWithUdfInfoOperation(
          x.udfname.getText,
          x.paramcols.asScala.map(x => {
            ColumnsInfoWithRowIndexOperation(x.colname.getText, null, -1)
          }),
          ColumnsInfoOperation(x.newColsName.getText, null)
        );
    })
    TableSelectInfoOperation(ctx.tablename.getText, colsInfo)
  }

  override def visitUdfstate(
      ctx: CustomSqlParserParser.UdfstateContext): AnyRef = {
    val r = visitUdfFunctionStateTest(ctx.udfFunctionStateTest())
    println(r)
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
    val newtablename = if (tb == null) "" else tb.newtablename.getText
    val cols = hbasestate.cols.asScala.map(x => {
      if (x.udfname == null) {
        if (x.cols.family == null) {
          ColumnsVisitOperationFactory
            .ColumnsWithUdfInfoOperation(
              null,
              null,
              ColumnsInfoOperation(x.cols.colname.getText, null))
        } else {
          ColumnsVisitOperationFactory
            .ColumnsWithUdfInfoOperation(
              null,
              null,
              ColumnsInfoOperation(x.cols.colname.getText,
                                   null,
                x.cols.family.getText))
        }

      } else {
        ColumnsVisitOperationFactory
          .ColumnsWithUdfInfoOperation(
            x.udfname.getText,
            x.paramcols.asScala.map { x =>
              if (x.family == null) {
                ColumnsInfoWithRowIndexOperation(x.colname.getText, null, -1)
              } else {
                ColumnsInfoWithRowIndexOperation(x.colname.getText,
                                                 null,
                                                 -1,
                                                 x.family.getText)
              }
            },
            ColumnsInfoOperation(x.newColsName.getText, null)
          )
      }
    })
    TableJoinHbaseInfoOperation(newtablename,
                                cols,
                                hbasestate.tablename.getText,
                                hbasestate.hbasetable.getText,
                                hbasestate.joinkey.getText,
                                hbasestate.zk.getText)
  }

}
