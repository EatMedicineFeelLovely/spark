package com.spark.learn

import com.antlr4.parser.{CustomSqlParserBaseVisitor, CustomSqlParserParser, CustomSqlParserVisitor}
import com.antrl4.visit.operation.impl.{AbstractVisitOperation, CheckpointVisitOperation, HelloWordVisitOperation}
import org.antlr.v4.runtime.tree.{ErrorNode, ParseTree, RuleNode, TerminalNode}

class CustomSqlParserVisitorImpl extends CustomSqlParserBaseVisitor[AbstractVisitOperation] {
//
//  override def visitCheckpointStatement(ctx: CustomSqlParserParser.CheckpointStatementContext): AbstractVisitOperation = {
//    CheckpointVisitOperation(ctx.table.getText, ctx.location.getText)
//  }
  override def visitHelloWordStatement(ctx: CustomSqlParserParser.HelloWordStatementContext): AbstractVisitOperation = {
    HelloWordVisitOperation(ctx.word.getText)
  }
  override def visitCheckpointStatement(ctx: CustomSqlParserParser.CheckpointStatementContext): AbstractVisitOperation = {
        CheckpointVisitOperation(ctx.table.getText, ctx.location.getText)
  }
//  override def visitCheckpoint(ctx: CustomSqlParserParser.CheckpointContext): AbstractVisitOperation = {
//    print("xxxx")
//
//    visitCheckpointStatement(ctx.checkpointStatement())
//  }

  override def visitHelloWord(ctx: CustomSqlParserParser.HelloWordContext): AbstractVisitOperation = {
print(">>>>>>>>>>")
    visitHelloWordStatement(ctx.helloWordStatement())
  }


}
