package com.antrl4.visit.operation.impl

import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory.ColumnsWithUdfInfoOperation

object TableInfoVisitOperationFactory {

  case class TableSelectInfoOperation(
      var resultTablename: String,
      var columnsInfo: Seq[ColumnsWithUdfInfoOperation])
      extends AnyRef

  case class TableJoinHbaseInfoOperation(
      resultTablename: String,
      columnsInfo: Seq[ColumnsWithUdfInfoOperation],
      joinTablename: String,
      hbaseTable: String,
      joinkey: String,
      zk: String)
      extends AnyRef
}
