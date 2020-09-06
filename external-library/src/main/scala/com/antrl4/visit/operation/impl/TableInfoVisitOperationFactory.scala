package com.antrl4.visit.operation.impl

import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory._

object TableInfoVisitOperationFactory {

  case class TableSelectInfoOperation(
      var resultTablename: String,
      var columnsInfo: Seq[ColumnsInfo])
      extends AnyRef

  case class TableJoinHbaseInfoOperation(
      resultTablename: String,
      columnsInfo: Seq[ColumnsInfo],
      joinTablename: String,
      hbaseTable: String,
      joinkey: String,
      zk: String)
      extends AnyRef
}
