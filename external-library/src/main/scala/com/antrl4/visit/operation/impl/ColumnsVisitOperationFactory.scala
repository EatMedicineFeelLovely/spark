package com.antrl4.visit.operation.impl

object ColumnsVisitOperationFactory {

  case class ColumnsWithUdfInfoOperation(udfName: String,
                                         paramCols: Seq[ColumnsInfoWithRowIndexOperation],
                                         newColsName: ColumnsInfoOperation)
      extends AnyRef {}

  case class ColumnsInfoOperation(colName: String, colType: String, family: String= null)

  case class ColumnsInfoWithRowIndexOperation(colName: String, colType: String, index: Int, family: String= null)

}
