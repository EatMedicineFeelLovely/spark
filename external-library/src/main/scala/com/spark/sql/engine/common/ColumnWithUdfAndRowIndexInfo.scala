package com.spark.sql.engine.common

import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory._
import org.apache.spark.sql.types.StructField
case class ColumnWithUdfAndRowIndexInfo(
    userDefinedFunc: UserDefinedFunction2,
    columnInfo: ColumnsInfo,
    rowIndex: Int,
    structType: StructField) {}
