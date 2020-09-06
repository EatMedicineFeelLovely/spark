package com.spark.sql.engine.common

import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory.{ColumnsInfoOperation, ColumnsWithUdfInfoOperation}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructField
import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory
case class ColumnWithUdfAndRowIndexInfo(
    userDefinedFunc: UserDefinedFunction2,
    columnInfo: ColumnsWithUdfInfoOperation,
    rowIndex: Int,
    structType: StructField) {}
