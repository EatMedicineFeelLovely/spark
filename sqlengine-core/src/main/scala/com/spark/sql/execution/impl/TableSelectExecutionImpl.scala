package com.spark.sql.execution.impl

import com.antrl4.visit.operation.impl.TableInfoVisitOperationFactory.{
  TableQueryInfo,
  TableSelectInfoOperation
}
import com.spark.sql.engine.common.UserDefinedFunction2
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class TableSelectExecutionImpl(
    df: Dataset[Row],
    info: TableQueryInfo,
    udfManager: mutable.HashMap[String, UserDefinedFunction2])
    extends AbstractExecution {
  def exec(): DataFrame = {
    val schameMp = getSchameIndexMap(df)
    val newSchameMap = getNewSchame(info.columnsInfo, schameMp)(udfManager)
    df.map(row => {
      Row.fromSeq(newSchameMap.map { colInfo =>
        execUdf(colInfo, row, null)
      })
    })(RowEncoder(StructType(newSchameMap.map(_.colSchameInfo.structType))))
  }
}
