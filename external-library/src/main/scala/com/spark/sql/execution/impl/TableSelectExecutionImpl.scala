package com.spark.sql.execution.impl

import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory.ColumnsWithUdfInfoOperation
import com.antrl4.visit.operation.impl.TableInfoVisitOperationFactory.TableSelectInfoOperation
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class TableSelectExecutionImpl(
    df: Dataset[Row],
    info: TableSelectInfoOperation,
    udfManager: mutable.HashMap[String, UserDefinedFunction])
    extends AbstractExecution {

  def exec(): DataFrame = {
    val schameMp = getSchameIndexMap(df)
    val newSchameMap = getNewSchame(info.columnsInfo, schameMp)(udfManager)

    df.mapPartitions(itor => {
      itor.map(r => {
        Row.fromSeq(newSchameMap.map {
          colInfo =>
            if (colInfo.rowIndex >= 0) {
              r.get(colInfo.rowIndex)
            } else {
              val ff =
                colInfo.userDefinedFunc.inputTypes.size match {
                  case 1 =>
                    colInfo.userDefinedFunc.f.asInstanceOf[
                      Function1[Any, colInfo.userDefinedFunc.dataType.type]]
                }
              val udfParamV =
                colInfo.columnInfo.paramCols.map(x =>
                  r.get(schameMp(x.colName)._1))
              ff(udfParamV.head)
            }
        })
      })
    })(RowEncoder(StructType(newSchameMap.map(_.structType))))

  }

}
