package com.spark.sql.execution.impl

import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory.ColumnsWithUdfInfoOperation
import com.spark.sql.engine.common.{ColumnWithUdfAndRowIndexInfo, UserDefinedFunction2}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait AbstractExecution {

  /**
   * 获取原始df的schame mapping
   *
   * @param df
   * @return schamename -> (index, struct)
   */
  def getSchameIndexMap(df: Dataset[Row]): Map[String, (Int, StructField)] = {

    val schame = df.schema
      .map(x => (x.name, x))
      .zip(0 to (df.schema.size - 1))
      .map { case ((name, tp), index) => (name, (index, tp)) }
      .toMap
    schame
  }

  /**
   * 获取转化后的scham
   *
   * @param schameMap
   * @param udfManager
   * @return
   */
  def getNewSchame(columnsInfo: Seq[ColumnsWithUdfInfoOperation],
                   schameMap: Map[String, (Int, StructField)])(
                    udfManager: mutable.HashMap[String, UserDefinedFunction2])
  : ArrayBuffer[ColumnWithUdfAndRowIndexInfo] = {
    val newSchameMap = new ArrayBuffer[ColumnWithUdfAndRowIndexInfo]()
    columnsInfo.foreach(r => {
      val newColInfo = r.newColsName
      if (r.udfName == null || r.udfName.isEmpty) {
        if (newColInfo.family == null
          || newColInfo.family.equals("null")
          || newColInfo.family.isEmpty) {
          val (index, tp) = schameMap(newColInfo.colName)
          newSchameMap.+=(
            ColumnWithUdfAndRowIndexInfo(null, r, index, tp))
        } else {
          newSchameMap.+=(
            ColumnWithUdfAndRowIndexInfo(null, r, -1,
              StructField(newColInfo.family + "." + newColInfo.colName,
                StringType)))
        }
      } else {
        val udff = udfManager.get(r.udfName).get
        val paramColInfo =
          r.paramCols.map(x => {
            if (x.family == null
              || x.family.equals("null")
              || x.family.isEmpty) {
              x.copy(index = schameMap(x.colName)._1)
            } else
            x.copy(index = -1)
          }
          )
        newSchameMap.+=(
          ColumnWithUdfAndRowIndexInfo(udff,
            r.copy(paramCols = paramColInfo),
            -1,
            StructField(r.newColsName.colName,
              udff.dataType)))
      }
    })
    newSchameMap
  }


}
