package com.spark.sql.execution.impl

import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory._
import com.spark.sql.engine.common.UserDefinedFunction2
import org.apache.hadoop.hbase.client.Result
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
  def getNewSchame(columnsInfo: Seq[ColumnsInfo],
                   schameMap: Map[String, (Int, StructField)])(
                    udfManager: mutable.HashMap[String, UserDefinedFunction2])
  : ArrayBuffer[ColumnsInfo] = {
    val newSchameMap = new ArrayBuffer[ColumnsInfo]()
    columnsInfo.foreach(col => {
      val asColName = col.colName
      val udfInfo = col.udfInfo
      val tansColInfo =
      // 无udf
      if (udfInfo == null || udfInfo.udfName.isEmpty) {
        if (col.colName.family == null) {
          val (index, tp) = schameMap(asColName.toString)
            col.copy(colSchameInfo = col.colSchameInfo.copy(rowIndex = index, structType = tp))
        } else {
            col.copy(colSchameInfo = col.colSchameInfo.copy(structType = StructField(asColName.toString,
              StringType)))
        }
      } else {
        val userDefinedFunc = udfManager.get(udfInfo.udfName).get
        val paramColInfo = getNewSchame(col.udfInfo.paramCols, schameMap)(udfManager)
        if(userDefinedFunc.inputParamsNum != paramColInfo.size)
          throw new Exception(
            s"org.apache.spark.sql.AnalysisException: " +
              s"Invalid number of arguments for function ${col.udfInfo.udfName}. Expected: ${userDefinedFunc.inputParamsNum}; Found: ${paramColInfo.size}")
        col.copy(
          udfInfo = col.udfInfo.copy(paramCols = paramColInfo, userDefinedFunc = userDefinedFunc),
          colSchameInfo = col.colSchameInfo.copy(structType = StructField(asColName.toString,
            userDefinedFunc.dataType)))
      }
      newSchameMap.+=(tansColInfo)
    })
    newSchameMap
  }


  /**
   *
   * @param colInfo
   * @param row
   */
  def execUdf(colInfo: ColumnsInfo, row: Row, result: Result): Any ={
    val colSchameInfo = colInfo.colSchameInfo
    val colName = colInfo.colName
    val colUdfInfo = colInfo.udfInfo
    if (colSchameInfo.rowIndex >= 0) {
      row.get(colSchameInfo.rowIndex)
    } else {
      if(colName.family != null) { // 因为是从hbase取数导致的index 未知（-1）
        if(result!=null && result.containsColumn(colName.family.getBytes(), colName.colName.getBytes()))
        new String(result.getValue(colName.family.getBytes(), colName.colName.getBytes()))
        else "null"
      } else { // 否则是因为udf导致的
        val parmsValue = colUdfInfo.paramCols.map{x =>
          execUdf(x, row, result)
        }
        colUdfInfo.userDefinedFunc.inputParamsNum match {
          case 1 =>
            val func = colUdfInfo.userDefinedFunc.f.asInstanceOf[
              Function1[Any, colUdfInfo.userDefinedFunc.dataType.type]]
            func(parmsValue.head)
          case 2 =>
            val func = colUdfInfo.userDefinedFunc.f.asInstanceOf[
              Function2[Any, Any, colUdfInfo.userDefinedFunc.dataType.type]]
            func(parmsValue(0), parmsValue(1))
          case _ => throw new Exception("hhhh")
        }
      }
    }

  }

}
