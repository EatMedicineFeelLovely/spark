package com.spark.sql.execution.impl

import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory.ColumnsInfo
import com.spark.sql.engine.common.SQLEnumeration._
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
      val tansColInfo = col.colTransType match {
        case BASIC_COL => {
          val (index, tp) = schameMap(asColName.toString)
          col.copy(
            colSchameInfo =
              col.colSchameInfo.copy(rowIndex = index, structType = tp))
        }
        case CONSTANTPARAM_COL => col
        case HBASE_COL         => col
        case UDF_COL => {
          val userDefinedFunc = udfManager.get(udfInfo.udfName).get
          val paramColInfo =
            getNewSchame(col.udfInfo.paramCols, schameMap)(udfManager)
          if (userDefinedFunc.inputParamsNum != paramColInfo.size) // 防止传入的参数和udf不符合
            throw new Exception(s"org.apache.spark.sql.AnalysisException: " +
              s"Invalid number of arguments for function ${col.udfInfo.udfName}. Expected: ${userDefinedFunc.inputParamsNum}; Found: ${paramColInfo.size}")
          col.copy(
            udfInfo = col.udfInfo.copy(paramCols = paramColInfo,
                                       userDefinedFunc = userDefinedFunc),
            colSchameInfo = col.colSchameInfo.copy(
              structType =
                StructField(asColName.toString, userDefinedFunc.dataType))
          )
        }
        case _ => null
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
  def execUdf(colInfo: ColumnsInfo, row: Row, result: Result): Any = {
    val colSchameInfo = colInfo.colSchameInfo
    val colName = colInfo.colName
    val colUdfInfo = colInfo.udfInfo
    colInfo.colTransType match {
      case BASIC_COL => {
        row.get(colSchameInfo.rowIndex)
      }
      case CONSTANTPARAM_COL => {
        colInfo.colName.colV
      }
      case HBASE_COL => {
        if (result != null && result.containsColumn(colName.family.getBytes(),
                                                    colName.colName.getBytes()))
          new String(
            result.getValue(colName.family.getBytes(),
                            colName.colName.getBytes()))
        else "null"
      }
      case UDF_COL => {
        val parmsValue = colUdfInfo.paramCols.map { x =>
          execUdf(x, row, result)
        }
        colUdfInfo.userDefinedFunc.inputParamsNum match {
          case 1 =>
            val func = colUdfInfo.userDefinedFunc.f
              .asInstanceOf[Any => colUdfInfo.userDefinedFunc.dataType.type]
            func(parmsValue.head)
          case 2 =>
            val func = colUdfInfo.userDefinedFunc.f
              .asInstanceOf[(Any,
                             Any) => colUdfInfo.userDefinedFunc.dataType.type]
            func(parmsValue(0), parmsValue(1))
          case _ => throw new Exception("udf 参数个数不匹配")
        }
      }
    }
  }

}
