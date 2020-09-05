package com.spark.sql.execution.impl

import com.antrl4.visit.operation.impl.ColumnsVisitOperationFactory.ColumnsWithUdfInfoOperation
import com.spark.sql.engine.common.ColumnWithUdfAndRowIndexInfo
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait AbstractExecution {

  /**
    * 获取原始df的schame mapping
    * @param df
    * @return  schamename -> (index, struct)
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
    * @param schameMap
    * @param udfManager
    * @return
    */
  def getNewSchame(columnsInfo: Seq[ColumnsWithUdfInfoOperation],
                   schameMap: Map[String, (Int, StructField)])(
      udfManager: mutable.HashMap[String, UserDefinedFunction])
    : ArrayBuffer[ColumnWithUdfAndRowIndexInfo] = {
    val newSchameMap = new ArrayBuffer[ColumnWithUdfAndRowIndexInfo]()
    columnsInfo.foreach(r => {
      if (r.udfName.isEmpty) {
        val (index, tp) = schameMap(r.newColsName.colName)
        newSchameMap.+=(
          ColumnWithUdfAndRowIndexInfo(null, r, index, tp))
      } else {
        val udff = udfManager.get(r.udfName).get
        val paramColInfo =
          r.paramCols.map(x => x.copy(index = schameMap(x.colName)._1))
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

//  def getHbaseNewSchame(d: HbaseJoinInfoOperation,
//                        schameMap: Map[String, (Int, StructField)])(
//      udfManager: mutable.HashMap[String, UserDefinedFunction])
//    : ArrayBuffer[ColumnWithUdfAndRowIndexInfo] = {
//    val newSchameMap = new ArrayBuffer[ColumnWithUdfAndRowIndexInfo]()
//
////    d.columnsInfo.foreach(x => {
////      if (x.newColsName.family == null || x.newColsName.family.equals("null") || x.newColsName.family.isEmpty) {
////        val (index, tp) = schameMap(x.newColsName.colName)
////        newSchameMap.+=(
////          ColumnWithUdfAndRowIndexInfo(ColumnsInfoOperation(x.newColsName.colName, null, null),
////                     null,
////                     null,
////                     index,
////                     tp))
////      } else { // hbase的数据统一string类型
////        newSchameMap.+=(
////          ColumnWithUdfAndRowIndexInfo(ColumnsInfoOperation(x.colName, null, x.family),
////                     null,
////                     null,
////                     -1,
////                     StructField(x.family + "." + x.colName, StringType)))
////      }
////    })
//    newSchameMap
//  }

}
