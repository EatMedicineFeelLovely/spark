package com.antrl4.visit.operation.impl

import com.spark.sql.engine.common.SQLEnumeration.SQLEnumeration
import com.spark.sql.engine.common.UserDefinedFunction2
import org.apache.spark.sql.types.StructField

object ColumnsVisitOperationFactory {

  /**
   *
   * @param colTransType
   * @param colName
   * @param colSchameInfo
   * @param udfInfo
   */
  case class ColumnsInfo(colTransType: SQLEnumeration,
                         colName: ColumnsBasicInfo,
                         colSchameInfo: ColumnsSchameInfo,
                         udfInfo: UdfInfo)

  case class ColumnsSchameInfo(colType: String,
                               rowIndex: Int,
                               structType: StructField)

  /**
    *
    * @param colName 字段名
    * @param colV 字段的值；正常情况为0，当出现常量作为字段的时候需要
    * @param family
    */
  case class ColumnsBasicInfo(colName: String,
                              colV: Any,
                              family: String = null) {
    override def toString: String = {
      if (family == null) { colName } else family + "." + colName
    }
  }

  case class UdfInfo(udfName: String,
                     paramCols: Seq[ColumnsInfo],
                     userDefinedFunc: UserDefinedFunction2)
//  case class ColumnsWithUdfInfoOperation(udfName: String,
//                                         paramCols: Seq[ColumnsInfoWithRowIndexOperation],
//                                         newColsName: ColumnsInfoOperation,
//                                         colName: String,
//                                         colType: String,
//                                         index: Int,
//                                         family: String= null)
//
//
//      extends AnyRef {}
//
//  case class ColumnsInfoOperation(colName: String, colType: String, family: String= null)
//
//  case class ColumnsInfoWithRowIndexOperation(colName: String, colType: String, index: Int, family: String= null)

}
